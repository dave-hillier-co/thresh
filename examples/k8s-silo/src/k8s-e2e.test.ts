import { execFile, spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * Kubernetes end-to-end for the Phase-3 exit criteria (docs/13): a StatefulSet of
 * silos forms a cluster, calls route to one activation across pods, killing a pod
 * reactivates its grains on a survivor, and a rolling update drains and rejoins
 * without losing state.
 *
 * Opt-in: heavyweight (builds an image, deploys, rolls pods), so it runs only
 * when `K8S_E2E=1` and a cluster is reachable. Build/deploy against the current
 * kube context (Docker Desktop or kind — load the image with `kind load` first):
 *
 *   K8S_E2E=1 pnpm exec vitest run examples/k8s-silo/src/k8s-e2e.test.ts
 */
const ENABLED = process.env.K8S_E2E === "1";

const exec = promisify(execFile);
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");
const IMAGE = "thresh-k8s-silo:dev";
const ns = `thresh-e2e-${Math.random().toString(36).slice(2, 8)}`;

const sh = (cmd: string, args: string[], timeoutMs = 120_000) =>
  exec(cmd, args, { timeout: timeoutMs, maxBuffer: 16 * 1024 * 1024, cwd: repoRoot });
const kubectl = (args: string[], timeoutMs?: number) =>
  sh("kubectl", ["-n", ns, ...args], timeoutMs);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function podNames(): Promise<string[]> {
  const { stdout } = await kubectl([
    "get",
    "pods",
    "-l",
    "app=silo",
    "-o",
    "jsonpath={.items[*].metadata.name}",
  ]);
  return stdout.trim().split(/\s+/).filter(Boolean);
}

interface Forward {
  port: number;
  stop: () => void;
}

/** Port-forward a pod's API port, letting kubectl pick the local port. */
function portForward(pod: string): Promise<Forward> {
  return new Promise<Forward>((resolveFwd, reject) => {
    const child = spawn("kubectl", ["-n", ns, "port-forward", `pod/${pod}`, ":8081"]);
    let settled = false;
    const fail = (err: Error) => {
      if (settled) return;
      settled = true;
      child.kill();
      reject(err);
    };
    child.stdout.on("data", (chunk: Buffer) => {
      const match = /127\.0\.0\.1:(\d+)/.exec(chunk.toString());
      if (match !== null && !settled) {
        settled = true;
        resolveFwd({ port: Number(match[1]), stop: () => child.kill() });
      }
    });
    child.on("error", fail);
    child.on("exit", () => fail(new Error(`port-forward to ${pod} exited early`)));
    setTimeout(() => fail(new Error(`port-forward to ${pod} timed out`)), 20_000);
  });
}

interface CounterReply {
  value: number;
  host: string;
}

/** Increment via the forwarded port, retrying transient routing/forward errors. */
async function increment(port: number, key: string): Promise<CounterReply> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 20; attempt += 1) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/counters/${key}/increment`, {
        method: "POST",
      });
      if (res.ok) return (await res.json()) as CounterReply;
      lastError = new Error(`HTTP ${res.status}`);
    } catch (err) {
      lastError = err;
    }
    await sleep(500);
  }
  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function withForward<T>(pod: string, fn: (port: number) => Promise<T>): Promise<T> {
  const fwd = await portForward(pod);
  try {
    return await fn(fwd.port);
  } finally {
    fwd.stop();
  }
}

describe.skipIf(!ENABLED)("k8s silo cluster (e2e)", () => {
  beforeAll(async () => {
    await sh("docker", ["build", "-t", IMAGE, "-f", "examples/k8s-silo/Dockerfile", "."], 600_000);
    await sh("kubectl", ["create", "namespace", ns]);
    const apply = (file: string) => kubectl(["apply", "-f", `examples/k8s-silo/deploy/${file}`]);
    await apply("rbac.yaml");
    await apply("redis.yaml");
    await apply("silo.yaml");
    await kubectl(["rollout", "status", "deploy/redis", "--timeout=180s"], 200_000);
    await kubectl(["rollout", "status", "statefulset/silo", "--timeout=300s"], 320_000);
  }, 900_000);

  afterAll(async () => {
    if (!ENABLED) return;
    await sh("kubectl", ["delete", "namespace", ns, "--wait=false"]).catch(() => {});
  });

  it("forms a 3-silo cluster from the StatefulSet", async () => {
    const pods = await podNames();
    expect(pods.sort()).toEqual(["silo-0", "silo-1", "silo-2"]);
  });

  it("routes calls to one activation regardless of which pod is hit", async () => {
    const key = `route-${Date.now()}`;
    const first = await withForward("silo-0", (port) => increment(port, key));
    const second = await withForward("silo-1", (port) => increment(port, key));
    const third = await withForward("silo-2", (port) => increment(port, key));

    // One activation: every pod's call landed on the same host and the value
    // advanced monotonically across them (no divergent second activation).
    expect(second.host).toBe(first.host);
    expect(third.host).toBe(first.host);
    expect([first.value, second.value, third.value]).toEqual([1, 2, 3]);
  }, 60_000);

  it("reactivates a grain on a survivor when its host pod is killed", async () => {
    const key = `failover-${Date.now()}`;
    const before = await withForward("silo-0", (port) => increment(port, key));
    const host = before.host; // e.g. "silo-2"
    const survivor = (await podNames()).find((p) => p !== host)!;

    await kubectl(["delete", "pod", host, "--wait=false"]);

    // Poll a survivor until the activation has moved off the killed pod. State is
    // durable (Redis), so the value continues climbing rather than resetting.
    const deadline = Date.now() + 90_000;
    let latest = before;
    await withForward(survivor, async (port) => {
      while (Date.now() < deadline) {
        latest = await increment(port, key);
        if (latest.host !== host) return;
        await sleep(1000);
      }
    });

    expect(latest.host).not.toBe(host);
    expect(latest.value).toBeGreaterThan(before.value);
  }, 180_000);

  it("survives a rolling update with state intact", async () => {
    const key = `rollout-${Date.now()}`;
    const before = await withForward("silo-0", (port) => increment(port, key));

    await kubectl(["rollout", "restart", "statefulset/silo"]);
    await kubectl(["rollout", "status", "statefulset/silo", "--timeout=300s"], 320_000);

    const after = await withForward("silo-0", (port) => increment(port, key));
    expect(after.value).toBeGreaterThan(before.value);
  }, 420_000);
});
