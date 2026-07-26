import { afterEach, describe, expect, it } from "vitest";
import { HealthCheck } from "@thresh/hosting/health-check";
import { HealthServer } from "@thresh/hosting/health-server";

let server: HealthServer | undefined;
afterEach(async () => {
  await server?.close();
  server = undefined;
});

async function start(health: HealthCheck): Promise<string> {
  server = new HealthServer(health);
  const port = await server.listen(0, "127.0.0.1");
  return `http://127.0.0.1:${port}`;
}

describe("HealthServer", () => {
  it("serves 503 for readiness before the silo is ready, 200 after", async () => {
    const health = new HealthCheck();
    const base = await start(health);

    expect((await fetch(`${base}/ready`)).status).toBe(503);

    health.update({
      started: true,
      transportReady: true,
      membershipHealthy: true,
    });
    const ready = await fetch(`${base}/ready`);
    expect(ready.status).toBe(200);
    expect(((await ready.json()) as { ok: boolean }).ok).toBe(true);
  });

  it("serves liveness and startup probes", async () => {
    const health = new HealthCheck();
    const base = await start(health);
    expect((await fetch(`${base}/live`)).status).toBe(200);
    expect((await fetch(`${base}/startup`)).status).toBe(503);
    health.update({ started: true });
    expect((await fetch(`${base}/startup`)).status).toBe(200);
  });

  it("404s an unknown path", async () => {
    const base = await start(new HealthCheck());
    expect((await fetch(`${base}/nope`)).status).toBe(404);
  });
});
