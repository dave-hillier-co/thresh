import { createServer, type Server } from "node:http";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { counter } from "@thresh/example-k8s-silo/interfaces";

const hostName = (): string => process.env.POD_NAME ?? "local";

/**
 * A tiny HTTP front for the counter grain so the e2e can drive grains over the
 * transport without an external client. Any pod can serve any request: the proxy
 * routes to the grain's single activation wherever it lives.
 *
 * - `POST /counters/:key/increment` → `{ value, host }`
 * - `GET  /counters/:key`           → `{ value, host }`
 * - `GET  /whoami`                  → `{ pod }` (which pod served the request)
 */
export function createCounterApi(host: SiloHost): Server {
  return createServer((req, res) => {
    void handle(req.method ?? "GET", req.url ?? "/", host)
      .then((body) => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify(body));
      })
      .catch((err: unknown) => {
        res.writeHead(500, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: String(err) }));
      });
  });
}

async function handle(method: string, url: string, host: SiloHost): Promise<unknown> {
  const path = url.split("?")[0] ?? "/";
  if (method === "GET" && path === "/whoami") return { pod: hostName() };

  const match = /^\/counters\/([^/]+)(\/increment)?$/.exec(path);
  if (match !== null) {
    const key = decodeURIComponent(match[1]!);
    const counterRef = host.getGrain(counter, key);
    if (method === "POST" && match[2] === "/increment") return counterRef.increment();
    if (method === "GET" && match[2] === undefined) return counterRef.current();
  }
  throw new Error(`no route for ${method} ${path}`);
}
