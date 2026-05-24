import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import type { HealthCheck, ProbeResult } from "@tsva/hosting/health-check";

/** Serves the `/live`, `/ready` and `/startup` probe endpoints over HTTP. */
export class HealthServer {
  private server: Server | undefined;

  constructor(private readonly health: HealthCheck) {}

  async listen(port: number, host = "0.0.0.0"): Promise<number> {
    const server = createServer((req, res) => this.handle(req, res));
    this.server = server;
    await new Promise<void>((resolve) => server.listen(port, host, resolve));
    const address = server.address();
    return typeof address === "object" && address !== null ? address.port : port;
  }

  async close(): Promise<void> {
    const server = this.server;
    if (server === undefined) return;
    this.server = undefined;
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }

  private handle(req: IncomingMessage, res: ServerResponse): void {
    const probe = this.probeFor(req.url);
    if (probe === undefined) {
      res.writeHead(404);
      res.end();
      return;
    }
    res.writeHead(probe.ok ? 200 : 503, { "content-type": "application/json" });
    res.end(JSON.stringify(probe));
  }

  private probeFor(url: string | undefined): ProbeResult | undefined {
    switch (url) {
      case "/live":
        return this.health.live();
      case "/ready":
        return this.health.ready();
      case "/startup":
        return this.health.startup();
      default:
        return undefined;
    }
  }
}
