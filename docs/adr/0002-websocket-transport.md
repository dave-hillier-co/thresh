# ADR 0002 — WebSocket/HTTP transport

- Status: Accepted
- Context doc: [04 — Messaging and serialization](../04-messaging-and-serialization.md)

## Context

Silos must exchange messages (grain calls and responses) with each other and with external clients.
Orleans implements a custom TCP protocol with explicit length-prefixed framing
(`Orleans.Core/Networking`) and a connection preamble handshake. We need a transport for the
TypeScript/Node runtime that is reliable, multiplexed, debuggable, and comfortable on Kubernetes.

Options considered:

1. **WebSocket over HTTP.** One persistent duplex connection per silo pair, binary messages,
   multiplexed by correlation id.
2. **gRPC (HTTP/2).** Bidirectional streaming, mature framing/multiplexing, strong observability
   integrations.
3. **Raw TCP with custom length-prefixed framing**, mirroring Orleans most closely.

## Decision

Use **WebSocket over HTTP** (option 1), behind a `Transport` interface that keeps gRPC and raw TCP
as future substitutions.

## Rationale

- **Simple and ubiquitous in Node.** Mature, lightweight libraries; trivial to stand up and to test
  in-process and over real sockets.
- **Framing is provided.** WebSocket delivers discrete binary messages, so we do not reimplement
  Orleans' 8-byte header framing; each frame carries one serialized `Message`.
- **Duplex and multiplexable.** A single long-lived connection per peer carries all traffic,
  multiplexed by `correlationId`, avoiding per-call setup.
- **Kubernetes- and client-friendly.** Works through ordinary Services and ingress; external
  browser/web clients can use the very same transport to reach a gateway silo.
- **Debuggable.** Human-inspectable with standard tooling; pairs well with a JSON serialization mode
  for diagnostics.

gRPC was attractive for its multiplexing and observability but adds a heavier dependency, schema/IDL
expectations that fit awkwardly with the runtime-`Proxy` approach in
[ADR 0001](0001-runtime-proxy-grain-references.md), and more ceremony than the project needs at v1.
Raw TCP would be the most faithful port but means owning framing, backpressure and connection
management ourselves for little benefit over WebSocket.

## Consequences

- **The `Transport` interface is the seam.** If high-throughput silo-to-silo traffic later demands
  it, a gRPC or raw-TCP transport can be dropped in without touching the runtime, because routing,
  correlation and serialization sit above the transport
  ([04](../04-messaging-and-serialization.md)).
- **We own multiplexing and backpressure at the message layer.** Correlation-id matching, in-flight
  limits and overload rejection are implemented in the dispatcher rather than inherited from the
  protocol.
- **Connection lifecycle is ours to manage.** Reconnect, preamble handshake and cluster-id
  rejection are explicit (mirroring Orleans `ConnectionPreamble`).
- **Performance ceiling is adequate, not maximal.** Acceptable for v1; revisit via the interface if
  profiling shows the transport is the bottleneck.
