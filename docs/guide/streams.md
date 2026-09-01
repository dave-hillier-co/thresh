# Streams and broadcast channels

Streams decouple producers from consumers. Obtain a typed stream from a named provider and stream
identity, subscribe a handler, and publish values. Keep the subscription handle when you need to
unsubscribe or resume explicitly.

Memory streams suit one process and tests. Redis, Postgres, and Kafka pulling providers provide
durable queues; generator streams produce configured values. Provider names must match on all
silos. Subscription registries, cursors, filters, and failure stores have separate durability.
Consumers must be idempotent because failover can redeliver; sequence tokens express ordering, not
global exactly-once execution.

Implicit subscriptions bind a grain interface to a namespace and activate matching consumers.
Broadcast channels are lighter best-effort fan-out; use them for transient notifications rather
than recoverable processing.
