# Timers, reminders, and durable jobs

**Timers** are activation-scoped periodic work. They disappear on deactivation or silo failure and
callbacks run through grain scheduling. Use a timer only when loss is acceptable.

**Reminders** persist registrations and reactivate the target grain to deliver ticks. Configure
memory, Redis, or Postgres reminders and make handlers idempotent. Delivery can repeat during
failure, so a reminder is a durable trigger, not an exactly-once transaction.

**Durable jobs** are sharded queued work with retry/poll outcomes. Attach a handler with
`useDurableJobHandler`; return `completed`, `pollAfter(duration)`, or `failed(error)`. Jobs suit
payload-bearing retryable work and survive activation loss. Configure memory or Redis job shards
and an explicit retry policy.
