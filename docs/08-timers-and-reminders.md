# 08 — Timers and reminders

Two mechanisms for doing something later or periodically, with very different durability guarantees,
mirroring Orleans.

> Orleans references: `Orleans.Core.Abstractions/Runtime/IGrainTimer.cs`,
> `Orleans.Runtime/Timers/GrainTimer.cs`, `Orleans.Reminders/Timers/IRemindable.cs`,
> `Orleans.Reminders/SystemTargetInterfaces/{IReminderService,IReminderTable}.cs`,
> `Orleans.Reminders/ReminderService/LocalReminderService.cs`.

| | Timer | Reminder |
| --- | --- | --- |
| Durable | No | Yes |
| Survives deactivation | No (cancelled on deactivate) | Yes (reactivates the grain) |
| Survives pod/silo loss | No | Yes (fires from another silo) |
| Backed by | In-memory on the activation | A durable store (Redis default) |
| Granularity | Sub-second possible | Coarser (minutes-scale typical) |

A timer is an optimisation tied to the current activation; a reminder is a durable promise to call the
grain even if everything restarts.

## Timers

A timer fires a callback on the grain's activation **as a turn** (so it respects single-threaded
execution; [02](02-actor-model.md)), is non-durable, and is cancelled when the activation deactivates.
It does not by itself keep a grain alive (Orleans' default behaviour).

```ts
const heartbeat = ctx.runtime.registerTimer(checkIdle, { seconds: 30 }, { seconds: 30 }); // due, period
// interface GrainTimer { change(due, period?): void; dispose(): void; }
```

## Reminders

A reminder is a **durable, named schedule** attached to a grain identity. It is persisted, so it fires
even if the grain was deactivated or its silo died — the runtime activates the grain (wherever it is
then placed) and delivers the tick. A grain receives ticks via a `receiveReminder` method (the
`Remindable` shape):

```ts
const BillingGrain = defineGrain<IBilling>("Billing", (ctx) => ({
  startMonthlyInvoice: async () => ctx.runtime.registerReminder("invoice", { days: 1 }, { days: 30 }),
  stop: async () => ctx.runtime.unregisterReminder("invoice"),
  // the runtime reactivates the grain and delivers the tick as a turn
  receiveReminder: async (name, _tick: TickStatus) => { if (name === "invoice") await issueInvoice(); },
}));
// interface TickStatus { firstTickAt: Date; period: Duration; currentTickAt: Date; }
```

**Tick semantics (faithful to Orleans).** A reminder fires at `firstTickAt + N·period`. Missed ticks
(silo down / grain unreachable) are **skipped, not caught up** — firing resumes at the next scheduled
time, and `currentTickAt` reports the *scheduled* time so the handler can detect lateness. Each tick is
an ordinary single-threaded turn (`IRemindable.ReceiveReminder`), and unregistering cleanly stops
future ticks.

### Distribution across silos

Reminders live in a pluggable `ReminderTable` (`upsert` / `remove` / `read` / `readForGrain` /
`readRange`, with an etag) driven by a per-silo service (Orleans' `IReminderTable` +
`LocalReminderService`). **Responsibility** is partitioned by the **same consistent-hash ring** the
directory uses ([06](06-grain-directory-and-placement.md)): each silo owns the hash ranges the ring
assigns it and fires the reminders whose `grainId` hashes into them, reading its ranges from the table
(`readRange`) on startup, on every membership change, and periodically (so a reminder registered on a
non-owner is discovered by the owner). A fired tick is **routed to the grain's single activation
through the dispatcher** (directory → placement), so the reminder owner never spins up a second
activation. When a silo leaves, its ranges reassign to the new owners, which pick up the affected
reminders from the table — no reminder is dropped, only briefly delayed.

### Providers

- **Redis (default)** — reminders are hashes indexed in a sorted set by grain-hash so `readRange` is a
  server-side query; atomic Lua upsert/remove with etag CAS. `useRedisReminders({ url, keyPrefix? })`.
- **Postgres** — each reminder a row with an indexed `hash` column for `readRange` (incl. wrap-around)
  and etag-CAS remove. `usePostgresReminders({ connectionString, tableName? })`.
- **In-memory** — dev/tests; not durable, single-silo. `useReminders()`.

All three are interchangeable behind `ReminderTable`; Redis is the default
([ADR 0005](adr/0005-redis-default-providers.md)). Timers are in-memory, fired as turns via the
injectable clock and cancelled on deactivation.

A common pattern: a coarse **reminder** wakes the grain, which then uses a fine-grained **timer** while
it is active.
