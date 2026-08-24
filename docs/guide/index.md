# Introduction

Thresh is a TypeScript virtual actor runtime. **Grains** are logical objects that are always
addressable. The runtime creates an in-memory activation on demand, routes calls to it, serializes
its turns, and collects it after an idle period. Callers hold lightweight references rather than
connections to a particular process.

## Mental model

1. Write the grain with `defineGrain(name, factory)`. The factory runs once per activation; the
   object it returns is the message surface, and the definition *is* the contract.
2. Register that definition on each silo with `registerGrain(definition)`.
3. Obtain a reference with `getGrain(definition, key)` and call asynchronous methods.

For a contract that crosses a process boundary, declare it separately with `defineGrainInterface` so
callers need not import the implementation, and register with `registerGrain(ctor, { interfaces })`.

Activation-local closure variables are volatile. Use a state facet for data that must survive
deactivation. A grain processes one call at a time by default, but calls can cross silos and should
always be treated as remote: keep arguments serializable, avoid blocking, and propagate errors.

## Choose a facility

| Need | Facility |
|---|---|
| Mutable durable record | persistent state |
| Pure event folding into a snapshot | reducer state / reducer grain |
| Atomic state spanning grains | transactional state |
| Event log and durable collections | journaling / durable state |
| Work only while activation lives | grain timer |
| Durable scheduled callback | reminder |
| Durable retryable queue work | durable job |
| Ordered pub/sub | stream |
| Best-effort fan-out | broadcast channel |

Thresh targets Orleans 10 semantics where practical. See [deviations](/deviations) for intentional
TypeScript and Kubernetes differences and the project `EPICS.md` for implementation status.
