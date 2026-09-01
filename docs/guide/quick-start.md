# Quick start

## Requirements

- Node.js 22 or newer
- pnpm 10

This repository is currently a private workspace: consume `@thresh/*` packages through the
workspace rather than npm. Install and validate it with `pnpm install`, `pnpm typecheck`, and
`pnpm test`.

## Write a grain

```ts
import { defineGrain } from "@thresh/core/define-grain";

export const Counter = defineGrain("Counter", () => {
  let count = 0;
  return { increment: async () => ++count, value: async () => count };
});
```

The factory runs once per activation and the object it returns is the message surface, so the
definition is also the contract: `registerGrain(Counter)` on each silo, then
`getGrain(Counter, key)` to call it. Public grain methods return promises.

Keys default to `string`; declare another kind with `{ key: "integer" }` alongside the factory.
Per-method invocation options go in `interfaceOptions`:

```ts
defineGrain("Counter", factory, {
  key: "integer",
  interfaceOptions: { value: { readOnly: true } },
});
```

## Declare a contract separately when it crosses a process

A fused definition couples callers to the implementation module. Where that is wrong — an external
client, another service — declare the contract on its own instead:

```ts
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

type Counter = GrainKey<string> & {
  increment(): Promise<number>;
  value(): Promise<number>;
};

export const counter = defineGrainInterface<Counter>("example.counter", {
  options: { value: { readOnly: true } },
});
```

Interface names are protocol identifiers: keep them stable and globally unique, and pin
`{ interfaceName }` on any grain that has been deployed. For executable setup, copy
`examples/greeter`; for real multi-node transport copy `examples/cluster`; for deployment copy
`examples/k8s-silo`.
