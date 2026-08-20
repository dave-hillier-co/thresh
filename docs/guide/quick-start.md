# Quick start

## Requirements

- Node.js 22 or newer
- pnpm 10

This repository is currently a private workspace: consume `@thresh/*` packages through the
workspace rather than npm. Install and validate it with `pnpm install`, `pnpm typecheck`, and
`pnpm test`.

## Define a contract and implementation

```ts
import { defineGrain } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

type Counter = GrainKey<string> & {
  increment(): Promise<number>;
  value(): Promise<number>;
};

export const counter = defineGrainInterface<Counter>("example.counter", {
  options: { value: { readOnly: true } },
});

export const CounterGrain = defineGrain<Counter>("Counter", () => {
  let count = 0;
  return { increment: async () => ++count, value: async () => count };
});
```

Interface names are protocol identifiers: keep them stable and globally unique. Public grain
methods return promises. The key marker is compile-time only and determines which key type
`getGrain` accepts. For executable setup, copy `examples/greeter`; for real multi-node transport
copy `examples/cluster`; for deployment copy `examples/k8s-silo`.
