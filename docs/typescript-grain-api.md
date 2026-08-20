# TypeScript grain API idioms

Thresh keeps Orleans-compatible names for parity work, but application code can be
more idiomatic TypeScript than a direct C# port.

## Prefer structural type aliases over `I*` interfaces

A grain surface is just the structural shape of the methods callers can invoke.
You do not need an `I` prefix or declaration-merging-style names.

```ts
import { defineGrain } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

type Thermostat = GrainKey<string> & {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
};

const thermostat = defineGrainInterface<Thermostat>("example.Thermostat");

const ThermostatGrain = defineGrain<Thermostat>("Thermostat", (ctx) => ({
  async onUpdate(status) {
    // ...
    return [];
  },
}));
```

This reads like ordinary TypeScript: a lowercase value (`thermostat`) carries the
runtime descriptor, while the `Thermostat` type is erased at compile time.

## Use `GrainKey<TKey>` for key type, not Orleans marker names

The older `GrainWithStringKey`, `GrainWithIntegerKey`, and compound-key marker
interfaces remain supported because they map directly to Orleans concepts. For
new TypeScript code, `GrainKey<TKey>` or `KeyedGrain<TKey>` makes the API intent
clear without encoding the key kind in a C#-style interface name.

```ts
type Account = GrainKey<bigint> & {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
};
```

If no key marker is present, `getGrain` still defaults to `string` keys for
backwards compatibility.

## Keep `defineGrainInterface` as the runtime descriptor

TypeScript erases interfaces and type aliases, so Thresh still needs a value to
carry the stable interface id, version, and per-method invocation options. Treat
that value like a schema or route descriptor rather than a C# interface object.
