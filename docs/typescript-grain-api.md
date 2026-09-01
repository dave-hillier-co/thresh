# TypeScript grain API idioms

Thresh keeps Orleans-compatible names for parity work, but application code can be
more idiomatic TypeScript than a direct C# port.

## Let the definition be the interface

`defineGrain` infers the message surface from what the factory returns, so the
definition it hands back *is* the contract. One declaration, not four:

```ts
import { defineGrain, usePersistentState } from "@thresh/core/define-grain";

const Thermostat = defineGrain("Thermostat", () => {
  const state = usePersistentState<Reading>("thermostat", {
    defaultValue: (): Reading => ({ tempC: 20, targetC: 21 }),
  });

  return {
    onUpdate: async (tempC: number): Promise<Command[]> => {
      state.value.tempC = tempC;
      await state.write();
      return tempC < state.value.targetC ? [{ kind: "heat" }] : [];
    },
  };
});

silo.registerGrain(Thermostat);
silo.getGrain(Thermostat, "kitchen");
```

Callers see exactly the surface the factory returned, promise-lifted;
`onActivate`/`onDeactivate` and symbol-keyed system hooks are not part of it.

**This is a trade-off, not a strict improvement.** `getGrain(Thermostat, key)`
means the caller imports the implementation module and its transitive storage and
stream dependencies. That is right in-process and wrong across a process
boundary — see below.

## Hooks follow the rules of hooks

`usePersistentState`, `useReducerState`, `useTransactionalState`, `useDurable*`
and `useDurableJobHandler` resolve the activation being set up from an ambient
slot that is live only during the *synchronous* body of the factory. Call them at
the top level of the factory, never after an `await`, from a method body, or from
`onActivate` — those throw an error naming the hook and the rule.

The factory keeps its `ctx` parameter for reaching the runtime *after* setup:
capture it in the closure and use `ctx.runtime` / `ctx.id` / `ctx.getGrain` from
method bodies. That is not a hook call and stays valid for the activation's life.
`useContext()` returns the same object during setup.

## Declare a cross-process contract separately

When callers must not import the implementation — an external client, a service
in another process — declare the contract on its own and register the two
together. This is also the form for one grain under several interfaces, and for
per-silo interface versions during a rolling upgrade.

```ts
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export type Thermostat = GrainKey<string> & {
  onUpdate(status: ThermostatStatus): Promise<Command[]>;
};

export const thermostat = defineGrainInterface<Thermostat>("example.thermostat");

silo.registerGrain(ThermostatGrain, { interfaces: [thermostat, thermostatControl] });
```

A lowercase value (`thermostat`) carries the runtime descriptor; the `Thermostat`
type is erased at compile time. TypeScript erases interfaces and type aliases, so
Thresh still needs a value to carry the stable interface id, version, and
per-method invocation options — treat it like a schema or route descriptor rather
than a C# interface object.

An explicit `interfaces` list is used verbatim, not unioned with the definition's
own interface. `registerGrain(Def, { interfaces: [Other] })` therefore leaves
`getGrain(Def, key)` unroutable, and says so by throwing.

**Interface ids are derived from the name.** Renaming an interface — including by
fusing a declared one into its implementation — changes the wire id with no
compile-time signal. Any grain that has ever been deployed must pin
`{ interfaceName: "example.thermostat" }`.

## Declare the key kind, or intersect `GrainKey<TKey>`

A fused definition has no separate interface to mark, so it declares its key kind
directly. `"string"` is the default; the others are `"integer"`, `"guid"`,
`"guid-compound"`, and `"integer-compound"`.

```ts
const Account = defineGrain("Account", () => ({ ... }), { key: "integer" });

silo.getGrain(Account, 42n);
```

A separately declared contract intersects the marker instead:

```ts
type Account = GrainKey<bigint> & {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
};
```

Both paths resolve to the same key type. The older `GrainWithStringKey`,
`GrainWithIntegerKey`, and compound-key marker names remain supported as aliases
of `GrainKey<TKey>`, so a marked interface keeps its caller key type with no
edit. If no kind is declared and no marker is present,
`getGrain` defaults to `string` keys.
