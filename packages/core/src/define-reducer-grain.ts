import {
  defineGrain,
  usePersistentState,
  type DefineGrainOptions,
  type GrainSetup,
} from "./define-grain";
import type { Grain } from "./grain";
import { defineGrainInterface, type GrainInterface } from "./grain-interface";

/**
 * What a reducer returns: the next immutable state plus any effects to run after
 * the fold. The reducer itself stays pure — effects are *data* (descriptors),
 * not calls — so it is trivially testable; the runtime executes them.
 */
export interface ReducerResult<S> {
  state: S;
  effects?: readonly Effect[];
}

/** A pure, total fold of an action onto state, optionally emitting effects. */
export type EffectfulReducer<S, A> = (state: S, action: A) => ReducerResult<S>;

/** An effect the runtime runs after the fold: dispatch an action to another reducer grain. */
export interface SendEffect {
  readonly kind: "send";
  readonly grain: ReducerGrain<unknown, unknown>;
  readonly key: string;
  readonly action: unknown;
}

export type Effect = SendEffect;

/** Build a typed "dispatch this action to grain `key`" effect for a reducer to return. */
export function call<S, A>(grain: ReducerGrain<S, A>, key: string, action: A): Effect {
  return { kind: "send", grain: grain as ReducerGrain<unknown, unknown>, key, action };
}

/**
 * The fixed, per-grain-agnostic surface of every reducer grain: a write channel
 * (`dispatch`) and a read-only snapshot (`query`). Because it is the *same two
 * methods* for every reducer grain, there is no per-grain method table to define
 * or generate — `S`/`A` carry all the type information.
 */
export interface ReducerClient<S, A> {
  dispatch(action: A): Promise<S>;
  query(): Promise<S>;
}

/** A registered reducer grain: usable as a `GrainInterface` for `getGrain`, and carrying its ctor. */
export interface ReducerGrain<S, A> extends GrainInterface<ReducerClient<S, A>> {
  /** The implementation constructor, for `registerGrain(grain.grain, { interfaces: [grain] })`. */
  readonly grain: new () => Grain;
}

export interface DefineReducerGrainOptions<S, A> extends DefineGrainOptions {
  /** The state before any action has been folded in. */
  initial: () => S;
  /** Pure fold of an action onto state, optionally emitting effects. */
  reduce: EffectfulReducer<S, A>;
  /** Storage state name; defaults to the grain name. */
  stateName?: string;
  /** Storage provider name; defaults to the silo's default provider. */
  provider?: string;
}

/**
 * Register a grain whose entire public surface is `dispatch(action)` + `query()`,
 * driven by a pure reducer (see [ADR 0010](../../docs/adr/0010-message-dispatch-reducer-grains.md)).
 * The single fixed interface removes the per-grain method table that would
 * otherwise be hand-written or generated; the `Action` union is the protocol.
 *
 * The folded state is persisted as a snapshot through the existing `GrainStorage`
 * (so a storage provider must be configured), and effects the reducer returns run
 * after the fold under the same single-turn guarantees as any other grain call.
 */
export function defineReducerGrain<S, A>(
  name: string,
  options: DefineReducerGrainOptions<S, A>,
): ReducerGrain<S, A> {
  const { initial, reduce, stateName = name, provider, ...grainOptions } = options;

  const iface = defineGrainInterface<ReducerClient<S, A>>(name, {
    options: { query: { readOnly: true } },
  });

  const ctor = defineGrain<ReducerClient<S, A>>(
    name,
    (ctx: GrainSetup) => {
      const store = usePersistentState<S>(ctx, stateName, {
        defaultValue: initial,
        ...(provider !== undefined ? { provider } : {}),
      });

      const dispatch = async (action: A): Promise<S> => {
        const previous = store.value;
        const result = reduce(previous, action);
        if (result.state !== previous) {
          store.value = result.state;
          await store.write(); // snapshot persist; events/effects are transient
        }
        for (const effect of result.effects ?? []) {
          await ctx.getGrain(effect.grain, effect.key).dispatch(effect.action);
        }
        return store.value;
      };

      const query = async (): Promise<S> => store.value;

      return { dispatch, query };
    },
    grainOptions,
  );

  return Object.assign(iface, { grain: ctor }) as ReducerGrain<S, A>;
}
