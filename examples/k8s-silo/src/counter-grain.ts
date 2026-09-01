import { defineGrain, usePersistentState } from "@thresh/core/define-grain";
import type { CounterReply } from "@thresh/example-k8s-silo/interfaces";

interface CounterState {
  value: number;
}

/** The pod this process runs on — set by the downward API (see deploy/statefulset.yaml). */
const hostName = (): string => process.env.POD_NAME ?? "local";

/**
 * A trivial durable counter. Its single activation lives on whichever silo the
 * directory places it on, regardless of which pod handled the HTTP request, so
 * incrementing through any pod hits the same activation — and the activation
 * moves to a survivor when its host pod dies. State is persisted (Redis in the
 * cluster), so reactivation resumes from the stored value rather than starting
 * over.
 *
 * The definition is its own interface — `getGrain(CounterGrain, key)` sees
 * exactly the `increment` / `current` surface below, inferred from what the
 * factory returns.
 */
export const CounterGrain = defineGrain(
  "Counter",
  () => {
    const state = usePersistentState<CounterState>("counter", {
      defaultValue: (): CounterState => ({ value: 0 }),
    });

    return {
      increment: async (): Promise<CounterReply> => {
        state.value.value += 1;
        await state.write();
        return { value: state.value.value, host: hostName() };
      },

      current: async (): Promise<CounterReply> => ({
        value: state.value.value,
        host: hostName(),
      }),
    };
  },
  { interfaceOptions: { current: { readOnly: true } } },
);
