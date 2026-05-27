import { defineGrain, usePersistentState } from "@tsva/core/define-grain";
import type { CounterReply, ICounter } from "@tsva/example-k8s-silo/interfaces";

interface CounterState {
  value: number;
}

/** The pod this process runs on — set by the downward API (see deploy/statefulset.yaml). */
const hostName = (): string => process.env.POD_NAME ?? "local";

/**
 * Durable counter grain. State is persisted (Redis in the cluster), so when the
 * host pod dies the activation reactivates on a survivor and resumes from the
 * stored value rather than starting over.
 */
export const CounterGrain = defineGrain<ICounter>("Counter", (ctx) => {
  const state = usePersistentState<CounterState>(ctx, "counter", {
    defaultValue: (): CounterState => ({ value: 0 }),
  });

  return {
    increment: async (): Promise<CounterReply> => {
      state.value.value += 1;
      await state.write();
      return { value: state.value.value, host: hostName() };
    },

    current: async (): Promise<CounterReply> => ({ value: state.value.value, host: hostName() }),
  };
});
