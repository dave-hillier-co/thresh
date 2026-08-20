import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** A counter read: its current value and the silo (pod) the activation runs on. */
export interface CounterReply {
  value: number;
  /** The pod hosting this activation — the signal the e2e uses to observe reactivation. */
  host: string;
}

/**
 * A trivial durable counter. Its single activation lives on whichever silo the
 * directory places it on, regardless of which pod handled the HTTP request, so
 * incrementing through any pod hits the same activation — and the activation
 * moves to a survivor when its host pod dies.
 */
export type Counter = GrainKey<string> & {
  increment(): Promise<CounterReply>;
  current(): Promise<CounterReply>;
};

export const counter = defineGrainInterface<Counter>("k8s.counter", {
  options: { current: { readOnly: true } },
});
