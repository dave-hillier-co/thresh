import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

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
export interface ICounter extends GrainWithStringKey {
  increment(): Promise<CounterReply>;
  current(): Promise<CounterReply>;
}

export const ICounter = defineGrainInterface<ICounter>("k8s.ICounter", {
  options: { current: { readOnly: true } },
});
