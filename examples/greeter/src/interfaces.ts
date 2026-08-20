import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** The smallest useful grain: it greets, and counts how often it has greeted. */
export type Greeter = GrainKey<string> & {
  greet(name: string): Promise<string>;
  /** How many greetings this activation has served (volatile, per-activation). */
  greetings(): Promise<number>;
};

export const greeter = defineGrainInterface<Greeter>("example.greeter", {
  options: { greetings: { readOnly: true } },
});
