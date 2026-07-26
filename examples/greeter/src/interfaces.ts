import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";

/** The smallest useful grain: it greets, and counts how often it has greeted. */
export interface IGreeter extends GrainWithStringKey {
  greet(name: string): Promise<string>;
  /** How many greetings this activation has served (volatile, per-activation). */
  greetings(): Promise<number>;
}

export const IGreeter = defineGrainInterface<IGreeter>("example.IGreeter", {
  options: { greetings: { readOnly: true } },
});
