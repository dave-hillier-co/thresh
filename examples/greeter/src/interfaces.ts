import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";

/** The smallest useful grain: it greets, and counts how often it has greeted. */
export interface IGreeter extends GrainWithStringKey {
  greet(name: string): Promise<string>;
  /** How many greetings this activation has served (volatile, per-activation). */
  greetings(): Promise<number>;
}

export const IGreeter = defineGrainInterface<IGreeter>("example.IGreeter", {
  methods: ["greet", "greetings"],
  options: { greetings: { readOnly: true } },
});
