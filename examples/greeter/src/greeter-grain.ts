import { defineGrain } from "@thresh/core/define-grain";
import type { IGreeter } from "@thresh/example-greeter/interfaces";

/**
 * A minimal grain showing the core actor guarantees with no providers:
 *
 * - `onActivate` runs before the first message — the greeting prefix it sets is
 *   always present, so no call ever sees an un-initialised grain.
 * - calls run as serialized turns — `count++` needs no lock and never races.
 * - the `count` is volatile activation state — after the grain deactivates while
 *   idle, the next call reactivates it fresh and the count starts again at 1.
 */
export const GreeterGrain = defineGrain<IGreeter>("Greeter", (ctx) => {
  let prefix = "uninitialised";
  let count = 0;

  return {
    onActivate: async () => {
      prefix = `[${String(ctx.id.key)}]`;
    },

    greet: async (name: string): Promise<string> => {
      count++;
      return `${prefix} Hello, ${name}! (greeting #${count})`;
    },

    greetings: async (): Promise<number> => count,
  };
});
