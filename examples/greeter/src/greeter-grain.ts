import { defineGrain, useOnActivate } from "@thresh/core/define-grain";

/**
 * A minimal grain showing the core actor guarantees with no providers:
 *
 * - the `useOnActivate` hook runs before the first message — the greeting prefix
 *   it sets is always present, so no call ever sees an un-initialised grain.
 * - calls run as serialized turns — `count++` needs no lock and never races.
 * - the `count` is volatile activation state — after the grain deactivates while
 *   idle, the next call reactivates it fresh and the count starts again at 1.
 *
 * The definition is its own interface — `getGrain(GreeterGrain, key)` sees
 * exactly the `greet` / `greetings` surface below, inferred from what the
 * factory returns.
 */
export const GreeterGrain = defineGrain(
  "Greeter",
  (ctx) => {
    let prefix = "uninitialised";
    let count = 0;

    useOnActivate(() => {
      prefix = `[${String(ctx.id.key)}]`;
    });

    return {
      greet: async (name: string): Promise<string> => {
        count++;
        return `${prefix} Hello, ${name}! (greeting #${count})`;
      },

      /** How many greetings this activation has served (volatile, per-activation). */
      greetings: async (): Promise<number> => count,
    };
  },
  { interfaceOptions: { greetings: { readOnly: true } } },
);
