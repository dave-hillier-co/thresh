import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { IGreeter } from "@tsva/example-greeter/interfaces";

/**
 * A minimal grain showing the core actor guarantees with no providers:
 *
 * - `onActivate` runs before the first message — the greeting prefix it sets is
 *   always present, so no call ever sees an un-initialised grain.
 * - calls run as serialized turns — `count++` needs no lock and never races.
 * - the `count` is volatile activation state — after the grain deactivates while
 *   idle, the next call reactivates it fresh and the count starts again at 1.
 */
@grain()
export class GreeterGrain extends Grain implements IGreeter {
  private prefix = "uninitialised";
  private count = 0;

  override async onActivate(): Promise<void> {
    this.prefix = `[${String(this.id.key)}]`;
  }

  async greet(name: string): Promise<string> {
    this.count++;
    return `${this.prefix} Hello, ${name}! (greeting #${this.count})`;
  }

  async greetings(): Promise<number> {
    return this.count;
  }
}
