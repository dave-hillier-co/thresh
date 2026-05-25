import { grain, persistentState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { PersistentState } from "@tsva/core/persistent-state";
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
@grain()
export class CounterGrain extends Grain implements ICounter {
  @persistentState("counter", { defaultValue: (): CounterState => ({ value: 0 }) })
  private state!: PersistentState<CounterState>;

  async increment(): Promise<CounterReply> {
    this.state.value.value += 1;
    await this.state.write();
    return { value: this.state.value.value, host: hostName() };
  }

  async current(): Promise<CounterReply> {
    return { value: this.state.value.value, host: hostName() };
  }
}
