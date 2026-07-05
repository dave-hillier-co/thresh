// Ported from dotnet/orleans test/Grains/TestGrains/ChainedGrain.cs @ v10.1.0 (MIT).
// Upstream is a stateful storage-backed grain; the ported tests only observe
// state within one activation, so an in-memory field suffices (see
// echo-task-grain.ts for the same simplification).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import {
  IChainedGrain,
  type ChainGrainHolder,
} from "@tsva/parity/grains/interfaces/chained-grain-interfaces";

export { IChainedGrain };
export type { ChainGrainHolder };

@grain({ name: "UnitTests.Grains.ChainedGrain" })
export class ChainedGrain extends Grain implements IChainedGrain {
  private grainId = 0;
  private x = 0;
  private next: IChainedGrain | null = null;

  async getId(): Promise<number> {
    return this.grainId;
  }

  async getX(): Promise<number> {
    return this.x;
  }

  async getNext(): Promise<IChainedGrain | null> {
    return this.next;
  }

  async getCalculatedValue(): Promise<number> {
    if (this.next === null) return this.x;
    return this.x + (await this.next.getCalculatedValue());
  }

  async setNext(next: IChainedGrain | null): Promise<void> {
    this.next = next;
  }

  async setNextNested(next: ChainGrainHolder): Promise<void> {
    this.next = next.next;
  }

  async validate(nextIsSet: boolean): Promise<void> {
    if ((nextIsSet && this.next !== null) || (!nextIsSet && this.next === null)) return;
    throw new Error(`ChainGrain Id=${this.grainId} is in an invalid state. Next=${this.next}`);
  }

  async passThis(next: IChainedGrain): Promise<void> {
    await next.setNext(this.selfReference());
  }

  async passNull(next: IChainedGrain): Promise<void> {
    await next.setNext(null);
  }

  async passThisNested(next: ChainGrainHolder): Promise<void> {
    await next.next!.setNextNested({ next: this.selfReference() });
  }

  async passNullNested(next: ChainGrainHolder): Promise<void> {
    await next.next!.setNextNested({ next: null });
  }

  private selfReference(): IChainedGrain {
    return this.getGrain(IChainedGrain, this.id.key as bigint);
  }
}
