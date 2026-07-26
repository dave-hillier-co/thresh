// Ported from dotnet/orleans test/Grains/TestGrains/CatalogTestGrain.cs @ v10.1.0 (MIT).
// Upstream delays 50ms in OnActivateAsync to widen the activation race window
// under a real threadpool; the ported test still drives thousands of
// concurrent async calls without that delay, so the simplification is dropped.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { ICatalogTestGrain } from "@thresh/parity/grains/interfaces/catalog-test-grain-interfaces";

export { ICatalogTestGrain };

@grain({ name: "UnitTests.Grains.CatalogTestGrain" })
export class CatalogTestGrain extends Grain implements ICatalogTestGrain {
  async initialize(): Promise<void> {}

  async blastCallNewGrains(
    nGrains: number,
    startingKey: bigint,
    nCallsToEach: number,
  ): Promise<void> {
    const calls: Promise<void>[] = [];
    for (let i = 0; i < nGrains; i += 1) {
      const target = this.getGrain(ICatalogTestGrain, startingKey + BigInt(i));
      for (let j = 0; j < nCallsToEach; j += 1) calls.push(target.initialize());
    }
    await Promise.all(calls);
  }
}
