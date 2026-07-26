// Ported from dotnet/orleans test/Grains/TestGrains/GetGrainGrains.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { IGuidGrain, IStringGrain } from "@thresh/parity/grains/interfaces/get-grain-interfaces";

export { IGuidGrain, IStringGrain };

@grain({ name: "UnitTests.Grains.StringGrain" })
export class StringGrain extends Grain implements IStringGrain {
  async foo(): Promise<boolean> {
    return true;
  }
}

@grain({ name: "UnitTests.Grains.GuidGrain" })
export class GuidGrain extends Grain implements IGuidGrain {
  async foo(): Promise<boolean> {
    return true;
  }
}
