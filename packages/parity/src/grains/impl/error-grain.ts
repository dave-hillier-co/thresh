// Ported from dotnet/orleans test/Grains/TestInternalGrains/ErrorGrain.cs @ v10.1.0 (MIT).
// Trimmed to the members IErrorGrain declares (see error-grain-interfaces.ts).
import { grain } from "@tsva/core/decorators";
import { SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { IErrorGrain } from "@tsva/parity/grains/interfaces/error-grain-interfaces";

export { IErrorGrain };

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

@grain({ name: "UnitTests.Grains.ErrorGrain" })
export class ErrorGrain extends SimpleGrain implements IErrorGrain {
  private counter = 0;

  async getAxBError(): Promise<number> {
    throw new Error("GetAxBError-Exception");
  }

  async longMethodWithError(waitTimeMs: number): Promise<void> {
    await delay(waitTimeMs);
    throw new Error("LongMethodWithError");
  }

  async delayMethod(milliseconds: number): Promise<void> {
    this.counter++;
    await delay(milliseconds);
  }

  async addChildren(_children: IErrorGrain[]): Promise<void> {
    // no-op, as upstream
  }
}
