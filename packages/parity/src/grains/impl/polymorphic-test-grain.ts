// Ported from dotnet/orleans test/Grains/TestGrains/PolymorphicTestGrain.cs @ v10.1.0 (MIT).
// See polymorphic-interface-interfaces.ts for why `commonMethod` is omitted.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IA,
  IB,
  IC,
  ID,
  IE,
  IF,
  IPolymorphicTestGrain,
} from "@thresh/parity/grains/interfaces/polymorphic-interface-interfaces";

export { IA, IB, IC, ID, IE, IF, IPolymorphicTestGrain };

@grain({ name: "UnitTests.Grains.PolymorphicTestGrain" })
export class PolymorphicTestGrain extends Grain implements IPolymorphicTestGrain {
  async f1Method(): Promise<string> {
    return "F1";
  }
  async f2Method(): Promise<string> {
    return "F2";
  }
  async f3Method(): Promise<string> {
    return "F3";
  }
  async e1Method(): Promise<string> {
    return "E1";
  }
  async e2Method(): Promise<string> {
    return "E2";
  }
  async e3Method(): Promise<string> {
    return "E3";
  }
  async d1Method(): Promise<string> {
    return "D1";
  }
  async d2Method(): Promise<string> {
    return "D2";
  }
  async d3Method(): Promise<string> {
    return "D3";
  }
  async c1Method(): Promise<string> {
    return "C1";
  }
  async c2Method(): Promise<string> {
    return "C2";
  }
  async c3Method(): Promise<string> {
    return "C3";
  }
  async b1Method(): Promise<string> {
    return "B1";
  }
  async b2Method(): Promise<string> {
    return "B2";
  }
  async b3Method(): Promise<string> {
    return "B3";
  }
  async a1Method(): Promise<string> {
    return "A1";
  }
  async a2Method(): Promise<string> {
    return "A2";
  }
  async a3Method(): Promise<string> {
    return "A3";
  }
}
