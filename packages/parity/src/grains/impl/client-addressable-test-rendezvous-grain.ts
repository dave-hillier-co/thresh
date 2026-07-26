// Ported from dotnet/orleans test/Grains/TestInternalGrains/ClientAddressableTestRendezvousGrain.cs @ v10.1.0 (MIT).
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import {
  IClientAddressableTestRendezvousGrain,
  type IClientAddressableTestProducer,
} from "@thresh/parity/grains/interfaces/client-addressable-interfaces";

export { IClientAddressableTestRendezvousGrain };

@grain({ name: "UnitTests.Grains.ClientAddressableTestRendezvousGrain" })
export class ClientAddressableTestRendezvousGrain
  extends Grain
  implements IClientAddressableTestRendezvousGrain
{
  private producer: IClientAddressableTestProducer | undefined;

  async getProducer(): Promise<IClientAddressableTestProducer> {
    if (this.producer === undefined) {
      throw new Error("producer is not set");
    }
    return this.producer;
  }

  async setProducer(producer: IClientAddressableTestProducer): Promise<void> {
    this.producer = producer;
  }
}
