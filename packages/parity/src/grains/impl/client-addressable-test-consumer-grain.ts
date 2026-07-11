// Ported from dotnet/orleans test/Grains/TestInternalGrains/ClientAddressableTestConsumerGrain.cs @ v10.1.0 (MIT).
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import {
  IClientAddressableTestConsumer,
  IClientAddressableTestRendezvousGrain,
  type IClientAddressableTestProducer,
} from "@tsva/parity/grains/interfaces/client-addressable-interfaces";

export { IClientAddressableTestConsumer };

@grain({ name: "UnitTests.Grains.ClientAddressableTestConsumerGrain" })
export class ClientAddressableTestConsumerGrain extends Grain implements IClientAddressableTestConsumer {
  private producer: IClientAddressableTestProducer | undefined;

  async pollProducer(): Promise<number> {
    if (this.producer === undefined) {
      throw new Error("producer is not set");
    }
    return this.producer.poll();
  }

  async setup(): Promise<void> {
    const rendezvous = this.getGrain(IClientAddressableTestRendezvousGrain, 0n);
    this.producer = await rendezvous.getProducer();
  }
}
