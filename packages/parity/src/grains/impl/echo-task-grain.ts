// Ported from dotnet/orleans test/Grains/TestInternalGrains/EchoTaskGrain.cs @ v10.1.0 (MIT).
// Upstream keeps LastEcho in Grain<EchoTaskGrainState> over MemoryStore; the
// ported tests only observe state within one activation, so instance fields
// suffice. The Blocking grains' CallMethod* helpers call the echo grains with
// `this.GetPrimaryKey()` — here a guid derived from the integer key.
import { grain, reentrant } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { Guid } from "@tsva/core/guid";
import {
  IBlockingEchoTaskGrain,
  IEchoGrain,
  IEchoTaskGrain,
  IReentrantBlockingEchoTaskGrain,
} from "@tsva/parity/grains/interfaces/echo-task-grain-interfaces";

export { IBlockingEchoTaskGrain, IEchoGrain, IEchoTaskGrain, IReentrantBlockingEchoTaskGrain };

/** A stable guid for an integer grain key (upstream: legacy key-to-guid embedding). */
export function guidForKey(key: bigint): Guid {
  const hex = key.toString(16).padStart(12, "0").slice(-12);
  return Guid.parse(`00000000-0000-0000-0000-${hex}`);
}

@grain({ name: "UnitTests.Grains.EchoGrain" })
export class EchoGrain extends Grain implements IEchoGrain {
  private lastEcho = "";

  async getLastEcho(): Promise<string> {
    return this.lastEcho;
  }

  async echo(data: string): Promise<string> {
    this.lastEcho = data;
    return data;
  }

  async echoError(data: string): Promise<string> {
    this.lastEcho = data;
    throw new Error(data);
  }

  async echoNullable(value: Date | null): Promise<Date | null> {
    return value;
  }
}

@grain({ name: "UnitTests.Grains.EchoTaskGrain" })
export class EchoTaskGrain extends Grain implements IEchoTaskGrain {
  private myId = 0;
  private lastEcho = "";

  async getMyIdAsync(): Promise<number> {
    return this.myId;
  }

  async getLastEchoAsync(): Promise<string> {
    return this.lastEcho;
  }

  async echoAsync(data: string): Promise<string> {
    this.lastEcho = data;
    return data;
  }

  async echoErrorAsync(data: string): Promise<string> {
    this.lastEcho = data;
    throw new Error(data);
  }

  async pingAsync(): Promise<void> {
    return undefined;
  }
}

@grain({ name: "UnitTests.Grains.BlockingEchoTaskGrain" })
export class BlockingEchoTaskGrain extends Grain implements IBlockingEchoTaskGrain {
  private myId = 0;
  private lastEcho = "";

  async getMyId(): Promise<number> {
    return this.myId;
  }

  async getLastEcho(): Promise<string> {
    return this.lastEcho;
  }

  async echo(data: string): Promise<string> {
    this.lastEcho = data;
    return data;
  }

  async callMethodTask_Await(data: string): Promise<string> {
    const avGrain = this.getGrain(IEchoTaskGrain, guidForKey(this.id.key as bigint));
    return avGrain.echoAsync(data);
  }

  async callMethodAV_Await(data: string): Promise<string> {
    const avGrain = this.getGrain(IEchoGrain, guidForKey(this.id.key as bigint));
    return avGrain.echo(data);
  }
}

@reentrant()
@grain({ name: "UnitTests.Grains.ReentrantBlockingEchoTaskGrain" })
export class ReentrantBlockingEchoTaskGrain
  extends Grain
  implements IReentrantBlockingEchoTaskGrain
{
  private myId = 0;
  private lastEcho = "";

  async getMyId(): Promise<number> {
    return this.myId;
  }

  async getLastEcho(): Promise<string> {
    return this.lastEcho;
  }

  async echo(data: string): Promise<string> {
    this.lastEcho = data;
    return data;
  }

  async callMethodTask_Await(data: string): Promise<string> {
    const avGrain = this.getGrain(IEchoTaskGrain, guidForKey(this.id.key as bigint));
    return avGrain.echoAsync(data);
  }

  async callMethodAV_Await(data: string): Promise<string> {
    const avGrain = this.getGrain(IEchoGrain, guidForKey(this.id.key as bigint));
    return avGrain.echo(data);
  }
}
