// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/DefaultToleranceTests.cs
// (the nested grain interfaces/classes) @ v10.1.0 (MIT). Split into its own
// module so `default-tolerance-tests.test.ts` stays focused on the ported
// test bodies.
//
// `Receivers_ShouldMoveCloseTo_PullingAgent`'s upstream scenario relies on
// Orleans' streaming `PullingAgent` being affinitized to a specific silo by
// queue ownership, which this port does not model (no per-silo pulling-agent
// substrate). Substituted here with an explicit placement hint in
// `SP.firstPing`, forcing the receiver (`SR`) to activate on the same silo as
// the sending pulling grain (`SP`) — the same OBSERVABLE initial placement
// the upstream fixture asserts on — while the interesting part (the
// repartitioner recognising the `SP -> SR` call edge and moving `SR` closer
// to `SP` after `triggerRepartitionExchange`) stays faithful to the real
// protocol.
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import type { SiloAddress } from "@thresh/core/silo-address";

// ── A / B / C / CImmovable / D (Scenarios 1-4) ──────────────────────────────

export interface IBase extends GrainKey<string> {
  ping(scenario: string): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}

export interface IA extends IBase {
  firstPing(scenario: string, silo1: string, silo2: string): Promise<void>;
}
export const IA = defineGrainInterface<IA>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.IA",
);

export type IB = IBase;
export const IB = defineGrainInterface<IB>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.IB",
);

export interface IC extends IBase {
  pingWithHint(scenario: string, silo2: string): Promise<void>;
}
export const IC = defineGrainInterface<IC>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.IC",
);

export type ICImmovable = IBase;
export const ICImmovable = defineGrainInterface<ICImmovable>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.ICImmovable",
);

export type ID = IBase;
export const ID = defineGrainInterface<ID>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.ID",
);

abstract class GrainBase extends Grain {
  async getAddress(): Promise<SiloAddress> {
    return this.runtime.localSiloAddress();
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.A" })
export class A extends GrainBase implements IA {
  private silo1 = "";
  private silo2 = "";

  async firstPing(scenario: string, silo1: string, silo2: string): Promise<void> {
    this.silo1 = silo1;
    this.silo2 = silo2;

    switch (scenario) {
      case "1":
        RequestContext.set(PLACEMENT_HINT_KEY, this.silo2);
        await this.getGrain(IB, `b${scenario}`).ping(scenario);
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        return;
      case "2":
        RequestContext.set(PLACEMENT_HINT_KEY, this.silo2);
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        return;
      case "3":
        RequestContext.set(PLACEMENT_HINT_KEY, this.silo2);
        await this.getGrain(ICImmovable, `c${scenario}`).ping(scenario);
        return;
      case "4":
        RequestContext.set(PLACEMENT_HINT_KEY, this.silo1);
        await this.getGrain(IB, `b${scenario}`).ping(scenario);
        RequestContext.set(PLACEMENT_HINT_KEY, this.silo2);
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        await this.getGrain(IC, `c${scenario}`).pingWithHint(scenario, this.silo2);
        return;
      default:
        throw new Error(`unsupported scenario ${scenario}`);
    }
  }

  async ping(scenario: string): Promise<void> {
    switch (scenario) {
      case "1":
        await this.getGrain(IB, `b${scenario}`).ping(scenario);
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        return;
      case "2":
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        return;
      case "3":
        await this.getGrain(ICImmovable, `c${scenario}`).ping(scenario);
        return;
      case "4":
        await this.getGrain(IB, `b${scenario}`).ping(scenario);
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        await this.getGrain(IC, `c${scenario}`).pingWithHint(scenario, this.silo2);
        return;
      default:
        throw new Error(`unsupported scenario ${scenario}`);
    }
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.B" })
export class B extends GrainBase implements IB {
  async ping(scenario: string): Promise<void> {
    switch (scenario) {
      case "1":
      case "4":
        return;
      case "2":
        await this.getGrain(IC, `c${scenario}`).ping(scenario);
        return;
      case "3":
        await this.getGrain(ICImmovable, `c${scenario}`).ping(scenario);
        return;
      default:
        throw new Error(`unsupported scenario ${scenario}`);
    }
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.C" })
export class C extends GrainBase implements IC {
  async ping(scenario: string): Promise<void> {
    switch (scenario) {
      case "1":
        await this.getGrain(IB, `b${scenario}`).ping(scenario);
        return;
      case "2":
      case "3":
      case "4":
        return;
      default:
        throw new Error(`unsupported scenario ${scenario}`);
    }
  }

  async pingWithHint(scenario: string, silo2: string): Promise<void> {
    if (scenario !== "4") throw new Error(`unsupported scenario ${scenario}`);
    RequestContext.set(PLACEMENT_HINT_KEY, silo2);
    await this.getGrain(ID, `d${scenario}`).ping(scenario);
  }
}

@grain({
  name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.CImmovable",
  immovable: "repartitioner",
})
export class CImmovable extends GrainBase implements ICImmovable {
  async ping(scenario: string): Promise<void> {
    if (scenario !== "3") throw new Error(`unsupported scenario ${scenario}`);
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.D" })
export class D extends GrainBase implements ID {
  async ping(scenario: string): Promise<void> {
    if (scenario !== "4") throw new Error(`unsupported scenario ${scenario}`);
  }
}

// ── PullingAgent / SR (Receivers_ShouldMoveCloseTo_PullingAgent) ───────────
//
// Upstream drives this scenario through Orleans' real streaming
// `PullingAgent`: a per-queue system component, itself placement-affinitized
// by queue ownership, that delivers stream items to implicit subscribers —
// so the repartitioner's message sink sees `PullingAgent -> SR` delivery
// edges and migrates each `SR` toward wherever its agent lives. This port's
// memory stream provider delivers by direct in-process push with no
// pulling-agent substrate at all (see `MemoryStreamProvider`'s class doc), so
// there is no equivalent component whose OWN placement to migrate `SR`
// towards. Substituted with a plain stand-in `PullingAgent` grain occupying
// the same architectural role — forced onto Silo2 (mirroring the upstream
// comment that the real pulling agent ends up there) — which each `SR` calls
// a few times, generating the same shape of communication edge the real
// protocol reasons about. The interesting part (the repartitioner moving the
// receivers close to their agent) stays faithful to the real protocol.
export interface IPullingAgent extends GrainKey<string> {
  ping(): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}
export const IPullingAgent = defineGrainInterface<IPullingAgent>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.IPullingAgent",
);

export interface ISR extends GrainKey<string> {
  pingAgent(): Promise<void>;
  getAddress(): Promise<SiloAddress>;
}
export const ISR = defineGrainInterface<ISR>(
  "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.ISR",
);

@grain({
  name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.PullingAgent",
  immovable: "repartitioner",
})
export class PullingAgent extends GrainBase implements IPullingAgent {
  async ping(): Promise<void> {
    return;
  }
}

@grain({ name: "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.SR" })
export class SR extends GrainBase implements ISR {
  async pingAgent(): Promise<void> {
    await this.getGrain(IPullingAgent, "pa").ping();
  }
}
