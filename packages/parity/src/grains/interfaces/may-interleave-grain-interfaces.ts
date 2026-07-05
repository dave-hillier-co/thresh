// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IStatelessWorkerWithMayInterleaveGrain.cs
// @ v10.1.0 (MIT). Upstream signals a call's start/release via an
// `ICallbackGrainObserver` grain-observer argument (GAP-OBSERVERS: this
// framework has no grain-observer/client-object-reference mechanism), so this
// port replaces the observer with plain string tags resolved through the
// test-only signaling helpers in `may-interleave-grain.ts` — the interleaving
// behavior under test is unaffected by how a call's start/release is observed.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

export interface IMayInterleaveGrain extends GrainWithIntegerKey {
  goFast(tag: string): Promise<void>;
  goSlow(tag: string): Promise<void>;
}

export const IMayInterleaveGrain = defineGrainInterface<IMayInterleaveGrain>(
  "UnitTests.GrainInterfaces.IStatelessWorkerWithMayInterleaveGrain",
);
