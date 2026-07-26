// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IStatelessWorkerWithMayInterleaveGrain.cs
// @ v10.1.0 (MIT). Upstream signals a call's start/release via an
// `ICallbackGrainObserver` grain-observer argument. This port instead uses
// plain string tags resolved through the test-only signaling helpers in
// `may-interleave-grain.ts`: the interleaving behavior under test is unaffected
// by how a call's start/release is observed, and the string-tag form is the
// simpler equivalent. (A client-object-reference observer mechanism does now
// exist — see `client.createObjectReference` — but routing start/release
// through it would add wire round-trips without changing what is tested.)
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithIntegerKey } from "@thresh/core/key-kinds";

export interface IMayInterleaveGrain extends GrainWithIntegerKey {
  goFast(tag: string): Promise<void>;
  goSlow(tag: string): Promise<void>;
}

export const IMayInterleaveGrain = defineGrainInterface<IMayInterleaveGrain>(
  "UnitTests.GrainInterfaces.IStatelessWorkerWithMayInterleaveGrain",
);
