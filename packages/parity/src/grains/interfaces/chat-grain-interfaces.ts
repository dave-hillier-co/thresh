// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/EventSourcing/IChatGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { Guid } from "@thresh/core/guid";
import type { GrainKey } from "@thresh/core/key-kinds";

// Upstream returns an `XDocument`; there is no XML type in this stack, so
// `getChat` returns the same content serialised as text and callers parse the
// bits they need with the small helpers in chat-grain-tests.test.ts.
export interface IChatGrain extends GrainKey<string> {
  getChat(): Promise<string>;
  post(guid: Guid, user: string, text: string): Promise<void>;
  delete(guid: Guid): Promise<void>;
  edit(guid: Guid, text: string): Promise<void>;
}

export const IChatGrain = defineGrainInterface<IChatGrain>("UnitTests.GrainInterfaces.IChatGrain");
