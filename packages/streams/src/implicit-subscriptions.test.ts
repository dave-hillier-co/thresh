import { describe, expect, it } from "vitest";
import type { GrainType } from "@tsva/core/grain-type";
import { implicitSubscriberIds } from "@tsva/streams/implicit-subscriptions";

describe("implicitSubscriberIds", () => {
  const table: Record<string, GrainType[]> = {
    chat: ["ChatArchive", "Moderator"],
    telemetry: ["Aggregator"],
  };
  const typesFor = (ns: string): GrainType[] => table[ns] ?? [];

  it("synthesizes one grain id per implicitly-subscribed type, keyed by the stream key", () => {
    const ids = implicitSubscriberIds("chat/general", typesFor);
    expect(ids.map((g) => g.toString())).toEqual(["ChatArchive/general", "Moderator/general"]);
  });

  it("returns none when no type is subscribed to the namespace", () => {
    expect(implicitSubscriberIds("unknown/x", typesFor)).toEqual([]);
  });

  it("keeps the full key when it contains slashes", () => {
    const ids = implicitSubscriberIds("telemetry/region/eu/dev-1", typesFor);
    expect(ids.map((g) => g.toString())).toEqual(["Aggregator/region/eu/dev-1"]);
  });

  it("ignores a malformed stream key without a namespace separator", () => {
    expect(implicitSubscriberIds("nokey", typesFor)).toEqual([]);
  });
});
