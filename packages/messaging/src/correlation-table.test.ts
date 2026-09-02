import { describe, expect, it } from "vitest";

import { CorrelationTable } from "./correlation-table.js";
import type { Message } from "./message.js";

const response = (correlationId: bigint): Message =>
  ({ correlationId, direction: "response" }) as Message;

describe("CorrelationTable", () => {
  it("rejects only the lost peer's calls with rejectFor, leaving others pending", async () => {
    const table = new CorrelationTable();
    const toA = table.register(1n, undefined, "silo-a");
    const toB = table.register(2n, undefined, "silo-b");
    const untagged = table.register(3n);

    table.rejectFor("silo-a", new Error("connection to silo-a was lost"));

    await expect(toA).rejects.toThrow("connection to silo-a was lost");
    // The other peer's call and the untagged call are still completable.
    table.complete(response(2n));
    table.complete(response(3n));
    await expect(toB).resolves.toMatchObject({ correlationId: 2n });
    await expect(untagged).resolves.toMatchObject({ correlationId: 3n });
  });

  it("rejectFor on a peer with nothing outstanding is a no-op", async () => {
    const table = new CorrelationTable();
    const pending = table.register(1n, undefined, "silo-a");
    table.rejectFor("silo-b", new Error("lost"));
    table.complete(response(1n));
    await expect(pending).resolves.toMatchObject({ correlationId: 1n });
  });

  it("rejectAll still fails everything outstanding", async () => {
    const table = new CorrelationTable();
    const toA = table.register(1n, undefined, "silo-a");
    const untagged = table.register(2n);
    table.rejectAll(new Error("shutting down"));
    await expect(toA).rejects.toThrow("shutting down");
    await expect(untagged).rejects.toThrow("shutting down");
  });
});
