import type { createClient } from "redis";
import type { Pool } from "pg";
import { sharesJournalBacking } from "@thresh/core/journal-storage";
import { describe, expect, it } from "vitest";

import { MemoryJournalStorage } from "./memory-journal-storage";
import { PostgresJournalStorage } from "./postgres-journal-storage";
import { RedisJournalStorage } from "./redis-journal-storage";

// Construction touches no connection, so these run without Postgres or Redis.
const pool = {} as Pool;
const client = {} as ReturnType<typeof createClient>;

describe("journal provider backing identity", () => {
  it("treats two memory providers as one only when they share a backing", () => {
    const a = new MemoryJournalStorage();
    expect(sharesJournalBacking(a, new MemoryJournalStorage({ sharing: a }))).toBe(true);
    expect(sharesJournalBacking(a, new MemoryJournalStorage())).toBe(false);
  });

  it("treats Postgres providers over one pool, tables and service as one", () => {
    const a = new PostgresJournalStorage(pool);
    expect(sharesJournalBacking(a, new PostgresJournalStorage(pool))).toBe(true);
    expect(sharesJournalBacking(a, new PostgresJournalStorage({} as Pool))).toBe(false);
    expect(
      sharesJournalBacking(a, new PostgresJournalStorage(pool, { tablePrefix: "other" })),
    ).toBe(false);
    expect(sharesJournalBacking(a, new PostgresJournalStorage(pool, { serviceId: "svc" }))).toBe(
      false,
    );
  });

  it("treats Redis providers over one client, prefix and service as one", () => {
    const a = new RedisJournalStorage(client);
    expect(sharesJournalBacking(a, new RedisJournalStorage(client))).toBe(true);
    expect(sharesJournalBacking(a, new RedisJournalStorage({} as typeof client))).toBe(false);
    expect(sharesJournalBacking(a, new RedisJournalStorage(client, { keyPrefix: "x" }))).toBe(
      false,
    );
  });
});
