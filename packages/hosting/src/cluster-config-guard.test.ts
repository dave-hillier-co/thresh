import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";

/**
 * A clustered silo refuses the settings that only one silo can hold (#85): a per-silo in-memory
 * reminder table, grain store or journal store, and a multi-silo static view with no liveness.
 * Each is refused at `build()`, naming what to use instead, so the configuration cannot be
 * assembled by accident; the explicit opt-ins keep `TestCluster`'s shared in-process stores and a
 * deliberately static multi-silo view possible.
 */

const local = new SiloAddress("silo-guard", "uid-guard", "silo-guard:11111");
const peer = new SiloAddress("silo-guard2", "uid-guard2", "silo-guard2:11111");

function single(): SiloBuilder {
  return createSilo({ clusterId: "c-guard", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
}

/** A multi-silo static view whose missing liveness has been acknowledged. */
function clustered(): SiloBuilder {
  return createSilo({ clusterId: "c-guard", local })
    .useStaticMembership([local, peer], undefined, { allowWithoutLiveness: true })
    .useInProcessTransport(new InProcessNetwork());
}

/** One membership view shared by every silo in this process, as `TestCluster` builds them. */
function shared(): SiloBuilder {
  return createSilo({ clusterId: "c-guard", local })
    .useMembership(new StaticMembershipService(local, [local, peer]))
    .useInProcessTransport(new InProcessNetwork());
}

describe("single-silo development", () => {
  it("accepts every in-memory default on a static view of this silo alone", () => {
    expect(() =>
      single().useMemoryStorage().useMemoryJournaling().useReminders().build(),
    ).not.toThrow();
  });
});

describe("a multi-silo static view", () => {
  it("is refused without liveness, naming the providers that supply it", () => {
    const builder = createSilo({ clusterId: "c-guard", local })
      .useStaticMembership([local, peer])
      .useInProcessTransport(new InProcessNetwork());
    expect(() => builder.build()).toThrow(/liveness/);
    expect(() => builder.build()).toThrow(/useKubernetesMembership/);
    expect(() => builder.build()).toThrow(/allowWithoutLiveness/);
  });

  it("builds when the missing liveness is acknowledged", () => {
    expect(() => clustered().build()).not.toThrow();
  });
});

describe("reminders on a clustered silo", () => {
  it("refuses the per-silo in-memory default, naming the shared tables", () => {
    const builder = clustered().useReminders();
    expect(() => builder.build()).toThrow(/reminder/);
    expect(() => builder.build()).toThrow(/usePostgresReminders/);
    expect(() => builder.build()).toThrow(/useRedisReminders/);
  });

  it("refuses an in-memory table handed in without the shared-state opt-in", () => {
    expect(() => clustered().useReminders(new MemoryReminderTable()).build()).toThrow(
      /allowSharedInMemoryState/,
    );
  });

  it("accepts an in-memory table declared shared by every silo", () => {
    expect(() =>
      clustered().allowSharedInMemoryState().useReminders(new MemoryReminderTable()).build(),
    ).not.toThrow();
  });

  it("still refuses the default table when shared state is declared: it is per-silo by construction", () => {
    expect(() => clustered().allowSharedInMemoryState().useReminders().build()).toThrow(
      /usePostgresReminders/,
    );
  });

  it("accepts a table that every silo reads", () => {
    expect(() =>
      clustered()
        .usePostgresReminders({ connectionString: "postgres://unused.invalid/db" })
        .build(),
    ).not.toThrow();
  });

  it("refuses an in-memory table on a shared membership view too", () => {
    expect(() => shared().useReminders(new MemoryReminderTable()).build()).toThrow(
      /allowSharedInMemoryState/,
    );
  });
});

describe("grain storage on a clustered silo", () => {
  it("refuses the per-silo in-memory default, even with shared state declared", () => {
    expect(() => clustered().useMemoryStorage().build()).toThrow(/storage provider "default"/);
    expect(() => clustered().allowSharedInMemoryState().useMemoryStorage().build()).toThrow(
      /storage provider "default"/,
    );
  });

  it("refuses an in-memory provider handed in under any name without the opt-in", () => {
    expect(() => clustered().addStorage("ledger", new MemoryGrainStorage()).build()).toThrow(
      /storage provider "ledger"/,
    );
  });

  it("accepts in-memory providers declared shared by every silo", () => {
    expect(() =>
      clustered()
        .allowSharedInMemoryState()
        .useMemoryStorage(new MemoryGrainStorage())
        .addStorage("ledger", new MemoryGrainStorage())
        .build(),
    ).not.toThrow();
  });
});

describe("journal storage on a clustered silo", () => {
  it("refuses the per-silo in-memory default", () => {
    expect(() => clustered().useMemoryJournaling().build()).toThrow(/journal provider "default"/);
  });

  it("refuses an in-memory journal handed in without the opt-in", () => {
    expect(() => clustered().addJournaling("log", new MemoryJournalStorage()).build()).toThrow(
      /journal provider "log"/,
    );
  });

  it("accepts an in-memory journal declared shared by every silo", () => {
    expect(() =>
      clustered()
        .allowSharedInMemoryState()
        .useMemoryJournaling(new MemoryJournalStorage())
        .build(),
    ).not.toThrow();
  });
});

describe("the in-memory storage backend on a clustered silo", () => {
  it("keeps its own opt-in, which also admits the backend's per-silo reminder table", () => {
    expect(() =>
      clustered()
        .useStorageBackend({ kind: "memory", allowInCluster: true })
        .useReminders()
        .build(),
    ).not.toThrow();
  });

  it("is not admitted by the shared-state opt-in: the builder made its stores, one per silo", () => {
    expect(() =>
      clustered().allowSharedInMemoryState().useStorageBackend({ kind: "memory" }).build(),
    ).toThrow(/THRESH_STORAGE_URL/);
  });
});
