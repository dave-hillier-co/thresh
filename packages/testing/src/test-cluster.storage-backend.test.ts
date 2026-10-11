import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { JournaledGrain } from "@thresh/core/journaled-grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import type { StorageBackend } from "@thresh/hosting/storage-backend";
import { TestCluster } from "@thresh/testing/test-cluster";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachable(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => {});
    return undefined;
  }
}

const admin = await reachable(PG_URL);
const prefix = `thresh_test_cluster_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (admin === undefined) return;
  for (const table of [
    `${prefix}_grain_state`,
    `${prefix}_journal_logs`,
    `${prefix}_journal_entries`,
    `${prefix}_reminders`,
  ]) {
    await admin.query(`DROP TABLE IF EXISTS ${table}`);
  }
  await admin.end();
});

interface INote extends GrainKey<string> {
  set(text: string): Promise<void>;
  get(): Promise<string>;
}
const INote = defineGrainInterface<INote>("INote.test-cluster-backend", {
  options: { get: { readOnly: true } },
});

@grain()
class NoteGrain extends Grain implements INote {
  @persistentState("note", {
    provider: "notes",
    defaultValue: (): { text: string } => ({ text: "" }),
  })
  private note!: PersistentState<{ text: string }>;

  async set(text: string): Promise<void> {
    this.note.value = { text };
    await this.note.write();
  }

  async get(): Promise<string> {
    return this.note.value.text;
  }
}

interface ICounter extends GrainKey<string> {
  add(n: number): Promise<number>;
  total(): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.test-cluster-backend", {
  options: { total: { readOnly: true } },
});

@grain()
class CounterGrain extends JournaledGrain<number, number> implements ICounter {
  initialState(): number {
    return 0;
  }

  transitionState(state: number, event: number): number {
    return state + event;
  }

  async add(n: number): Promise<number> {
    this.raiseEvent(n);
    await this.confirmEvents();
    return this.state;
  }

  async total(): Promise<number> {
    return this.state;
  }
}

const grains = [
  { ctor: NoteGrain, interfaces: [INote] },
  { ctor: CounterGrain, interfaces: [ICounter] },
];

describe.skipIf(admin === undefined)("TestCluster storageBackend", () => {
  const backend: StorageBackend = {
    kind: "postgres",
    connectionString: PG_URL,
    tablePrefix: prefix,
  };

  it("keeps grain state and journals in the backend across a whole-cluster restart", async () => {
    const serviceId = `svc-${randomUUID()}`;

    const first = await TestCluster.start({
      initialSilos: 2,
      serviceId,
      storageBackend: backend,
      grains,
    });
    try {
      await first.getGrain(INote, "n").set("hello");
      await first.getGrain(ICounter, "c").add(3);
      await first.getGrain(ICounter, "c").add(4);
    } finally {
      await first.dispose();
    }

    const second = await TestCluster.start({
      initialSilos: 1,
      serviceId,
      storageBackend: backend,
      grains,
    });
    try {
      expect(await second.getGrain(INote, "n").get()).toBe("hello");
      expect(await second.getGrain(ICounter, "c").total()).toBe(7);
    } finally {
      await second.dispose();
    }
  });

  it("keeps two service ids apart in one backend", async () => {
    const a = await TestCluster.start({
      serviceId: `a-${randomUUID()}`,
      storageBackend: backend,
      grains,
    });
    try {
      await a.getGrain(INote, "shared").set("from a");
    } finally {
      await a.dispose();
    }

    const b = await TestCluster.start({
      serviceId: `b-${randomUUID()}`,
      storageBackend: backend,
      grains,
    });
    try {
      expect(await b.getGrain(INote, "shared").get()).toBe("");
    } finally {
      await b.dispose();
    }
  });
});
