interface FakeQueryResult {
  rows: Array<Record<string, unknown>>;
  rowCount: number;
}

/**
 * Minimal in-memory stand-in for `pg`'s `Pool`, scripted to enforce the one
 * piece of real Postgres behaviour that matters for `start()`-time migration
 * ordering bugs: `CREATE INDEX ... IF NOT EXISTS` validates its column list
 * BEFORE the `IF NOT EXISTS` skip applies, so an index that references a
 * column the table does not yet have raises `42703 undefined_column` even
 * though the index doesn't exist. A real Postgres does this; this fake does
 * too, on the handful of statement shapes the provider `start()`/migration
 * methods in this repo actually issue — it is not a SQL engine.
 *
 * Seed a table's starting columns via {@link seedTable} to model a table
 * that already exists (e.g. one created before a migration was added).
 */
export class FakePgPool {
  private readonly tables = new Map<string, Set<string>>();
  private readonly indexes = new Set<string>();

  /** Model a pre-existing table with the given columns, as if from a prior deploy. */
  seedTable(name: string, columns: string[]): void {
    this.tables.set(name, new Set(columns));
  }

  async query(text: string, params: unknown[] = []): Promise<FakeQueryResult> {
    const sql = text.trim();

    let m = /^CREATE TABLE IF NOT EXISTS (\w+)\s*\(([\s\S]*)\)$/i.exec(sql);
    if (m) {
      const [, table, body] = m;
      if (!this.tables.has(table!)) {
        this.tables.set(table!, new Set(this.parseColumnNames(body!)));
      }
      return { rows: [], rowCount: 0 };
    }

    m = /^CREATE INDEX IF NOT EXISTS (\S+)\s+ON (\w+)\s*\(([^)]*)\)$/i.exec(sql);
    if (m) {
      const [, indexName, table, colsRaw] = m;
      const columns = colsRaw!.split(",").map((c) => c.trim());
      const known = this.tables.get(table!);
      for (const column of columns) {
        if (known === undefined || !known.has(column)) {
          throw this.pgError("42703", `column "${column}" does not exist`);
        }
      }
      this.indexes.add(indexName!);
      return { rows: [], rowCount: 0 };
    }

    m = /^DROP INDEX IF EXISTS (\S+)$/i.exec(sql);
    if (m) {
      this.indexes.delete(m[1]!);
      return { rows: [], rowCount: 0 };
    }

    m = /^ALTER TABLE (\w+)\s+ADD COLUMN IF NOT EXISTS (\w+)/i.exec(sql);
    if (m) {
      const [, table, column] = m;
      this.tables.get(table!)?.add(column!);
      return { rows: [], rowCount: 0 };
    }

    if (/^ALTER TABLE \w+ ALTER COLUMN \w+ DROP DEFAULT$/i.test(sql)) {
      return { rows: [], rowCount: 0 };
    }

    if (/^SELECT 1 FROM information_schema\.columns/i.test(sql)) {
      const table = params[0] as string;
      const column = sql.includes("'service_id'") ? "service_id" : undefined;
      const has = column !== undefined && (this.tables.get(table)?.has(column) ?? false);
      return { rows: has ? [{ "?column?": 1 }] : [], rowCount: has ? 1 : 0 };
    }

    throw new Error(`FakePgPool: unhandled query: ${sql}`);
  }

  private parseColumnNames(body: string): string[] {
    return body
      .split(",")
      .map((line) => line.trim())
      .filter((line) => line.length > 0 && !/^PRIMARY KEY/i.test(line))
      .map((line) => line.split(/\s+/)[0]!);
  }

  private pgError(code: string, message: string): Error & { code: string } {
    const err = new Error(message) as Error & { code: string };
    err.code = code;
    return err;
  }
}
