/**
 * Where a silo keeps its durable state. One backend serves every grain-storage
 * provider name, journal provider name and the reminder table that a silo asks
 * for and does not register explicitly, so an application names its providers
 * and never chooses a database (see `SiloBuilder.useStorageBackend`).
 */
export type StorageBackend =
  | {
      readonly kind: "memory";
      /**
       * Accept in-memory storage on a multi-silo view. Each silo then holds its
       * own store, so a grain that reactivates on another silo starts empty:
       * only for clusters whose state is disposable.
       */
      readonly allowInCluster?: boolean;
    }
  | {
      readonly kind: "postgres";
      readonly connectionString: string;
      /** Prefix for the backend's tables (defaults to `"thresh"`). */
      readonly tablePrefix?: string;
    }
  | {
      readonly kind: "redis";
      readonly url: string;
      /** Namespace for the backend's keys (defaults to `"thresh"`). */
      readonly keyPrefix?: string;
    };

/** The backend's URL: `postgres://`, `postgresql://`, `redis://` or `rediss://`. Unset = memory. */
export const STORAGE_URL_ENV = "THRESH_STORAGE_URL";

/** `true` accepts in-memory storage on a multi-silo view; see `allowInCluster`. */
export const STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV = "THRESH_STORAGE_ALLOW_IN_MEMORY_CLUSTER";

type EnvSource = Readonly<Record<string, string | undefined>>;

/**
 * Resolve the backend from the environment. An unset URL is in-memory; a URL
 * that is set but blank is refused rather than silently degrading a silo to
 * non-durable storage, because that is almost always an unset Helm value or a
 * typo, not a request for memory.
 */
export function storageBackendFromEnvironment(env: EnvSource = process.env): StorageBackend {
  const url = env[STORAGE_URL_ENV];
  if (url === undefined) {
    return readBoolean(env, STORAGE_ALLOW_IN_MEMORY_CLUSTER_ENV)
      ? { kind: "memory", allowInCluster: true }
      : { kind: "memory" };
  }
  const trimmed = url.trim();
  if (trimmed.length === 0) {
    throw new Error(
      `${STORAGE_URL_ENV} is set but blank; unset it for in-memory storage or give a ` +
        `postgres:// or redis:// URL`,
    );
  }
  const scheme = trimmed.slice(0, trimmed.indexOf(":")).toLowerCase();
  switch (scheme) {
    case "postgres":
    case "postgresql":
      return { kind: "postgres", connectionString: trimmed };
    case "redis":
    case "rediss":
      return { kind: "redis", url: trimmed };
    default:
      throw new Error(
        `${STORAGE_URL_ENV} must be a postgres:// or redis:// URL, but its scheme was "${scheme}"`,
      );
  }
}

function readBoolean(env: EnvSource, name: string): boolean {
  const raw = env[name]?.trim();
  if (raw === undefined || raw.length === 0) return false;
  switch (raw.toLowerCase()) {
    case "true":
      return true;
    case "false":
      return false;
    default:
      // Loud: a typo in the one switch that says "state is disposable" must not read as "no".
      throw new Error(`${name} must be true or false, but was "${raw}"`);
  }
}
