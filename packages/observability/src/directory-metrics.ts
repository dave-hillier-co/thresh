import { metrics, type Attributes, type Counter, type Histogram } from "@opentelemetry/api";

/**
 * OpenTelemetry instruments for the grain directory (`@thresh/directory`):
 * `thresh.directory.lookups` and `thresh.directory.registrations`, each tagged
 * `thresh.directory.locality` (`"local"` when this silo owns the grain's
 * partition, `"remote"` when the call routed to a peer). Distinct from the
 * location-cache hit/miss gauge in `runtime-metrics.ts`, which measures the
 * caller-side cache in front of the directory, not the directory itself. The partition's view
 * changes report through the rest: how long requests wait on a range lock, ranges filled by
 * snapshot transfer, recoveries started and completed, recovery requests retried, and recoveries
 * abandoned when `directoryRecoveryPatience` elapsed.
 * Emitted through the global OpenTelemetry meter — a no-op until the host
 * registers an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  lookups: Counter;
  registrations: Counter;
  rangeLockWait: Histogram;
  snapshotTransfers: Counter;
  recoveriesStarted: Counter;
  recoveriesCompleted: Counter;
  recoveryRetries: Counter;
  recoveryPatienceExpiries: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@thresh/observability");
    instruments = {
      lookups: meter.createCounter("thresh.directory.lookups", {
        description: "Grain directory lookups",
        unit: "{lookup}",
      }),
      registrations: meter.createCounter("thresh.directory.registrations", {
        description: "Grain directory registrations",
        unit: "{registration}",
      }),
      rangeLockWait: meter.createHistogram("thresh.directory.range_lock.wait", {
        description:
          "How long a directory request or view change waited on a range lock held across a view change",
        unit: "ms",
      }),
      snapshotTransfers: meter.createCounter("thresh.directory.snapshot.transfers", {
        description: "Ranges a partition acquired by taking its previous owners' snapshots",
        unit: "{transfer}",
      }),
      recoveriesStarted: meter.createCounter("thresh.directory.recoveries.started", {
        description: "Ranges a partition began rebuilding from every member's activations",
        unit: "{recovery}",
      }),
      recoveriesCompleted: meter.createCounter("thresh.directory.recoveries.completed", {
        description: "Range recoveries that heard from every member",
        unit: "{recovery}",
      }),
      recoveryRetries: meter.createCounter("thresh.directory.recovery.retries", {
        description: "Recovery requests to a member that did not answer and were sent again",
        unit: "{request}",
      }),
      recoveryPatienceExpiries: meter.createCounter("thresh.directory.recovery.patience_expired", {
        description:
          "Range recoveries abandoned when directoryRecoveryPatience elapsed, unlocking the range without every member's answer",
        unit: "{recovery}",
      }),
    };
  }
  return instruments;
}

export type DirectoryLocality = "local" | "remote";

export function recordDirectoryLookup(locality: DirectoryLocality, attrs: Attributes = {}): void {
  instrumentsOf().lookups.add(1, { "thresh.directory.locality": locality, ...attrs });
}

export function recordDirectoryRegistration(
  locality: DirectoryLocality,
  attrs: Attributes = {},
): void {
  instrumentsOf().registrations.add(1, { "thresh.directory.locality": locality, ...attrs });
}

/** A request or view change waited `ms` for a range lock to be released. */
export function recordDirectoryRangeLockWait(ms: number, attrs: Attributes = {}): void {
  instrumentsOf().rangeLockWait.record(ms, attrs);
}

/** A gained range was filled from its previous owners' snapshots. */
export function recordDirectorySnapshotTransfer(attrs: Attributes = {}): void {
  instrumentsOf().snapshotTransfers.add(1, attrs);
}

/** A gained range began recovery from every member. */
export function recordDirectoryRecoveryStarted(attrs: Attributes = {}): void {
  instrumentsOf().recoveriesStarted.add(1, attrs);
}

/** A range recovery heard from every member and unlocked the range. */
export function recordDirectoryRecoveryCompleted(attrs: Attributes = {}): void {
  instrumentsOf().recoveriesCompleted.add(1, attrs);
}

/** A recovery request to a member that did not answer is being sent again. */
export function recordDirectoryRecoveryRetry(attrs: Attributes = {}): void {
  instrumentsOf().recoveryRetries.add(1, attrs);
}

/** `directoryRecoveryPatience` elapsed and a range was unlocked before recovery completed. */
export function recordDirectoryRecoveryPatienceExpired(attrs: Attributes = {}): void {
  instrumentsOf().recoveryPatienceExpiries.add(1, attrs);
}
