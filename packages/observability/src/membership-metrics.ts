import { metrics, type ObservableResult } from "@opentelemetry/api";

/** What a membership watch (the Kubernetes endpoint watch) reports about its own connection. */
export interface MembershipWatchSource {
  /** Cumulative failed closes and failed relists. */
  reconnectFailures: number;
  /** When the watch last relisted or applied an event, or `undefined` before the first. */
  lastSyncedAtMs: number | undefined;
  /** The watch's clock now, so the age is measured on the clock `lastSyncedAtMs` was read from. */
  nowMs: number;
}

/** Live silo values the membership instruments sample at collection time. */
export interface MembershipMetricSources {
  /** The membership view version this silo has applied (its ring and directory run on it). */
  appliedViewVersion: () => number;
  /** Silos in the view, whatever their status. */
  memberCount: () => number;
  /** Silos in the view that are `active`. */
  activeMemberCount: () => number;
  /** The membership watch's connection state; omit for a membership with no watch. */
  watch?: () => MembershipWatchSource;
}

/**
 * Register OpenTelemetry observable instruments for membership, which a call filter cannot see:
 * `thresh.membership.view.version`, `thresh.membership.members`, `thresh.membership.members.active`
 * and, when the membership has a watch, `thresh.membership.watch.reconnect.failures` (a cumulative
 * counter) and `thresh.membership.watch.last_sync.age` (seconds). A Kubernetes API outage freezes
 * every silo's view without failing any call, so a stale `last_sync.age` and a climbing failure
 * counter are the signal; readiness deliberately does not follow them. No-op without an SDK.
 * Returns a function that unregisters them (call it when the silo stops).
 */
export function registerMembershipMetrics(sources: MembershipMetricSources): () => void {
  const meter = metrics.getMeter("@thresh/observability");
  const unregister: Array<() => void> = [];

  type ObservableInstrument = {
    addCallback: (cb: (result: ObservableResult) => void) => void;
    removeCallback: (cb: (result: ObservableResult) => void) => void;
  };
  const observe = (
    instrument: ObservableInstrument,
    callback: (result: ObservableResult) => void,
  ): void => {
    instrument.addCallback(callback);
    unregister.push(() => instrument.removeCallback(callback));
  };

  observe(
    meter.createObservableGauge("thresh.membership.view.version", {
      description: "The membership view version this silo has applied",
      unit: "{version}",
    }),
    (result) => result.observe(sources.appliedViewVersion()),
  );
  observe(
    meter.createObservableGauge("thresh.membership.members", {
      description: "Silos in this silo's membership view",
      unit: "{silo}",
    }),
    (result) => result.observe(sources.memberCount()),
  );
  observe(
    meter.createObservableGauge("thresh.membership.members.active", {
      description: "Active silos in this silo's membership view",
      unit: "{silo}",
    }),
    (result) => result.observe(sources.activeMemberCount()),
  );

  const { watch } = sources;
  if (watch !== undefined) {
    observe(
      meter.createObservableCounter("thresh.membership.watch.reconnect.failures", {
        description: "Failed membership-watch closes and relists",
        unit: "{failure}",
      }),
      (result) => result.observe(watch().reconnectFailures),
    );
    observe(
      meter.createObservableGauge("thresh.membership.watch.last_sync.age", {
        description: "Seconds since the membership watch last synced with its source",
        unit: "s",
      }),
      (result) => {
        const { lastSyncedAtMs, nowMs } = watch();
        if (lastSyncedAtMs !== undefined) result.observe((nowMs - lastSyncedAtMs) / 1000);
      },
    );
  }

  return () => unregister.forEach((fn) => fn());
}
