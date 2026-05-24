/**
 * A silo's network identity, derived from its Kubernetes pod. `podName` is
 * stable across restarts (used for the directory ring); `podUid` distinguishes
 * a fresh incarnation so stale entries from a dead pod are recognised.
 */
export class SiloAddress {
  constructor(
    readonly podName: string,
    readonly podUid: string,
    readonly endpoint: string,
  ) {}

  /** Stable ring key: the pod name, which survives restarts. */
  get ringKey(): string {
    return this.podName;
  }

  toString(): string {
    return `${this.podName}#${this.podUid}@${this.endpoint}`;
  }

  equals(other: SiloAddress): boolean {
    return (
      this.podName === other.podName &&
      this.podUid === other.podUid &&
      this.endpoint === other.endpoint
    );
  }
}
