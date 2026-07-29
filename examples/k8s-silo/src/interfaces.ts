/** A counter read: its current value and the silo (pod) the activation runs on. */
export interface CounterReply {
  value: number;
  /** The pod hosting this activation — the signal the e2e uses to observe reactivation. */
  host: string;
}
