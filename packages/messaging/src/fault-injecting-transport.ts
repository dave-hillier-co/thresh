import { RejectionError } from "@thresh/core/errors";
import type { SiloAddress } from "@thresh/core/silo-address";
import { systemTimeProvider, type TimeProvider } from "@thresh/core/time-provider";
import type { Message } from "@thresh/messaging/message";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";

/**
 * Names one end of a link: a `SiloAddress`, or a string matched against a silo's pod name or
 * its endpoint.
 */
export type SiloSelector = string | SiloAddress;

/** A directed link between silos; an end left out matches any silo. */
export interface Link {
  readonly from?: SiloSelector;
  readonly to?: SiloSelector;
}

/** What happens to one message on a link. */
export type Verdict = "deliver" | "drop" | { readonly delayMs: number };

/** A fault that is in force until healed. */
export interface FaultHandle {
  heal(): void;
}

interface CountedRule {
  readonly link: Link;
  /** Messages (or connects) the rule still applies to; `undefined` = until healed. */
  remaining: number | undefined;
}

interface DelayRule extends CountedRule {
  readonly delayMs: number;
}

interface Partition {
  readonly sideA: readonly SiloSelector[];
  readonly sideB: readonly SiloSelector[];
}

interface Schedule {
  readonly link: Link;
  readonly verdicts: Verdict[];
}

function matches(selector: SiloSelector | undefined, silo: SiloAddress): boolean {
  if (selector === undefined) return true;
  if (typeof selector === "string") return selector === silo.podName || selector === silo.endpoint;
  return selector.equals(silo);
}

function onLink(link: Link, from: SiloAddress, to: SiloAddress): boolean {
  return matches(link.from, from) && matches(link.to, to);
}

/**
 * The faults a set of {@link FaultInjectingTransport}s inject, shared by every silo's transport so
 * a test drives the whole network from one place. Every fault is explicit — set by a direct call
 * or laid out as a per-message {@link schedule} — and delays run on the injected clock, so a run
 * is reproducible: nothing here draws on randomness or the wall clock.
 *
 * Faults are applied by the SENDING side's transport (a message is judged once, where it leaves),
 * so both ends of a link must be behind a `FaultInjectingTransport` sharing this object for a
 * fault to reach replies sent down an accepted connection as well as requests.
 *
 * Precedence for one message: a partition between its ends drops it; otherwise the next verdict
 * of the first schedule on its link decides; otherwise the first drop rule, then the first delay
 * rule; otherwise it is delivered.
 */
export class NetworkFaults {
  private readonly drops: CountedRule[] = [];
  private readonly delays: DelayRule[] = [];
  private readonly connectFailures: CountedRule[] = [];
  private readonly partitions: Partition[] = [];
  private readonly schedules: Schedule[] = [];
  private droppedCount = 0;
  private delayedCount = 0;
  private refusedCount = 0;

  constructor(readonly time: TimeProvider = systemTimeProvider) {}

  /** Messages dropped so far. */
  get dropped(): number {
    return this.droppedCount;
  }

  /** Messages delayed so far. */
  get delayed(): number {
    return this.delayedCount;
  }

  /** Connects refused so far. */
  get refusedConnects(): number {
    return this.refusedCount;
  }

  /** Drop messages on `link` — every one until healed, or only the next `count`. */
  drop(link: Link, options: { count?: number } = {}): FaultHandle {
    return this.add(this.drops, { link, remaining: options.count });
  }

  /** Delay messages on `link` by `delayMs` on this object's clock. */
  delay(link: Link, delayMs: number, options: { count?: number } = {}): FaultHandle {
    return this.add(this.delays, { link, delayMs, remaining: options.count });
  }

  /** Refuse connects on `link` with the transport's rejection (`unknownTarget`). */
  failConnects(link: Link, options: { count?: number } = {}): FaultHandle {
    return this.add(this.connectFailures, { link, remaining: options.count });
  }

  /**
   * Cut every silo in `sideA` off from every silo in `sideB`, both ways: messages across are
   * dropped and connects across are refused. Links within a side are untouched.
   */
  partition(sideA: readonly SiloSelector[], sideB: readonly SiloSelector[]): FaultHandle {
    return this.add(this.partitions, { sideA: [...sideA], sideB: [...sideB] });
  }

  /**
   * Decide the next messages on `link` one at a time, in order: each message takes the next
   * verdict, and once the list is used up the link falls back to the standing rules.
   */
  schedule(link: Link, verdicts: readonly Verdict[]): FaultHandle {
    return this.add(this.schedules, { link, verdicts: [...verdicts] });
  }

  /** Remove every fault. */
  healAll(): void {
    for (const list of [
      this.drops,
      this.delays,
      this.connectFailures,
      this.partitions,
      this.schedules,
    ] as unknown[][]) {
      list.length = 0;
    }
  }

  /** Whether a connect from `from` to `to` is refused; consumes a counted rule if so. */
  refusesConnect(from: SiloAddress, to: SiloAddress): boolean {
    if (this.partitioned(from, to) || this.consume(this.connectFailures, from, to) !== undefined) {
      this.refusedCount += 1;
      return true;
    }
    return false;
  }

  /** The verdict for one message from `from` to `to`; consumes whatever rule decided it. */
  verdictFor(from: SiloAddress, to: SiloAddress): Verdict {
    const verdict = this.decide(from, to);
    if (verdict === "drop") this.droppedCount += 1;
    else if (verdict !== "deliver") this.delayedCount += 1;
    return verdict;
  }

  private decide(from: SiloAddress, to: SiloAddress): Verdict {
    if (this.partitioned(from, to)) return "drop";
    for (const schedule of this.schedules) {
      if (!onLink(schedule.link, from, to)) continue;
      const next = schedule.verdicts.shift();
      if (schedule.verdicts.length === 0) this.remove(this.schedules, schedule);
      if (next !== undefined) return next;
    }
    if (this.consume(this.drops, from, to) !== undefined) return "drop";
    const delay = this.consume(this.delays, from, to);
    if (delay !== undefined) return { delayMs: delay.delayMs };
    return "deliver";
  }

  private partitioned(a: SiloAddress, b: SiloAddress): boolean {
    const on = (side: readonly SiloSelector[], silo: SiloAddress): boolean =>
      side.some((s) => matches(s, silo));
    return this.partitions.some(
      (p) => (on(p.sideA, a) && on(p.sideB, b)) || (on(p.sideB, a) && on(p.sideA, b)),
    );
  }

  private consume<R extends CountedRule>(
    rules: R[],
    from: SiloAddress,
    to: SiloAddress,
  ): R | undefined {
    const rule = rules.find((r) => onLink(r.link, from, to));
    if (rule === undefined) return undefined;
    if (rule.remaining !== undefined) {
      rule.remaining -= 1;
      if (rule.remaining <= 0) this.remove(rules, rule);
    }
    return rule;
  }

  private add<R>(list: R[], rule: R): FaultHandle {
    list.push(rule);
    return { heal: () => this.remove(list, rule) };
  }

  private remove<R>(list: R[], rule: R): void {
    const i = list.indexOf(rule);
    if (i >= 0) list.splice(i, 1);
  }
}

/**
 * A `Transport` decorator that injects the faults a shared {@link NetworkFaults} describes into
 * everything this silo sends: on the connections it dials and on the connections it accepts (the
 * duplex path a reply to a client takes). Connects it is told to fail reject with the same
 * `RejectionError` (`unknownTarget`) the transports raise for an unreachable peer. Wraps any
 * transport, so the same faults apply over the in-process network and over WebSocket.
 */
export class FaultInjectingTransport implements Transport {
  constructor(
    private readonly inner: Transport,
    private readonly faults: NetworkFaults,
  ) {}

  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    const accept: ConnectionAcceptHandler | undefined =
      onAccept === undefined
        ? undefined
        : (preamble, connection) =>
            onAccept(preamble, this.faulty(connection, address, preamble.siloAddress));
    return this.inner.listen(address, onMessage, accept);
  }

  async connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const from = preamble.siloAddress;
    if (this.faults.refusesConnect(from, to)) {
      throw new RejectionError(
        `connect from ${from.toString()} to ${to.toString()} refused (injected fault)`,
        "unknownTarget",
      );
    }
    return this.faulty(await this.inner.connect(to, preamble, onMessage), from, to);
  }

  private faulty(connection: Connection, from: SiloAddress, to: SiloAddress): Connection {
    const faults = this.faults;
    const wrapped: Connection = {
      send: (message: Message) => {
        const verdict = faults.verdictFor(from, to);
        if (verdict === "drop") return;
        if (verdict === "deliver") {
          connection.send(message);
          return;
        }
        faults.time.setTimer(() => {
          try {
            connection.send(message);
          } catch {
            // The peer went away while the message was in flight: it is lost, as on a network.
          }
        }, verdict.delayMs);
      },
      close: (reason?: string) => connection.close(reason),
    };
    if (connection.onClose !== undefined) {
      wrapped.onClose = (callback) => connection.onClose!(callback);
    }
    return wrapped;
  }
}
