import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { RejectionError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { MembershipService } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type {
  Connection,
  ConnectionAcceptHandler,
  ConnectionPreamble,
  Listener,
  MessageHandler,
  Transport,
} from "@thresh/messaging/transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface IPausable extends GrainKey<string> {
  run(): Promise<number>;
}
const IPausable = defineGrainInterface<IPausable>("IPausable.connectionLoss");

// Module-level so the grain instance (activated deep inside `ClusterNode`,
// with no handle this test otherwise gets) can signal the test and be held
// mid-turn by it. Reset in `beforeEach`.
let runCount = 0;
let notifyStarted!: () => void;
let startedPromise!: Promise<void>;
let releaseGate!: () => void;
let gatePromise!: Promise<void>;

function resetGrainState(): void {
  runCount = 0;
  startedPromise = new Promise((resolve) => (notifyStarted = resolve));
  gatePromise = new Promise((resolve) => (releaseGate = resolve));
}

@grain()
class PausableGrain extends Grain implements IPausable {
  async run(): Promise<number> {
    runCount += 1;
    notifyStarted();
    await gatePromise;
    return runCount;
  }
}

const CLUSTER = "c1";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}
  current() {
    return this.shared.current();
  }
  updates() {
    return this.shared.updates();
  }
  localSilo() {
    return this.local;
  }
}

/**
 * Wraps `InProcessTransport` so the dial to `dropTarget` hands back a
 * `Connection` whose `onClose` this test can fire on demand -- simulating the
 * transport reporting a connection lost out from under a pending call
 * (`ConnectionManager.get` wires exactly this callback). Plain
 * `InProcessTransport` never loses a connection on its own, so there is
 * nothing to trigger without this wrapper.
 */
class DroppableTransport implements Transport {
  private closeCallback: ((err?: unknown) => void) | undefined;
  constructor(
    private readonly inner: Transport,
    private readonly dropTarget: SiloAddress,
  ) {}
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    return this.inner.listen(address, onMessage, onAccept);
  }
  async connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    const conn = await this.inner.connect(to, preamble, onMessage);
    if (!to.equals(this.dropTarget)) return conn;
    return {
      send: (m) => conn.send(m),
      close: (r) => conn.close(r),
      onClose: (cb) => {
        this.closeCallback = cb;
      },
    };
  }
  /** Simulate the transport reporting the pooled connection to `dropTarget` lost. */
  drop(err?: unknown): void {
    this.closeCallback?.(err);
  }
}

// Issue #88: a pooled connection dying mid-call used to fail the pending call
// with the SAME "unknownTarget" kind a stale cache/directory entry produces,
// which `DistributedDispatcher.deliver` treats as safe to invalidate and
// resend -- so a call already executing on the callee got re-sent and ran
// twice. Orleans never resends here: a dead target fails the caller with
// `SiloUnavailableException` and nothing more (`CallbackData.OnTargetSiloFail`).
describe("connection loss gives a non-retriable rejection, not a resend (issue #88)", () => {
  beforeEach(() => resetGrainState());

  it("fails the caller with siloUnavailable and never re-sends an already-executing call", async () => {
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const droppable = new DroppableTransport(new InProcessTransport(network, CLUSTER), silo(1));

    const node0 = new ClusterNode({
      local: silo(0),
      clusterId: CLUSTER,
      membership: new MembershipView(membership, silo(0)),
      transport: droppable,
      random: () => 0.99, // picks silo-1 (the remote candidate) for a fresh placement
    });
    node0.registerGrain(PausableGrain, { interfaces: [IPausable] });

    const node1 = new ClusterNode({
      local: silo(1),
      clusterId: CLUSTER,
      membership: new MembershipView(membership, silo(1)),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0.99,
    });
    node1.registerGrain(PausableGrain, { interfaces: [IPausable] });

    await node0.start();
    await node1.start();

    try {
      const call = node0.getGrain(IPausable, "g1").run();

      // Wait until silo-1's turn has actually STARTED before dropping the
      // connection: the bug this guards against is a call already executing
      // getting resent, not merely one that was dispatched.
      await startedPromise;
      expect(runCount).toBe(1);

      droppable.drop(new Error("simulated socket reset"));

      const err = await call.catch((e: unknown) => e);
      expect(err).toBeInstanceOf(RejectionError);
      expect((err as RejectionError).kind).toBe("siloUnavailable");

      // Let silo-1's original (still in-flight) turn finish, then confirm it
      // ran exactly once: nothing on silo-0 resent the call underneath it.
      releaseGate();
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(runCount).toBe(1);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
