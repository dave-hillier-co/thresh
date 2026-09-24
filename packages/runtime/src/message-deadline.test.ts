import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainCallTimeoutError, isCancellationError } from "@thresh/core/errors";
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
import { invocationContext } from "@thresh/runtime/invocation-context";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ICounter extends GrainKey<string> {
  increment(): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.messageDeadline");

let count = 0;

@grain()
class CounterGrain extends Grain implements ICounter {
  async increment(): Promise<number> {
    count += 1;
    return count;
  }
}

interface IGated extends GrainKey<string> {
  run(): Promise<number>;
}
const IGated = defineGrainInterface<IGated>("IGated.messageDeadline");

let gatedRuns = 0;
let gatedStarted!: Promise<void>;
let notifyGatedStarted!: () => void;
let releaseGated!: () => void;
let gatedGate!: Promise<void>;

@grain()
class GatedGrain extends Grain implements IGated {
  async run(): Promise<number> {
    gatedRuns += 1;
    notifyGatedStarted();
    await gatedGate;
    return gatedRuns;
  }
}

/** Counts request messages as they arrive at the wrapped listener. */
class ArrivalCountingTransport implements Transport {
  arrivals = 0;
  private waiters: { count: number; resolve: () => void }[] = [];
  constructor(private readonly inner: Transport) {}
  listen(
    address: SiloAddress,
    onMessage: MessageHandler,
    onAccept?: ConnectionAcceptHandler,
  ): Promise<Listener> {
    return this.inner.listen(
      address,
      (message, from) => {
        const result = onMessage(message, from);
        if (message.direction === "request" && message.method === "run") {
          this.arrivals += 1;
          this.waiters = this.waiters.filter((w) => {
            if (this.arrivals < w.count) return true;
            w.resolve();
            return false;
          });
        }
        return result;
      },
      onAccept,
    );
  }
  connect(
    to: SiloAddress,
    preamble: ConnectionPreamble,
    onMessage?: MessageHandler,
  ): Promise<Connection> {
    return this.inner.connect(to, preamble, onMessage);
  }
  arrived(count: number): Promise<void> {
    if (this.arrivals >= count) return Promise.resolve();
    return new Promise((resolve) => this.waiters.push({ count, resolve }));
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

// Issue #90: `InvocationRequest.deadline` never rode `ClusterNode.sendRemote`'s
// wire `Message` -- it stopped at the `RemoteInvoker` boundary -- so a remote
// turn had no deadline signal at all, and a call the caller had already given
// up on still ran to completion on the callee. Orleans carries this as
// `Message.TimeToLive` and drops an expired request before it runs
// (`ActivationData.ReceiveMessage`).
describe("call deadlines cross a silo forward (issue #90)", () => {
  it("drops an already-expired call on the remote silo instead of running it", async () => {
    count = 0;
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const makeNode = (local: SiloAddress) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        random: () => 0.99, // picks silo-1 (the remote candidate) for a fresh placement
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const node0 = makeNode(silo(0));
    const node1 = makeNode(silo(1));
    await node0.start();
    await node1.start();

    try {
      const grainRef = node0.getGrain(ICounter, "g1");
      const err = await invocationContext
        .run(
          {
            senderId: undefined,
            ownerId: undefined,
            reentrancyId: "r1",
            deadline: Date.now() - 1_000, // already expired before it even leaves silo-0
          },
          () => grainRef.increment(),
        )
        .catch((e: unknown) => e);

      expect(isCancellationError(err)).toBe(true);
      expect(count).toBe(0); // the remote turn on silo-1 never ran
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});

// Orleans serializes `Message.TimeToLive` as the REMAINING milliseconds
// (`MessageSerializer` writes `GetTimeToLiveMilliseconds()`, the receiver
// re-arms a local stopwatch from it), so expiry never compares two silos'
// wall clocks. An absolute epoch deadline on the wire would.
describe("the wire deadline is relative, not an absolute timestamp (issue #90)", () => {
  it("runs a call whose deadline is still in the future even when the callee's clock is far ahead", async () => {
    count = 0;
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const callerTime = new FakeTimeProvider();
    const calleeTime = new FakeTimeProvider();
    calleeTime.advance(1_000_000); // skewed well past the caller's deadline
    const makeNode = (local: SiloAddress, time: FakeTimeProvider) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport: new InProcessTransport(network, CLUSTER),
        random: () => 0.99,
        time,
      });
      node.registerGrain(CounterGrain, { interfaces: [ICounter] });
      return node;
    };
    const node0 = makeNode(silo(0), callerTime);
    const node1 = makeNode(silo(1), calleeTime);
    await node0.start();
    await node1.start();

    try {
      const grainRef = node0.getGrain(ICounter, "skewed");
      const result = await invocationContext.run(
        {
          senderId: undefined,
          ownerId: undefined,
          reentrancyId: "r-skew",
          deadline: callerTime.now() + 10_000, // 10s left, on the CALLER's clock
        },
        () => grainRef.increment(),
      );

      expect(result).toBe(1);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});

// Orleans stamps every expirable request with a `TimeToLive` of its response
// timeout (`InsideRuntimeClient.SendRequest`) and drops it at invoke time once
// that has passed (`InsideRuntimeClient.Invoke`), so a request left queued
// behind a long turn does not run after its caller has already given up --
// whether or not the caller set an explicit deadline.
describe("a request carries its caller's call timeout as a time-to-live (issue #90)", () => {
  it("does not run a request still queued when its caller's call timeout has passed", async () => {
    gatedRuns = 0;
    gatedStarted = new Promise((resolve) => (notifyGatedStarted = resolve));
    gatedGate = new Promise((resolve) => (releaseGated = resolve));
    const network = new InProcessNetwork();
    const membership = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const time = new FakeTimeProvider();
    const calleeTransport = new ArrivalCountingTransport(new InProcessTransport(network, CLUSTER));
    const makeNode = (local: SiloAddress, transport: Transport) => {
      const node = new ClusterNode({
        local,
        clusterId: CLUSTER,
        membership: new MembershipView(membership, local),
        transport,
        random: () => 0.99,
        time,
        callTimeoutMs: 5_000,
      });
      node.registerGrain(GatedGrain, { interfaces: [IGated] });
      return node;
    };
    const node0 = makeNode(silo(0), new InProcessTransport(network, CLUSTER));
    const node1 = makeNode(silo(1), calleeTransport);
    await node0.start();
    await node1.start();

    try {
      const grainRef = node0.getGrain(IGated, "queued");
      const first = grainRef.run();
      await gatedStarted;
      const second = grainRef.run().catch((e: unknown) => e);
      await calleeTransport.arrived(2);
      await new Promise((resolve) => setTimeout(resolve, 0));

      time.advance(6_000); // past the caller's 5s call timeout, on the callee's clock
      releaseGated();

      expect(await first).toBe(1);
      expect(await second).toBeInstanceOf(GrainCallTimeoutError);
      expect(gatedRuns).toBe(1); // the queued request never ran
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
