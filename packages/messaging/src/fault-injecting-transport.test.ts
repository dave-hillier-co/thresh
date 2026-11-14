import { describe, expect, it } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import {
  FaultInjectingTransport,
  NetworkFaults,
} from "@thresh/messaging/fault-injecting-transport";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import type { Message } from "@thresh/messaging/message";
import { PROTOCOL_VERSION, type Connection } from "@thresh/messaging/transport";

const CLUSTER = "c1";
const A = new SiloAddress("silo-a", "ua", "a:1");
const B = new SiloAddress("silo-b", "ub", "b:1");
const C = new SiloAddress("silo-c", "uc", "c:1");

const message = (method: string, from: SiloAddress): Message => ({
  correlationId: 1n,
  direction: "oneWay",
  targetGrain: new GrainId("Probe", "x"),
  sendingSilo: from,
  interfaceId: 0,
  method,
  body: new Uint8Array(),
});

/** Three silos on one in-process network, each behind its own fault-injecting transport. */
async function fabric(faults: NetworkFaults) {
  const net = new InProcessNetwork();
  const received = new Map<string, string[]>();
  const duplexReceived: string[] = [];
  const accepted = new Map<string, Connection>();
  const transports = new Map<string, FaultInjectingTransport>();
  for (const silo of [A, B, C]) {
    const transport = new FaultInjectingTransport(new InProcessTransport(net, CLUSTER), faults);
    transports.set(silo.podName, transport);
    received.set(silo.podName, []);
    await transport.listen(
      silo,
      (msg) => {
        received.get(silo.podName)!.push(msg.method);
      },
      (preamble, connection) =>
        accepted.set(`${silo.podName}<-${preamble.siloAddress.podName}`, connection),
    );
  }
  const dial = (from: SiloAddress, to: SiloAddress): Promise<Connection> =>
    transports
      .get(from.podName)!
      .connect(
        to,
        { protocolVersion: PROTOCOL_VERSION, siloAddress: from, clusterId: CLUSTER, epoch: 1 },
        (msg) => {
          duplexReceived.push(msg.method);
        },
      );
  return { received, duplexReceived, accepted, dial };
}

const flush = async (): Promise<void> => {
  for (let i = 0; i < 3; i++) await Promise.resolve();
};

describe("FaultInjectingTransport", () => {
  it("delivers everything while no fault is set", async () => {
    const f = await fabric(new NetworkFaults());
    (await f.dial(A, B)).send(message("hello", A));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["hello"]);
  });

  it("drops messages on a link and leaves every other link untouched", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    const ac = await f.dial(A, C);
    const ba = await f.dial(B, A);
    faults.drop({ from: "silo-a", to: "silo-b" });

    ab.send(message("lost", A));
    ac.send(message("kept", A));
    ba.send(message("reverse", B));
    await flush();

    expect(f.received.get("silo-b")).toEqual([]);
    expect(f.received.get("silo-c")).toEqual(["kept"]);
    expect(f.received.get("silo-a")).toEqual(["reverse"]);
    expect(faults.dropped).toBe(1);
  });

  it("drops only as many messages as it was told to", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    faults.drop({ from: A, to: B }, { count: 2 });
    for (const m of ["1", "2", "3"]) ab.send(message(m, A));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["3"]);
  });

  it("delays messages on the clock it was given, in order", async () => {
    const time = new FakeTimeProvider();
    const faults = new NetworkFaults(time);
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    faults.delay({ from: "silo-a", to: "silo-b" }, 100);
    ab.send(message("first", A));
    ab.send(message("second", A));
    await flush();
    expect(f.received.get("silo-b")).toEqual([]);
    time.advance(99);
    await flush();
    expect(f.received.get("silo-b")).toEqual([]);
    time.advance(1);
    await flush();
    expect(f.received.get("silo-b")).toEqual(["first", "second"]);
  });

  it("fails a connect with the transport's own rejection", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    faults.failConnects({ from: "silo-a", to: "silo-b" });
    const error = await f.dial(A, B).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RejectionError);
    expect((error as RejectionError).kind).toBe("unknownTarget");
    await expect(f.dial(A, C)).resolves.toBeDefined();
    await expect(f.dial(B, A)).resolves.toBeDefined();
  });

  it("partitions two sides in both directions, replies included, and heals", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    const bc = await f.dial(B, C);
    const partition = faults.partition(["silo-a"], ["silo-b"]);

    ab.send(message("across", A));
    f.accepted.get("silo-b<-silo-a")!.send(message("reply-across", B));
    bc.send(message("unaffected", B));
    await flush();
    expect(f.received.get("silo-b")).toEqual([]);
    expect(f.duplexReceived).toEqual([]);
    expect(f.received.get("silo-c")).toEqual(["unaffected"]);
    await expect(f.dial(B, A)).rejects.toBeInstanceOf(RejectionError);

    partition.heal();
    ab.send(message("healed", A));
    f.accepted.get("silo-b<-silo-a")!.send(message("reply-healed", B));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["healed"]);
    expect(f.duplexReceived).toEqual(["reply-healed"]);
    await expect(f.dial(B, A)).resolves.toBeDefined();
  });

  it("follows an explicit per-message schedule, then falls back to the standing rules", async () => {
    const time = new FakeTimeProvider();
    const faults = new NetworkFaults(time);
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    faults.schedule({ from: "silo-a", to: "silo-b" }, ["deliver", "drop", { delayMs: 10 }]);
    for (const m of ["1", "2", "3", "4"]) ab.send(message(m, A));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["1", "4"]);
    time.advance(10);
    await flush();
    expect(f.received.get("silo-b")).toEqual(["1", "4", "3"]);
  });

  it("severs the established connections on a link at the accepting end, and only those", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    const ac = await f.dial(A, C);
    const lost: string[] = [];
    ab.onClose?.(() => lost.push("a->b"));
    ac.onClose?.(() => lost.push("a->c"));

    expect(await faults.sever({ from: "silo-a", to: "silo-b" })).toBe(1);
    await flush();

    expect(lost).toEqual(["a->b"]);
    expect(() => ab.send(message("after", A))).toThrow();
    ac.send(message("still up", A));
    await flush();
    expect(f.received.get("silo-c")).toEqual(["still up"]);
    // A new connection on the severed link is unaffected.
    const again = await f.dial(A, B);
    again.send(message("redialled", A));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["redialled"]);
  });

  it("heals every fault at once", async () => {
    const faults = new NetworkFaults();
    const f = await fabric(faults);
    const ab = await f.dial(A, B);
    faults.drop({ to: "silo-b" });
    faults.partition(["silo-a"], ["silo-c"]);
    faults.healAll();
    ab.send(message("through", A));
    await flush();
    expect(f.received.get("silo-b")).toEqual(["through"]);
    await expect(f.dial(A, C)).resolves.toBeDefined();
  });
});
