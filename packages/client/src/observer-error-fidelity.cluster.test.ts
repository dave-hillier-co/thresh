import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { UnavailableExceptionFallbackException } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { registerSurrogate } from "@thresh/core/value-codec";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createClient } from "@thresh/client/client-node";

/**
 * The client -> silo half of the error path. A silo answering a grain call sends the error VALUE
 * alongside its message; a CLIENT answering a call into an object it hosts (an observer) used to
 * send only the message text, so a callback's domain error reached the calling grain with no type
 * at all — not even with a surrogate registered.
 */
class ObserverRefusedError extends Error {
  constructor(
    message: string,
    readonly reason: string,
  ) {
    super(message);
    this.name = "ObserverRefusedError";
  }
}

class RegisteredObserverError extends Error {
  constructor(
    message: string,
    readonly code: number,
  ) {
    super(message);
    this.name = "RegisteredObserverError";
  }
}

registerSurrogate<RegisteredObserverError>({
  tag: "test.registeredObserverError",
  test: (value) => value instanceof RegisteredObserverError,
  encode: (error) => ({ message: error.message, code: error.code }),
  decode: (fields) => new RegisteredObserverError(fields.message as string, fields.code as number),
});

interface IFailingObserver extends GrainWithStringKey {
  onEvent(text: string): Promise<string>;
}
const IFailingObserver = defineGrainInterface<IFailingObserver>("test.IFailingObserver");

/** What the grain observed when the observer threw, flattened so it can cross back to the test. */
interface ObservedFailure {
  readonly name: string;
  readonly message: string;
  readonly isFallback: boolean;
  readonly isRegistered: boolean;
  readonly carried: unknown;
}

interface IPokerGrain extends GrainWithStringKey {
  subscribe(observer: IFailingObserver): Promise<void>;
  pokeAndReport(text: string): Promise<ObservedFailure>;
}
const IPokerGrain = defineGrainInterface<IPokerGrain>("test.IPokerGrain");

@grain()
class PokerGrain extends Grain implements IPokerGrain {
  private observer: IFailingObserver | undefined;

  async subscribe(observer: IFailingObserver): Promise<void> {
    this.observer = observer;
  }

  async pokeAndReport(text: string): Promise<ObservedFailure> {
    if (this.observer === undefined) throw new Error("no observer subscribed");
    try {
      await this.observer.onEvent(text);
      throw new Error("the observer was expected to throw");
    } catch (error) {
      const err = error as Error & { reason?: unknown; code?: unknown };
      return {
        name: err.name,
        message: err.message,
        isFallback: error instanceof UnavailableExceptionFallbackException,
        isRegistered: error instanceof RegisteredObserverError,
        carried: err.reason ?? err.code,
      };
    }
  }
}

const CLUSTER = "observer-error-fidelity";

async function withClientAndSilo(
  run: (silo: ClusterNode, client: ReturnType<typeof createClient>) => Promise<void>,
): Promise<void> {
  const network = new InProcessNetwork();
  const siloAddr = new SiloAddress("silo-1", "uid-1", "silo-1:11111");
  const silo = new ClusterNode({
    local: siloAddr,
    clusterId: CLUSTER,
    membership: new StaticMembershipService(siloAddr, [siloAddr]),
    transport: new InProcessTransport(network, CLUSTER),
    random: () => 0,
  });
  silo.registerGrain(PokerGrain, { interfaces: [IPokerGrain] });
  await silo.start();
  const client = createClient({
    clusterId: CLUSTER,
    transport: new InProcessTransport(network, CLUSTER),
    gateway: siloAddr,
  }).registerGrain(PokerGrain, { interfaces: [IPokerGrain] });
  await client.connect();
  try {
    await run(silo, client);
  } finally {
    await client.close();
    await silo.stop();
  }
}

describe("client-hosted observer error fidelity", () => {
  it("carries an unregistered observer error's name and carried state back to the grain", async () => {
    await withClientAndSilo(async (_silo, client) => {
      const ref = client.createObjectReference(IFailingObserver, {
        onEvent: async () => {
          throw new ObserverRefusedError("the observer refused", "unsubscribed");
        },
      });
      const grainRef = client.getGrain(IPokerGrain, "p1");
      await grainRef.subscribe(ref);
      const observed = await grainRef.pokeAndReport("hello");
      expect(observed.name).toBe("ObserverRefusedError");
      expect(observed.message).toBe("the observer refused");
      expect(observed.isFallback).toBe(true);
      expect(observed.carried).toBe("unsubscribed");
    });
  });

  it("rebuilds a surrogate-registered observer error as its own class", async () => {
    await withClientAndSilo(async (_silo, client) => {
      const ref = client.createObjectReference(IFailingObserver, {
        onEvent: async () => {
          throw new RegisteredObserverError("registered refusal", 7);
        },
      });
      const grainRef = client.getGrain(IPokerGrain, "p2");
      await grainRef.subscribe(ref);
      const observed = await grainRef.pokeAndReport("hello");
      expect(observed.isRegistered).toBe(true);
      expect(observed.name).toBe("RegisteredObserverError");
      expect(observed.carried).toBe(7);
    });
  });
});
