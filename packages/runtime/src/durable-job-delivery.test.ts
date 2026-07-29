import { describe, expect, it } from "vitest";
import { defineGrain, useDurableJobHandler } from "@thresh/core/define-grain";
import { completed, type JobRunContext } from "@thresh/core/durable-job";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

interface IWorker extends GrainWithStringKey {
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.jobdelivery");

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildNode(onRun: () => Promise<void>): ClusterNode {
  const network = new InProcessNetwork();
  const node = new ClusterNode({
    local,
    clusterId: "c1",
    membership: new StaticMembershipService(local, [local]),
    transport: new InProcessTransport(network, "c1"),
  });
  const WorkerGrain = defineGrain<IWorker>("Worker", () => {
    useDurableJobHandler(async () => {
      await onRun();
      return completed;
    });
    return { ping: async () => "pong" };
  });
  node.registerGrain(WorkerGrain.grain, { interfaces: [IWorker] });
  return node;
}

function jobContext(runId: string): JobRunContext {
  return {
    id: "job-1",
    name: "ok",
    dueTime: new Date(0),
    target: new GrainId("Worker", "w1"),
    metadata: {},
    dequeueCount: 1,
    runId,
  };
}

describe("ClusterNode.deliverDurableJob — in-flight RunId dedup", () => {
  it("coalesces concurrent deliveries of the same RunId into a single handler run", async () => {
    let runCount = 0;
    let releaseHandler: (() => void) | undefined;
    const handlerGate = new Promise<void>((resolve) => {
      releaseHandler = resolve;
    });
    const node = buildNode(async () => {
      runCount += 1;
      await handlerGate;
    });
    await node.start();
    try {
      const job = jobContext("run-1");
      const first = node.deliverDurableJob(job);
      const second = node.deliverDurableJob(job);
      // Let both calls reach the in-flight map before releasing the handler.
      await Promise.resolve();
      await Promise.resolve();
      releaseHandler?.();
      const [firstResult, secondResult] = await Promise.all([first, second]);
      expect(runCount).toBe(1);
      expect(firstResult).toEqual({ kind: "completed" });
      expect(secondResult).toEqual({ kind: "completed" });
    } finally {
      await node.stop();
    }
  });

  it("runs a fresh delivery once the prior RunId's dispatch has settled", async () => {
    let runCount = 0;
    const node = buildNode(async () => {
      runCount += 1;
    });
    await node.start();
    try {
      await node.deliverDurableJob(jobContext("run-1"));
      await node.deliverDurableJob(jobContext("run-1"));
      expect(runCount).toBe(2);
    } finally {
      await node.stop();
    }
  });
});
