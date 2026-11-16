/**
 * Run as its own process by `test-cluster.leaving.test.ts`: start a three-silo cluster on each
 * membership, call some grains, dispose it and return. The process must then exit on its own,
 * which it does only if nothing the clusters armed is left behind.
 */
import { defineGrain } from "@thresh/core/define-grain";
import { TestCluster } from "@thresh/testing/test-cluster";

const Counter = defineGrain("DisposeExitCounter", () => {
  let n = 0;
  return { inc: async (): Promise<number> => ++n };
});

for (const membership of ["static", "kubernetes"] as const) {
  const cluster = await TestCluster.start({
    initialSilos: 3,
    membership,
    grains: [Counter],
    reminders: false,
  });
  for (let i = 0; i < 12; i++) {
    await cluster.silos[i % 3]!.host.getGrain(Counter, `exit-${i}`).inc();
  }
  await cluster.dispose();
}
process.stdout.write("disposed static and kubernetes clusters\n");
