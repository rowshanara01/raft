import { describe, expect, it } from "vitest";
import { runChaos } from "../src/chaos.js";

function assertHealthy(r: ReturnType<typeof runChaos>, label: string) {
  expect(r.cluster.violations, `${label} safety violations`).toEqual([]);
  expect(r.readViolations, `${label} read violations`).toEqual([]);
  expect(r.settled, `${label} chaos er por cluster stable hoy ni`).toBe(true);
  expect(r.kvConsistent, `${label} node der state alada`).toBe(true);
  expect(r.exactlyOnce, `${label} write hariyeche ba duibar apply`).toBe(true);
  expect(r.acked, `${label} kono write ack hoyni`).toBeGreaterThan(0);
}

describe("chaos: crash + partition + drop/dup/reorder + concurrent clients", () => {
  it("5-node cluster, 30ta alada seed", () => {
    let crashes = 0, partitions = 0, acked = 0;
    for (let seed = 1; seed <= 30; seed++) {
      const r = runChaos({ seed, size: 5, ms: 10_000 });
      assertHealthy(r, `seed ${seed}`);
      crashes += r.stats.crashes; partitions += r.stats.partitions; acked += r.acked;
    }
    expect(crashes).toBeGreaterThan(50);       // chaos sotti-i hoyechhe
    expect(partitions).toBeGreaterThan(20);
    expect(acked).toBeGreaterThan(300);
  });

  it("3-node ar 7-node cluster", () => {
    for (const size of [3, 7]) for (let seed = 100; seed < 112; seed++) assertHealthy(runChaos({ seed, size, ms: 8000 }), `size ${size} seed ${seed}`);
  });

  it("Jor shonkhok node (2, 4, 6): majority = floor(n/2)+1, 'adha' noy", () => {
    for (const size of [2, 4, 6]) for (let seed = 200; seed < 210; seed++) assertHealthy(runChaos({ seed, size, ms: 6000 }), `size ${size} seed ${seed}`);
  });

  it("chaos chhara (control): onek beshi throughput", () => {
    const calm = runChaos({ seed: 5, ms: 5000, faults: false });
    const wild = runChaos({ seed: 5, ms: 5000 });
    assertHealthy(calm, "calm");
    expect(calm.acked).toBeGreaterThan(wild.acked);
  });

  it("DETERMINISM: same seed = hubohu same execution (bug hole seed diye replay kora jay)", () => {
    const a = runChaos({ seed: 42, ms: 6000 });
    const b = runChaos({ seed: 42, ms: 6000 });
    const c = runChaos({ seed: 43, ms: 6000 });
    const sig = (r: ReturnType<typeof runChaos>) => JSON.stringify([r.cluster.committedLog(), r.cluster.messagesSent, r.stats]);
    expect(sig(a)).toBe(sig(b));
    expect(sig(a)).not.toBe(sig(c));
  });
});
