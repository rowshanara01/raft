import { Client, Cluster, mulberry32 } from "./sim.js";
import { DEFAULT_CONFIG, type RaftConfig } from "./types.js";

export interface ChaosOptions { seed: number; size?: number; ms?: number; clients?: number; faults?: boolean; config?: Partial<RaftConfig>; safetyOnly?: boolean }

/**
 * Chaos test: random node crash/restart, network partition, message drop/duplicate/reorder, ar tar moddhe client der continuous kaj.
 * Prottek client nijer key e sequentially append kore ar majhe majhe GET kore.
 * Prove kore: (1) safety invariant kokhono bhange na, (2) ack paoa write hariye jay na, (3) retry hole-o ekbar-i apply hoy (exactly-once),
 * (4) get always ager shob acked write dekhe (linearizable read).
 */
export function runChaos(o: ChaosOptions) {
  const size = o.size ?? 5;
  const ms = o.ms ?? 10_000;
  const cluster = new Cluster(size, o.seed, { ...DEFAULT_CONFIG, ...o.config });
  const clients = Array.from({ length: o.clients ?? 3 }, (_, i) => new Client(`c${i}`, cluster));
  const chaos = mulberry32(o.seed * 7919 + 13);          // chaos er alada random stream
  const readViolations: string[] = [];
  const stats = { crashes: 0, restarts: 0, partitions: 0, heals: 0 };
  let issuing = true;

  const expected = (i: number) => clients[i]!.acked.flatMap((a) => (a.op.kind === "append" ? [a.op.suffix] : [])).join("");

  const feed = () => {
    clients.forEach((c, i) => {
      if (!issuing || c.busy) return;
      const key = `k${i}`;
      if (c.seq % 3 === 2) c.submit({ kind: "get", key });
      else c.submit({ kind: "append", key, suffix: `${c.seq + 1},` });
    });
  };
  const verifyReads = () => {
    clients.forEach((c, i) => {
      const last = c.acked.at(-1);
      if (last && last.op.kind === "get" && !("error" in last.result) && !(last as { checked?: boolean }).checked) {
        (last as { checked?: boolean }).checked = true;
        const want = c.acked.slice(0, -1).flatMap((a) => (a.op.kind === "append" ? [a.op.suffix] : [])).join("");
        if ((last.result.value ?? "") !== want) readViolations.push(`client c${i}: GET returned "${last.result.value}" kintu ager acked write: "${want}"`);
      }
    });
  };

  for (let t = 0; t < ms; t++) {
    cluster.tick();
    feed();
    verifyReads();
    if (o.faults !== false && t % 50 === 0 && chaos() < 0.5) {
      const r = chaos();
      const live = cluster.live();
      if (r < 0.2 && live.length > 0) { cluster.crash(live[Math.floor(chaos() * live.length)]!.id); stats.crashes++; }
      else if (r < 0.5 && cluster.down.size > 0) { cluster.restart([...cluster.down][Math.floor(chaos() * cluster.down.size)]!); stats.restarts++; }
      else if (r < 0.65) {
        const ids = [...cluster.ids].sort(() => chaos() - 0.5);
        const cut = 1 + Math.floor(chaos() * (size - 1));
        cluster.partition(ids.slice(0, cut), ids.slice(cut));
        stats.partitions++;
      } else if (r < 0.8) { cluster.heal(); stats.heals++; }
      else cluster.net = { minLatency: 1, maxLatency: 5 + Math.floor(chaos() * 40), dropRate: chaos() * 0.15, dupRate: chaos() * 0.08 };
    }
  }

  if (o.safetyOnly) return { cluster, clients, stats, settled: false, kvConsistent: false, exactlyOnce: false, readViolations, acked: clients.reduce((n, c) => n + c.acked.length, 0) };

  // ---- Chaos shesh: sob thik kore, client der drain kori, cluster stable howa porjonto chalai ----
  issuing = false;
  const settled = cluster.settle(15_000);
  const drained = cluster.runUntil(() => clients.every((c) => !c.busy), 15_000);
  verifyReads();
  const settledAfter = cluster.settle(10_000);
  cluster.checkLogMatching();

  const leader = cluster.leader();
  const kvStates = cluster.live().map((n) => JSON.stringify([...n.kv.entries()].sort()));
  const kvConsistent = kvStates.every((s) => s === kvStates[0]);
  const exactlyOnce = clients.every((c, i) => (leader?.kv.get(`k${i}`) ?? "") === expected(i));   // na hariyeche, na duibar apply
  const acked = clients.reduce((n, c) => n + c.acked.length, 0);
  return { cluster, clients, stats, settled: settled && drained && settledAfter, kvConsistent, exactlyOnce, readViolations, acked };
}
