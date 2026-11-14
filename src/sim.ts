import { RaftNode } from "./raft.js";
import { DEFAULT_CONFIG, type KvOp, type Message, type NodeId, type ProposeResult, type RaftConfig, type Storage } from "./types.js";

/** Deterministic PRNG: same seed = bilkul same execution (bug dhora porle seed diye hubohu abar chalano jay) */
export function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

interface InFlight { at: number; id: number; from: NodeId; to: NodeId; msg: Message }
export interface NetworkOptions { minLatency: number; maxLatency: number; dropRate: number; dupRate: number }

/**
 * Simulated cluster: network (delay, drop, duplicate, reorder, partition), crash/restart, ar SAFETY INVARIANT checker.
 * Shomoy (virtual ms) nijer hate cholano hoy, tai hajar bochor er chaos kichu second e chalano jay.
 */
export class Cluster {
  now = 0;
  readonly ids: NodeId[];
  nodes = new Map<NodeId, RaftNode>();
  storages = new Map<NodeId, Storage>();
  down = new Set<NodeId>();
  net: NetworkOptions = { minLatency: 1, maxLatency: 10, dropRate: 0, dupRate: 0 };
  clients: Client[] = [];
  readonly rng: () => number;

  private queue: InFlight[] = [];
  private msgSeq = 0;
  private group = new Map<NodeId, number>();           // partition: alada group = joggajog nai
  private leaderOfTerm = new Map<number, NodeId>();
  private committed = new Map<number, string>();       // globally commit howa entry (index -> term+command)
  private checkedUpTo = new Map<NodeId, number>();
  violations: string[] = [];
  messagesSent = 0;

  constructor(size: number, seed: number, private cfg: RaftConfig = DEFAULT_CONFIG, initial: Record<NodeId, Storage> = {}) {
    this.rng = mulberry32(seed);
    this.ids = Array.from({ length: size }, (_, i) => i + 1);
    for (const id of this.ids) {
      this.storages.set(id, initial[id] ? structuredClone(initial[id]) : { term: 0, votedFor: null, log: [] });   // initial = pre-seeded disk (directed scenario test er jonno)
      this.boot(id);
    }
  }

  private boot(id: NodeId) {
    const node = new RaftNode(id, this.ids.filter((p) => p !== id), this.storages.get(id)!, {
      send: (to, msg) => this.send(id, to, msg),
      now: () => this.now,
      rng: this.rng,
      onLeader: (nid, term) => this.recordLeader(nid, term),
    }, this.cfg);
    this.nodes.set(id, node);
    this.checkedUpTo.set(id, 0);
  }

  // ---------- network ----------
  private partitioned(a: NodeId, b: NodeId) { return (this.group.get(a) ?? 0) !== (this.group.get(b) ?? 0); }

  private send(from: NodeId, to: NodeId, msg: Message) {
    if (this.down.has(from) || this.partitioned(from, to)) return;
    this.messagesSent++;
    if (this.rng() < this.net.dropRate) return;
    const lat = () => this.net.minLatency + Math.floor(this.rng() * (this.net.maxLatency - this.net.minLatency + 1));
    this.queue.push({ at: this.now + lat(), id: this.msgSeq++, from, to, msg });
    if (this.rng() < this.net.dupRate) this.queue.push({ at: this.now + lat(), id: this.msgSeq++, from, to, msg });
  }

  partition(...groups: NodeId[][]) {
    this.group.clear();
    groups.forEach((g, i) => g.forEach((id) => this.group.set(id, i + 1)));
  }
  heal() { this.group.clear(); }

  crash(id: NodeId) { this.down.add(id); this.nodes.delete(id); }          // volatile state ses, disk (storage) thake
  restart(id: NodeId) { if (this.down.delete(id)) this.boot(id); }

  // ---------- running ----------
  tick() {
    this.now++;
    const due = this.queue.filter((m) => m.at <= this.now).sort((a, b) => a.at - b.at || a.id - b.id);
    if (due.length) this.queue = this.queue.filter((m) => m.at > this.now);
    for (const m of due) {
      if (this.down.has(m.to) || this.partitioned(m.from, m.to)) continue;      // in-flight message-o partition e kete jay
      this.nodes.get(m.to)?.receive(m.from, m.msg);
    }
    for (const id of this.ids) this.nodes.get(id)?.tick();
    for (const c of this.clients) c.tick();
    this.checkCommitted();
  }

  run(ms: number) { for (let i = 0; i < ms; i++) this.tick(); }

  runUntil(pred: () => boolean, maxMs: number): boolean {
    for (let i = 0; i < maxMs; i++) { if (pred()) return true; this.tick(); }
    return pred();
  }

  // ---------- inspection ----------
  live(): RaftNode[] { return this.ids.filter((i) => !this.down.has(i)).map((i) => this.nodes.get(i)!); }
  leaders(): RaftNode[] { return this.live().filter((n) => n.role === "leader"); }
  /** Shobcheye boro term er leader (partition e purono stale leader-o thakte pare) */
  leader(): RaftNode | undefined { return this.leaders().sort((a, b) => b.term - a.term)[0]; }

  // ---------- safety invariants (protita tick e check) ----------
  private recordLeader(id: NodeId, term: number) {
    const prev = this.leaderOfTerm.get(term);
    if (prev !== undefined && prev !== id) this.violations.push(`ELECTION SAFETY: term ${term} e duijon leader (${prev} ar ${id})`);
    this.leaderOfTerm.set(term, id);
  }

  private checkCommitted() {
    for (const n of this.live()) {
      for (let i = (this.checkedUpTo.get(n.id) ?? 0) + 1; i <= n.commitIndex; i++) {
        const e = n.log[i - 1];
        if (!e) { this.violations.push(`node ${n.id}: commitIndex ${n.commitIndex} kintu log e entry ${i} nai`); break; }
        const key = JSON.stringify([e.term, e.command]);
        const prev = this.committed.get(i);
        if (prev === undefined) this.committed.set(i, key);
        else if (prev !== key) this.violations.push(`STATE MACHINE SAFETY: index ${i} e duita alada committed entry (node ${n.id})`);
      }
      this.checkedUpTo.set(n.id, Math.max(this.checkedUpTo.get(n.id) ?? 0, n.commitIndex));
    }
  }

  /** Log Matching Property: duita log e same index + same term thakle tar age er shob entry same */
  checkLogMatching() {
    const live = this.live();
    for (const a of live) for (const b of live) {
      if (a.id >= b.id) continue;
      for (let i = Math.min(a.log.length, b.log.length); i >= 1; i--) {
        if (a.log[i - 1]!.term === b.log[i - 1]!.term) {
          for (let j = 1; j <= i; j++) if (JSON.stringify(a.log[j - 1]) !== JSON.stringify(b.log[j - 1])) this.violations.push(`LOG MATCHING: node ${a.id},${b.id} index ${j}`);
          break;
        }
      }
    }
  }

  /** Shob heal kore shob node restart, network thik kore stable howa porjonto chalai. */
  settle(maxMs = 8000): boolean {
    this.heal();
    for (const id of [...this.down]) this.restart(id);
    this.net = { ...this.net, dropRate: 0, dupRate: 0 };
    const stable = () => {
      const l = this.leader();
      if (!l || this.leaders().length !== 1) return false;
      return this.live().every((n) => n.commitIndex === l.commitIndex && n.lastApplied === l.lastApplied) && l.commitIndex === l.log.length;
    };
    return this.runUntil(stable, maxMs);
  }

  committedLog() { return [...this.committed.entries()].sort((a, b) => a[0] - b[0]).map(([, v]) => JSON.parse(v) as [number, { type: string; clientId?: string; seq?: number }]); }
}

/** Retry kora client: timeout hole onno node e ek (clientId, seq) pathay. Server dedup kore, tai exactly-once. */
export class Client {
  seq = 0;
  acked: { seq: number; op: KvOp; result: ProposeResult }[] = [];
  pending: { seq: number; op: KvOp; sentAt: number; retry: boolean } | null = null;
  private hint: NodeId | null = null;
  private rotate = 0;
  attempts = 0;

  constructor(readonly id: string, private cluster: Cluster, private timeoutMs = 250) { cluster.clients.push(this); }

  get busy() { return this.pending !== null; }

  submit(op: KvOp) {
    if (this.pending) throw new Error("one outstanding request per client");
    this.pending = { seq: ++this.seq, op, sentAt: -1, retry: true };
  }

  tick() {
    const p = this.pending;
    if (!p) return;
    if (!p.retry && this.cluster.now - p.sentAt < this.timeoutMs) return;
    if (!p.retry) this.hint = null;          // timeout = ei node/leader ta dhora-chhoar baire (partition?), onno node try kori
    p.retry = false;
    p.sentAt = this.cluster.now;
    this.attempts++;
    const live = this.cluster.live();
    if (live.length === 0) return;
    const target = (this.hint !== null && live.find((n) => n.id === this.hint)) || live[this.rotate++ % live.length]!;
    const seq = p.seq;
    target.propose(this.id, seq, p.op, (r) => {
      if (!this.pending || this.pending.seq !== seq) return;            // already ack hoyeche (onno attempt)
      if ("error" in r) {
        if (r.error === "not_leader") this.hint = r.leaderHint;
        this.pending.retry = true;                                       // porer tick e abar
        return;
      }
      this.acked.push({ seq, op: p.op, result: r });
      this.pending = null;
      this.hint = target.id;
    });
  }

  /** Test helper: ekta op pathao, ack na asha porjonto cluster chalao */
  exec(op: KvOp, maxMs = 5000): ProposeResult | undefined {
    this.submit(op);
    this.cluster.runUntil(() => !this.busy, maxMs);
    return this.acked.at(-1)?.seq === this.seq ? this.acked.at(-1)!.result : undefined;
  }
}
