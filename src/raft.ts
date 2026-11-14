import type {
  AppendEntries, AppendEntriesReply, Command, KvOp, KvResult, LogEntry, Message, NodeEnv, NodeId,
  ProposeResult, RaftConfig, RequestVote, RequestVoteReply, Storage,
} from "./types.js";
import { DEFAULT_CONFIG } from "./types.js";

type Role = "follower" | "candidate" | "leader";

/**
 * Raft consensus node (Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm").
 * Implement kora: leader election, log replication, safety (election restriction + current-term commit rule),
 * persistence, client session dedup (exactly-once). Snapshot ar membership change nai (README dekho).
 */
export class RaftNode {
  // ---- Persistent state (crash er por-o thake) ----
  term: number;
  votedFor: NodeId | null;
  log: LogEntry[];
  // ---- Volatile state ----
  role: Role = "follower";
  leaderId: NodeId | null = null;
  commitIndex = 0;
  lastApplied = 0;
  private votes = new Set<NodeId>();
  private nextIndex = new Map<NodeId, number>();
  private matchIndex = new Map<NodeId, number>();
  private electionDeadline = 0;
  private nextHeartbeat = 0;
  // ---- State machine (log replay kore banano hoy) ----
  kv = new Map<string, string>();
  private sessions = new Map<string, { seq: number; result: KvResult }>();   // client dedup: kon client er kon seq age apply hoyechhe
  private pending = new Map<number, { term: number; cb: (r: ProposeResult) => void }>();

  constructor(
    readonly id: NodeId,
    private peers: NodeId[],
    private storage: Storage,
    private env: NodeEnv,
    private cfg: RaftConfig = DEFAULT_CONFIG,
  ) {
    this.term = storage.term;
    this.votedFor = storage.votedFor;
    this.log = storage.log.slice();
    this.resetElectionTimer();
  }

  // ---------- helpers ----------
  private get size() { return this.peers.length + 1; }
  private lastIndex() { return this.log.length; }
  private termAt(i: number) { return i === 0 ? 0 : (this.log[i - 1]?.term ?? 0); }
  private persist() { this.storage.term = this.term; this.storage.votedFor = this.votedFor; this.storage.log = this.log.slice(); }
  private resetElectionTimer() {
    this.electionDeadline = this.env.now() + this.cfg.electionMin + this.env.rng() * (this.cfg.electionMax - this.cfg.electionMin);
  }
  private send(to: NodeId, msg: Message) { this.env.send(to, msg); }

  // ---------- time ----------
  tick() {
    const now = this.env.now();
    if (this.role === "leader") {
      if (now >= this.nextHeartbeat) { this.broadcastAppend(); this.nextHeartbeat = now + this.cfg.heartbeat; }
    } else if (now >= this.electionDeadline) {
      this.startElection();
    }
  }

  // ---------- term / role transitions ----------
  private becomeFollower(term: number) {
    if (term > this.term) { this.term = term; this.votedFor = null; this.persist(); }
    if (this.role === "leader") this.failPending({ error: "leadership_lost" });
    this.role = "follower";
    this.resetElectionTimer();
  }

  private startElection() {
    this.term++;
    this.votedFor = this.id;
    this.persist();                       // vote disk e, tarpor-i message
    this.role = "candidate";
    this.leaderId = null;
    this.votes = new Set([this.id]);
    this.resetElectionTimer();
    if (this.votes.size * 2 > this.size) return this.becomeLeader();
    const m: RequestVote = { type: "RequestVote", term: this.term, candidateId: this.id, lastLogIndex: this.lastIndex(), lastLogTerm: this.termAt(this.lastIndex()) };
    for (const p of this.peers) this.send(p, m);
  }

  private becomeLeader() {
    this.role = "leader";
    this.leaderId = this.id;
    for (const p of this.peers) { this.nextIndex.set(p, this.lastIndex() + 1); this.matchIndex.set(p, 0); }
    // No-op: nijer term er ekta entry commit na hole purono term er entry commit kora jay na (Raft paper §5.4.2, §8)
    if (this.cfg.leaderNoop) this.log.push({ term: this.term, command: { type: "noop" } });
    this.persist();
    this.env.onLeader?.(this.id, this.term);
    this.broadcastAppend();
    this.nextHeartbeat = this.env.now() + this.cfg.heartbeat;
    this.advanceCommit();
  }

  // ---------- messages ----------
  receive(from: NodeId, msg: Message) {
    if (msg.term > this.term) this.becomeFollower(msg.term);   // jekono message e boro term dekhle ami follower
    switch (msg.type) {
      case "RequestVote": return this.onRequestVote(from, msg);
      case "RequestVoteReply": return this.onVoteReply(from, msg);
      case "AppendEntries": return this.onAppendEntries(from, msg);
      case "AppendEntriesReply": return this.onAppendReply(from, msg);
    }
  }

  private onRequestVote(from: NodeId, m: RequestVote) {
    let grant = false;
    if (m.term === this.term && (this.votedFor === null || this.votedFor === m.candidateId)) {
      // Election restriction: candidate er log amar cheye kom up-to-date hole vote dibo na (committed data hariye jabe)
      const myLastTerm = this.termAt(this.lastIndex());
      const upToDate = m.lastLogTerm > myLastTerm || (m.lastLogTerm === myLastTerm && m.lastLogIndex >= this.lastIndex());
      if (upToDate) {
        grant = true;
        this.votedFor = m.candidateId;
        this.persist();                  // vote disk e na rekhe reply dewa jabe na, nahole restart e double vote
        this.resetElectionTimer();
      }
    }
    const reply: RequestVoteReply = { type: "RequestVoteReply", term: this.term, voteGranted: grant };
    this.send(from, reply);
  }

  private onVoteReply(from: NodeId, m: RequestVoteReply) {
    if (this.role !== "candidate" || m.term !== this.term || !m.voteGranted) return;
    this.votes.add(from);
    if (this.votes.size * 2 > this.size) this.becomeLeader();
  }

  private onAppendEntries(from: NodeId, m: AppendEntries) {
    const fail = (conflictIndex: number) => {
      const r: AppendEntriesReply = { type: "AppendEntriesReply", term: this.term, success: false, matchIndex: 0, conflictIndex };
      this.send(from, r);
    };
    if (m.term < this.term) return fail(0);      // purono leader
    this.role = "follower";                       // ei term er valid leader ache
    this.leaderId = m.leaderId;
    this.resetElectionTimer();

    // Log matching check: leader er prevLogIndex/prevLogTerm amar log e ache ki?
    if (m.prevLogIndex > this.lastIndex()) return fail(this.lastIndex() + 1);
    if (this.termAt(m.prevLogIndex) !== m.prevLogTerm) {
      const t = this.termAt(m.prevLogIndex);
      let i = m.prevLogIndex;
      while (i > 1 && this.termAt(i - 1) === t) i--;   // oi term er shuru-te jai, leader ke bole dei (fast backup)
      return fail(i);
    }
    let idx = m.prevLogIndex;
    let changed = false;
    for (const e of m.entries) {
      idx++;
      if (idx <= this.lastIndex()) {
        if (this.termAt(idx) !== e.term) { this.log.length = idx - 1; this.log.push(e); changed = true; }   // conflict: amar uncommitted entry mushe fele
      } else { this.log.push(e); changed = true; }
      // (same term er same index = same entry, Raft Log Matching Property, tai skip)
    }
    if (changed) this.persist();
    const matchIndex = m.prevLogIndex + m.entries.length;
    if (m.leaderCommit > this.commitIndex) { this.commitIndex = Math.min(m.leaderCommit, matchIndex); this.applyCommitted(); }
    const r: AppendEntriesReply = { type: "AppendEntriesReply", term: this.term, success: true, matchIndex, conflictIndex: 0 };
    this.send(from, r);
  }

  private onAppendReply(from: NodeId, m: AppendEntriesReply) {
    if (this.role !== "leader" || m.term !== this.term) return;
    const match = this.matchIndex.get(from) ?? 0;
    if (m.success) {
      const newMatch = Math.max(match, m.matchIndex);
      this.matchIndex.set(from, newMatch);
      this.nextIndex.set(from, newMatch + 1);
      this.advanceCommit();
      if (newMatch < this.lastIndex()) this.sendAppend(from);     // follower ekhono pichone, pipeline kori
    } else {
      const next = this.nextIndex.get(from) ?? 1;
      this.nextIndex.set(from, Math.max(match + 1, Math.min(m.conflictIndex, next - 1), 1));
      this.sendAppend(from);
    }
  }

  // ---------- replication ----------
  private broadcastAppend() { for (const p of this.peers) this.sendAppend(p); }

  private sendAppend(to: NodeId) {
    const next = this.nextIndex.get(to) ?? this.lastIndex() + 1;
    const prev = next - 1;
    const m: AppendEntries = { type: "AppendEntries", term: this.term, leaderId: this.id, prevLogIndex: prev, prevLogTerm: this.termAt(prev),
      entries: this.log.slice(prev, prev + this.cfg.maxBatch), leaderCommit: this.commitIndex };
    this.send(to, m);
  }

  private advanceCommit() {
    for (let n = this.lastIndex(); n > this.commitIndex; n--) {
      if (this.termAt(n) !== this.term) break;    // shudhu NIJER term er entry majority count kore commit (purono term er ta indirectly)
      let count = 1;
      for (const p of this.peers) if ((this.matchIndex.get(p) ?? 0) >= n) count++;
      if (count * 2 > this.size) { this.commitIndex = n; break; }
    }
    this.applyCommitted();
  }

  // ---------- state machine ----------
  private applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied - 1]!;
      const result = this.applyCommand(entry.command);
      const p = this.pending.get(this.lastApplied);
      if (p) {
        this.pending.delete(this.lastApplied);
        p.cb(p.term === entry.term ? result : { error: "overwritten" });
      }
    }
  }

  private applyCommand(c: Command): KvResult {
    if (c.type === "noop") return { ok: true, value: null };
    const s = this.sessions.get(c.clientId);
    if (s && c.seq <= s.seq) return c.seq === s.seq ? s.result : { ok: false, value: null };   // duplicate (client retry): abar apply kori na
    const result = this.exec(c.op);
    this.sessions.set(c.clientId, { seq: c.seq, result });
    return result;
  }

  private exec(op: KvOp): KvResult {
    const cur = this.kv.get(op.key) ?? null;
    switch (op.kind) {
      case "get": return { ok: true, value: cur };
      case "put": this.kv.set(op.key, op.value); return { ok: true, value: null };
      case "delete": this.kv.delete(op.key); return { ok: true, value: cur };
      case "append": { const v = (cur ?? "") + op.suffix; this.kv.set(op.key, v); return { ok: true, value: v }; }
      case "cas":
        if (cur !== op.expected) return { ok: false, value: cur };
        this.kv.set(op.key, op.value);
        return { ok: true, value: op.value };
    }
  }

  // ---------- client API ----------
  /** Shudhu leader e kaj kore. (clientId, seq) diye retry safe: duibar pathale ekbar-i apply hoy. */
  propose(clientId: string, seq: number, op: KvOp, cb: (r: ProposeResult) => void) {
    if (this.role !== "leader") return cb({ error: "not_leader", leaderHint: this.leaderId });
    const s = this.sessions.get(clientId);
    if (s && seq <= s.seq) return cb(seq === s.seq ? s.result : { error: "stale_seq" });
    this.log.push({ term: this.term, command: { type: "kv", clientId, seq, op } });
    this.persist();
    this.pending.set(this.lastIndex(), { term: this.term, cb });
    this.broadcastAppend();
    this.advanceCommit();                  // single-node cluster e sorasori commit
  }

  private failPending(r: ProposeResult) {
    const p = [...this.pending.values()];
    this.pending.clear();
    for (const x of p) x.cb(r);
  }
}
