export type NodeId = number;

/** Replicated state machine er operation: key-value store */
export type KvOp =
  | { kind: "put"; key: string; value: string }
  | { kind: "get"; key: string }                               // read-o log er moddhe diye jay (linearizable)
  | { kind: "delete"; key: string }
  | { kind: "append"; key: string; suffix: string }            // duibar apply hole dhora porbe (exactly-once test er jonno)
  | { kind: "cas"; key: string; expected: string | null; value: string };

export type Command = { type: "noop" } | { type: "kv"; clientId: string; seq: number; op: KvOp };
export interface LogEntry { term: number; command: Command }

export interface KvResult { ok: boolean; value: string | null }
export type ProposeResult = KvResult | { error: "not_leader"; leaderHint: NodeId | null } | { error: "leadership_lost" | "overwritten" | "stale_seq" };

export interface RequestVote { type: "RequestVote"; term: number; candidateId: NodeId; lastLogIndex: number; lastLogTerm: number }
export interface RequestVoteReply { type: "RequestVoteReply"; term: number; voteGranted: boolean }
export interface AppendEntries { type: "AppendEntries"; term: number; leaderId: NodeId; prevLogIndex: number; prevLogTerm: number; entries: LogEntry[]; leaderCommit: number }
export interface AppendEntriesReply { type: "AppendEntriesReply"; term: number; success: boolean; matchIndex: number; conflictIndex: number }
export type Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply;

/** Crash er por-o ja thake (disk). Raft paper Figure 2: currentTerm, votedFor, log */
export interface Storage { term: number; votedFor: NodeId | null; log: LogEntry[] }

export interface NodeEnv {
  send(to: NodeId, msg: Message): void;
  now(): number;
  rng(): number;
  onLeader?(id: NodeId, term: number): void;   // checker er jonno
}

export interface RaftConfig { electionMin: number; electionMax: number; heartbeat: number; maxBatch: number; leaderNoop: boolean }
export const DEFAULT_CONFIG: RaftConfig = { electionMin: 150, electionMax: 300, heartbeat: 50, maxBatch: 64, leaderNoop: true };
