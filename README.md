#### raf A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation # raft-kv: A Distributed Key-Value  ####
Store Built on Raft, Tested with Deterministic Simulation apply

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

#### Quick start t0 ####
Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

#### What is implemented ####

| Area | Details about |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

#### Project layout ####

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works ##

#### Deterministic simulation ####

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

#### Safety invariants checked on every tick ####

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results ##

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
## raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
# raft-kv: A Distributed Key-Value Store Built on Raft, Tested with Deterministic Simulation

A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.


A from-scratch implementation of the **Raft consensus algorithm** in TypeScript, plus a **deterministic simulator** that
injects crashes, network partitions, and message loss/duplication/reordering while checking safety invariants on every tick.

Raft is what lets a cluster of servers behave like one reliable machine: if a server crashes or the network splits,
acknowledged data is not lost and the cluster never gives conflicting answers. Production systems built on Raft include
**etcd** (the datastore behind Kubernetes), **Consul**, **CockroachDB**, **TiKV**, and **Kafka (KRaft mode)**.

> A Banglish version of this document is in [README.md](README.md).

---

## Quick start

Requires Node.js 20+. There are no runtime dependencies.

```bash
npm install
npm test        # 18 tests, including randomized chaos runs
npm run demo    # a 5-node cluster: leader crash, partition, heal, printed as a timeline
```

Using the simulator directly:

```ts
import { Client, Cluster } from "./src/sim.js";

const cluster = new Cluster(5, /* seed */ 42);          // 5 nodes, deterministic
const client = new Client("alice", cluster);

cluster.runUntil(() => cluster.leaders().length === 1, 2000);   // wait for a leader
client.exec({ kind: "put", key: "x", value: "1" });             // replicated write
cluster.crash(cluster.leader()!.id);                            // kill the leader
cluster.runUntil(() => cluster.leader() !== undefined, 3000);   // a new leader is elected
client.exec({ kind: "get", key: "x" });                         // -> { ok: true, value: "1" }
console.log(cluster.violations);                                // -> [] (no safety violations)
```

---

## What is implemented

| Area | Details |
|---|---|
| **Leader election** | Randomized election timeouts, terms, majority voting, the **election restriction** (a candidate with a less up-to-date log cannot win) |
| **Log replication** | `AppendEntries` with the log-matching check, overwriting of conflicting uncommitted entries, batching, fast conflict backup using a conflict-index hint |
| **Commit rule** | An entry is committed when a majority stores it, but a leader only counts replicas for entries **from its own term** (Raft paper section 5.4.2, "Figure 8"). A leader appends a **no-op** on election (section 8) so older entries can be committed |
| **Persistence** | `currentTerm`, `votedFor`, and the log are written to (simulated) disk **before** any message that depends on them is sent |
| **State machine** | `put`, `get`, `delete`, `append`, `cas` (compare-and-set). Reads go **through the log**, so they are linearizable |
| **Exactly-once semantics** | Each client request carries `(clientId, seq)`. The state machine keeps a per-client session, so a retried request is applied once and the cached result is returned |
| **Client** | Retries on timeout against a different node, follows `not_leader` hints |

---

## Project layout

```
src/
  types.ts    Message types, log entries, KV operations, config (election timeout, heartbeat, batch size)
  raft.ts     The Raft node: election, replication, commit, state machine, client proposals
  sim.ts      Deterministic simulator: network, clock, crash/restart, partitions, invariant checker, retrying client
  chaos.ts    Randomized fault injection plus end-of-run verification
  demo.ts     Console demo (npm run demo)
test/
  raft.test.ts    Scenario tests (election, replication, partitions, durability, hostile network, Figure 8)
  chaos.test.ts   Randomized chaos runs across seeds and cluster sizes, determinism check
```

---

## How the testing works

### Deterministic simulation

All sources of nondeterminism are controlled by the simulator: the clock (virtual milliseconds advanced one tick at a time),
message latency, drops, duplication, partitions, and crashes. Everything is driven by a seeded pseudo-random generator, so
**the same seed always produces the exact same execution**. If a run finds a bug, you can replay it exactly.
This is the same idea used to test FoundationDB and TigerBeetle.

The simulated faults:

- Message delay (random latency, which also causes reordering), drops, and duplication
- Network partitions (including cutting messages that are already in flight)
- Node crash and restart: volatile state is lost, the simulated disk survives

### Safety invariants checked on every tick

1. **Election Safety**: at most one leader per term.
2. **State Machine Safety**: once an entry is committed at an index, no node ever commits a different entry at that index.
3. **Log Matching**: if two logs have an entry with the same index and term, their prefixes are identical (checked at the end of a run).

### End-to-end checks after each chaos run

After the faults stop, the cluster is healed and every client is allowed to finish. Each client appends unique tokens to its own
key and occasionally reads it back. The run then verifies:

- every node ends in the same state,
- **no acknowledged write was lost and none was applied twice**, even though requests were retried against different leaders,
- every `get` returned exactly the writes that client had already been acknowledged (a single-writer linearizability check).

---

## Results

Chaos run: 30 seeds, 10 simulated seconds each, 5 nodes, 3 concurrent clients, random crashes/partitions/message faults.

| Metric | Value |
|---|---|
| Node crashes injected | 567 |
| Node restarts | 534 |
| Network partitions | 816 |
| Client writes acknowledged | 22,445 |
| **Safety violations** | **0** |
| Highest term reached (election count proxy) | 43 |

The test suite also covers 3 and 7 nodes, **even-sized clusters (2, 4, 6)**, a 20% drop / 10% duplication network,
a majority-loss scenario, durability after crashing every node, and a directed reproduction of the Raft paper's Figure 8.

---

## Does the test suite actually catch bugs? (mutation testing)

Passing tests only matter if they can fail. I deliberately injected known Raft bugs into `raft.ts` and checked whether the suite noticed.

| Injected bug | Caught? |
|---|---|
| Election restriction disabled | Yes (5 tests failed) |
| Vote not persisted before replying | Yes |
| Committing an old-term entry by counting replicas (Figure 8 bug) | Yes, by a directed test |
| No client deduplication | Yes |
| Conflicting log entries not truncated | Yes |
| Election majority off by one | Yes (needs even-sized cluster tests) |
| Accepting `AppendEntries` from a stale term | Yes |
| Up-to-date check ignores the term | Yes |
| Not stepping down on seeing a higher term | Yes |
| Commit quorum smaller than a majority | Yes (4 tests failed) |
| Committing at exactly half of an even-sized cluster | **No. This one is not actually a bug** (see below) |

Two things I learned while doing this:

1. **Random chaos alone did not find the Figure 8 bug** (0 violations in 150 seeds). It needs a very specific interleaving, so I wrote a directed
   test that sets up the exact scenario from the paper. Random testing and hand-crafted scenarios complement each other.
2. The surviving mutant is an *equivalent mutant*. Committing with `n/2` acknowledgements in a cluster of even size `n` is still safe,
   because commit quorum (`n/2`) plus election quorum (`n/2 + 1`) exceeds `n`, so the two always overlap. The mutation changes behavior but not safety.

---

## Design notes worth knowing

- **Persist before you reply.** A node writes `votedFor` and `currentTerm` to disk before granting a vote. Otherwise a restart could let it vote twice in one term and produce two leaders.
- **Leader no-op.** A new leader cannot commit entries from earlier terms by counting replicas alone, so it appends a no-op in its own term. Committing that no-op commits everything before it.
- **Reads go through the log.** This makes reads linearizable without extra machinery, at the cost of latency. Production systems use ReadIndex or leader leases.
- **Client sessions.** Retries are the normal case in distributed systems (timeouts are ambiguous), so the state machine deduplicates by `(clientId, seq)`.
- **Fast backup.** When a follower rejects `AppendEntries`, it returns the first index of its conflicting term so the leader can skip back many entries at once.

---

## Limitations (what is missing for production use)

- **Simulated network and disk.** There is no real TCP/gRPC transport and no on-disk write-ahead log. A real system must `fsync` before replying; `persist()` marks the place.
- **No snapshots or log compaction.** The log grows forever and a restarted node replays all of it.
- **No cluster membership changes** (adding or removing nodes, joint consensus).
- **Reads are slow** because they go through the log.
- **No PreVote or CheckQuorum.** A node that was partitioned away keeps increasing its term and disrupts the cluster when it rejoins. This is why the highest term in the chaos runs is so large.
- **Single process, no performance tuning.**
- **The linearizability check is limited** to single-writer registers. It is not a general Jepsen/Porcupine-style checker.

---

## Challenges (roughly increasing difficulty)

1. Snapshots and the `InstallSnapshot` RPC.
2. PreVote and CheckQuorum; measure how much they reduce term inflation in the chaos runs.
3. ReadIndex reads (linearizable reads without writing to the log).
4. Membership changes (joint consensus), then add node joins/leaves to the chaos test.
5. A real TCP transport and an on-disk WAL with `fsync`; run three processes and test with `kill -9`.
6. Write a Porcupine-style linearizability checker and verify full client histories.
7. Multi-Raft: shard the key space into ranges, each with its own Raft group (the core idea behind CockroachDB and TiKV).

---

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus Algorithm* (the Raft paper), USENIX ATC 2014.
- Diego Ongaro, *Consensus: Bridging Theory and Practice* (PhD thesis, 2014): snapshots, membership changes, client semantics.
- FoundationDB's talk on simulation testing, and TigerBeetle's simulator, for deterministic simulation testing.
- Jepsen (jepsen.io) and Porcupine for linearizability checking.
