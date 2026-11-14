# raft-kv: Distributed Key-Value Store (Raft Consensus) + Deterministic Simulation Testing

> Complete English version: [README.en.md](README.en.md)

Raft holo ekta **consensus algorithm**: kichu server er ekta cluster jokhon "ekta computer er moto" kaj kore,
ekta server crash korle ba network bhenge gele-o data hariye jay na.
**etcd** (Kubernetes er mathay), **Consul**, **CockroachDB**, **TiKV**, **Kafka (KRaft)**: shob e eta use kore.
Eta nijer hate banale distributed system er shobcheye kothin ongsho bujhbe.

## Chalanor niyom (Node 20+, kono dependency nai)
```
npm install
npm test            # 18ta test (chaos shoho)
npm run demo        # 5-node cluster: leader crash, partition, heal, console e timeline
```

## Ki ki implement kora
- **Leader election**: randomized timeout, term, majority vote, election restriction (log up-to-date check)
- **Log replication**: AppendEntries, log matching check, fast conflict backup, conflicting entry overwrite, batching
- **Commit rule**: majority replicate hole commit, kintu shudhu **nijer term er** entry count kore (paper section 5.4.2, Figure 8); leader no-op (section 8)
- **Persistence**: term, votedFor, log disk e (vote disk e na likhe reply dewa jay na)
- **Replicated state machine**: put / get / delete / append / cas. **get-o log er moddhe diye jay (linearizable read)**
- **Exactly-once**: (clientId, seq) session dedup: client timeout e retry korle-o ekbar-i apply hoy
- **Client**: timeout hole onno node try kore, not_leader hint follow kore

## Testing: Deterministic Simulation (FoundationDB / TigerBeetle er technique)
Network, shomoy, crash: shob nijer simulator e. Seeded PRNG: **same seed = hubohu same execution**, tai bug dhorle seed diye replay kora jay.
Simulator: message delay / drop / duplicate / reorder, partition, node crash + restart (volatile state haray, disk thake).

**Protita tick e invariant check:**
1. Ekta term e ekjon-i leader (Election Safety)
2. Committed entry kokhono bodlay na (State Machine Safety)
3. Log Matching Property

Chaos result (30 seed x 10 second, 5 node, 3 concurrent client):

| | |
|---|---|
| node crash | 567 |
| node restart | 534 |
| network partition | 816 |
| ack paoa client write | 22,445 |
| safety violation | **0** |
| max term (election er sonkha) | 43 |

Chaos shesh e prottek run e prove kora: shob node er state ek, **ack paoa kono write hariyeni, duibar apply hoyni** (retry shotteo hoyechhe),
ar protita GET ager shob acked write dekhechhe (single-writer linearizability).
Aro: 3 ar 7 node, **jor sonkhok node (2, 4, 6)**, ar directed scenario **Figure 8** (section 5.4.2).

## Test ki sotti bug dhore? (Mutation testing: ami ichchhakrito bug dhukiye dekhechhi)

| Injected bug | Test dhorlo? |
|---|---|
| Election restriction bondho | ha (5ta test fail) |
| Vote disk e likha hoy na | ha |
| Purono term er entry replica gune commit (Figure 8) | ha (directed test) |
| Client dedup nai | ha |
| Conflicting entry truncate hoy na | ha |
| Election majority off-by-one | ha (jor sonkhok cluster test) |
| Purono term er AppendEntries mene nei | ha |
| Log up-to-date check e term bad | ha |
| Boro term dekhe step down kore na | ha |
| Commit quorum majority er cheye kom | ha (4ta test fail) |
| Commit e `>=` half (even cluster) | **na**: eta ashole safe (equivalent mutant). Commit quorum (n/2) + election quorum (n/2+1) > n, tai overlap thake |

Ekta shikha: shudhu random chaos Figure 8 bug **dhorte pare ni** (150 seed e 0 ta). Hate gore directed scenario lagse.
Test pass kora mane test bhalo na. Test fail korte pare kina, seta mapte hoy.

## Shimabodhdhota (sotti kotha, production e jete hole ja lagbe)
- **Real network/disk nai**: eta simulator. Production e TCP/gRPC transport + WAL file + **fsync before reply** lagbe (`persist()` shei jaygay boshbe).
- **Snapshot / log compaction nai**: log chirokal bare, restart e shob replay hoy.
- **Membership change nai** (node jora/bad dewa: joint consensus).
- **Read log diye jay** (correct kintu dhimi). Production e ReadIndex / lease read.
- **PreVote / CheckQuorum nai**: partition theke ferot asha node er boro term cluster ke disturb kore (chaos e max term 43 tai).
- Single process, performance optimization nai.
- Linearizability check shudhu single-writer register er jonno. Full Jepsen/Porcupine-style checker nai.

## Challenge (kothin theke aro kothin)
1. Snapshot + InstallSnapshot RPC
2. PreVote + CheckQuorum, term inflation komao (chaos max term compare koro)
3. ReadIndex read (log chhara linearizable read)
4. Membership change (joint consensus), tarpor chaos e node jora/bad
5. Real TCP transport + file WAL (fsync), 3ta process, `kill -9` diye test
6. Porcupine-style linearizability checker likho, shob client er history check koro
7. Multi-Raft: key range diye shard (CockroachDB / TiKV er mul idea)
