// npm run demo: 5-node Raft cluster, leader mara jay, partition hoy, tobu data hariye jay na
import { Client, Cluster } from "./sim.js";

const c = new Cluster(5, 7);
const cli = new Client("demo", c);
const role = () => c.live().map((n) => `S${n.id}:${n.role[0]!.toUpperCase()}(t${n.term})`).join(" ");
const say = (m: string) => console.log(`[t=${String(c.now).padStart(5)}ms] ${m}`);

c.runUntil(() => c.leaders().length === 1, 3000);
say(`leader election: S${c.leader()!.id} jitlo (term ${c.leader()!.term})   ${role()}`);

for (const [k, v] of [["name", "Robia"], ["role", "ML engineer"], ["city", "Dhaka"]] as const) {
  cli.exec({ kind: "put", key: k, value: v });
  say(`PUT ${k}=${v}  -> majority te commit hoyeche`);
}

const old = c.leader()!;
c.crash(old.id);
say(`*** leader S${old.id} CRASH ***`);
c.runUntil(() => c.leader() !== undefined && c.leader()!.id !== old.id, 3000);
say(`notun leader S${c.leader()!.id} (term ${c.leader()!.term})   ${role()}`);
say(`GET name -> ${JSON.stringify(cli.exec({ kind: "get", key: "name" }))}   (data hariye jayni)`);
cli.exec({ kind: "put", key: "status", value: "written-while-leader-down" });
c.restart(old.id);
c.settle();
say(`S${old.id} restart hoye catch up korlo, ekhon 5 node-i live   ${role()}`);

const l2 = c.leader()!;
const buddy = c.live().find((n) => n.id !== l2.id)!;
const rest = c.ids.filter((i) => i !== l2.id && i !== buddy.id);
c.partition([l2.id, buddy.id], rest);
say(`*** NETWORK PARTITION: {S${l2.id},S${buddy.id}} (minority) | {${rest.map((i) => "S" + i).join(",")}} (majority) ***`);
let zombie = "pending";
l2.propose("z", 1, { kind: "put", key: "zombie", value: "should-be-lost" }, (r) => { zombie = JSON.stringify(r); });
c.run(1500);
say(`minority leader S${l2.id} er write commit hoyni (result: ${zombie}); majority te notun leader: ${role()}`);
const l3 = c.leaders().sort((a, b) => b.term - a.term)[0]!;
const cli2 = new Client("demo2", c);
cli2.exec({ kind: "put", key: "after-partition", value: "committed-by-majority" });
say(`majority side e write hoyeche (leader S${l3.id}, term ${l3.term})`);

c.heal();
c.settle();
say(`*** HEAL ***  ${role()}`);
say(`purono minority leader er write: ${zombie}  (client ke bola holo, data-o nai)`);
for (const n of c.live()) console.log(`   S${n.id} kv = ${JSON.stringify(Object.fromEntries(n.kv))}`);
c.checkLogMatching();
say(c.violations.length ? `VIOLATIONS: ${c.violations.join("; ")}` : "safety invariant: kono violation nai (1 leader/term, committed entry kokhono bodlayni, log matching)");
