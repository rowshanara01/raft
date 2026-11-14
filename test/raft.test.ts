import { describe, expect, it } from "vitest";
import { Client, Cluster } from "../src/sim.js";
import { DEFAULT_CONFIG, type LogEntry, type ProposeResult } from "../src/types.js";

const elect = (c: Cluster) => { expect(c.runUntil(() => c.leaders().length === 1, 2000)).toBe(true); return c.leader()!; };
const allApplied = (c: Cluster) => c.live().every((n) => n.lastApplied === c.leader()!.lastApplied && n.commitIndex === c.leader()!.commitIndex);

describe("leader election", () => {
  it("ekjon-i leader, shobai ek term/leader e ekmot, violation nai", () => {
    const c = new Cluster(5, 1);
    const l = elect(c);
    c.run(500);
    expect(c.leaders()).toHaveLength(1);
    expect(c.live().every((n) => n.term === l.term && n.leaderId === l.id)).toBe(true);
    expect(c.violations).toEqual([]);
    expect(c.now).toBeLessThan(2000);
  });

  it("leader crash -> notun leader (boro term), purono leader restart hole follower hoy ar catch up kore", () => {
    const c = new Cluster(5, 2);
    const cli = new Client("a", c);
    const old = elect(c);
    expect(cli.exec({ kind: "put", key: "x", value: "1" })).toMatchObject({ ok: true });
    c.crash(old.id);
    expect(c.runUntil(() => c.leader() !== undefined && c.leader()!.id !== old.id, 3000)).toBe(true);
    const next = c.leader()!;
    expect(next.term).toBeGreaterThan(old.term);
    expect(cli.exec({ kind: "put", key: "y", value: "2" })).toMatchObject({ ok: true });
    c.restart(old.id);
    expect(c.settle()).toBe(true);
    expect(c.nodes.get(old.id)!.role).toBe("follower");
    for (const n of c.live()) expect([...n.kv]).toEqual([["x", "1"], ["y", "2"]]);
    c.checkLogMatching();
    expect(c.violations).toEqual([]);
  });

  it("single-node cluster-o kaj kore", () => {
    const c = new Cluster(1, 3);
    const cli = new Client("a", c);
    elect(c);
    expect(cli.exec({ kind: "put", key: "k", value: "v" })).toMatchObject({ ok: true });
    expect(cli.exec({ kind: "get", key: "k" })).toEqual({ ok: true, value: "v" });
  });
});

describe("replication & reads", () => {
  it("write shob node e, get (log diye, linearizable) shesh value dey; cas/delete/append", () => {
    const c = new Cluster(5, 4);
    const cli = new Client("a", c);
    elect(c);
    cli.exec({ kind: "put", key: "a", value: "1" });
    expect(cli.exec({ kind: "cas", key: "a", expected: "1", value: "2" })).toEqual({ ok: true, value: "2" });
    expect(cli.exec({ kind: "cas", key: "a", expected: "1", value: "3" })).toEqual({ ok: false, value: "2" });   // expected mile na
    expect(cli.exec({ kind: "append", key: "a", suffix: "!" })).toEqual({ ok: true, value: "2!" });
    expect(cli.exec({ kind: "get", key: "a" })).toEqual({ ok: true, value: "2!" });
    expect(cli.exec({ kind: "delete", key: "a" })).toEqual({ ok: true, value: "2!" });
    expect(cli.exec({ kind: "get", key: "a" })).toEqual({ ok: true, value: null });
    expect(c.runUntil(() => allApplied(c), 1000)).toBe(true);
    expect(c.live().every((n) => n.kv.size === 0)).toBe(true);
  });

  it("EXACTLY-ONCE: client retry (same clientId+seq) duibar pathale ekbar-i apply", () => {
    const c = new Cluster(3, 5);
    const l = elect(c);
    const results: ProposeResult[] = [];
    l.propose("cli", 1, { kind: "append", key: "k", suffix: "x" }, (r) => results.push(r));
    l.propose("cli", 1, { kind: "append", key: "k", suffix: "x" }, (r) => results.push(r));      // network retry
    c.runUntil(() => results.length === 2, 1000);
    expect(results[0]).toEqual(results[1]);
    l.propose("cli", 1, { kind: "append", key: "k", suffix: "x" }, (r) => results.push(r));      // apply howar por-o retry: cached result
    expect(results[2]).toEqual(results[0]);
    l.propose("cli", 2, { kind: "append", key: "k", suffix: "y" }, (r) => results.push(r));
    c.runUntil(() => results.length === 4, 1000);
    expect(results[3]).toEqual({ ok: true, value: "xy" });
    l.propose("cli", 1, { kind: "append", key: "k", suffix: "x" }, (r) => results.push(r));      // seq 2 apply howar por seq 1 = stale
    expect(results[4]).toEqual({ error: "stale_seq" });
    c.runUntil(() => allApplied(c), 1000);
    expect(c.live().map((n) => n.kv.get("k"))).toEqual(["xy", "xy", "xy"]);
  });

  it("follower propose reject kore leader hint dey", () => {
    const c = new Cluster(3, 6);
    const l = elect(c);
    c.run(200);
    const f = c.live().find((n) => n.id !== l.id)!;
    let r: ProposeResult | undefined;
    f.propose("c", 1, { kind: "get", key: "k" }, (x) => { r = x; });
    expect(r).toEqual({ error: "not_leader", leaderHint: l.id });
  });
});

describe("partitions & quorum", () => {
  it("minority leader commit korte pare na; majority notun leader; heal e stale write DISCARD, log converge", () => {
    const c = new Cluster(5, 7);
    const cli = new Client("a", c);
    const old = elect(c);
    cli.exec({ kind: "put", key: "before", value: "1" });
    const buddy = c.live().find((n) => n.id !== old.id)!;
    const majority = c.ids.filter((i) => i !== old.id && i !== buddy.id);
    c.partition([old.id, buddy.id], majority);

    let stale: ProposeResult | undefined;
    old.propose("zombie", 1, { kind: "put", key: "stale", value: "LOST" }, (r) => { stale = r; });   // purono leader nijeke leader mone kore
    c.run(100);
    expect(stale).toBeUndefined();                                    // majority nai, commit hoyni, ack-o na

    expect(c.runUntil(() => c.live().some((n) => majority.includes(n.id) && n.role === "leader"), 3000)).toBe(true);
    expect(cli.exec({ kind: "put", key: "after", value: "2" })).toMatchObject({ ok: true });
    expect(old.commitIndex).toBeLessThan(c.live().find((n) => majority.includes(n.id) && n.role === "leader")!.commitIndex);

    c.heal();
    expect(c.settle()).toBe(true);
    expect(stale).toEqual({ error: "leadership_lost" });             // purono leader boro term dekhe step down, client ke jani
    for (const n of c.live()) expect([...n.kv].sort()).toEqual([["after", "2"], ["before", "1"]]);   // "stale" kothao nai
    expect(new Set(c.live().map((n) => JSON.stringify(n.log))).size).toBe(1);
    c.checkLogMatching();
    expect(c.violations).toEqual([]);
  });

  it("quorum chhara (3/5 down) kono write commit hoy na; restart e progress shuru", () => {
    const c = new Cluster(5, 8);
    const cli = new Client("a", c);
    const l = elect(c);
    cli.exec({ kind: "put", key: "k", value: "1" });
    const followers = c.live().filter((n) => n.id !== l.id).slice(0, 3).map((n) => n.id);
    followers.forEach((id) => c.crash(id));
    const before = l.commitIndex;
    cli.submit({ kind: "put", key: "k", value: "2" });
    c.run(1500);
    expect(cli.busy).toBe(true);                                      // ack nai
    expect(c.nodes.get(l.id)!.commitIndex).toBe(before);              // commit-o hoyni
    followers.forEach((id) => c.restart(id));
    expect(c.runUntil(() => !cli.busy, 5000)).toBe(true);
    expect(cli.exec({ kind: "get", key: "k" })).toEqual({ ok: true, value: "2" });
    expect(c.violations).toEqual([]);
  });

  it("ELECTION RESTRICTION: pichiye thaka (stale) node, jar term onek boro, committed data mushte pare na", () => {
    const c = new Cluster(5, 9);
    const cli = new Client("a", c);
    const l = elect(c);
    const stale = c.live().find((n) => n.id !== l.id)!.id;
    c.partition([stale], c.ids.filter((i) => i !== stale));
    for (let i = 0; i < 5; i++) cli.exec({ kind: "put", key: `k${i}`, value: `${i}` });
    c.run(2000);                                                      // stale node bar bar election kore term barachchhe
    expect(c.nodes.get(stale)!.term).toBeGreaterThan(c.leader()!.term);
    c.heal();
    expect(c.settle()).toBe(true);
    for (const n of c.live()) expect(n.kv.size).toBe(5);              // committed 5ta-i ache
    expect(c.violations).toEqual([]);
  });
});

describe("durability (persistence)", () => {
  it("shob node crash + restart: committed data thake, term kome na", () => {
    const c = new Cluster(5, 10);
    const cli = new Client("a", c);
    const l = elect(c);
    for (let i = 0; i < 4; i++) cli.exec({ kind: "put", key: `k${i}`, value: `v${i}` });
    const termBefore = l.term;
    for (const id of c.ids) c.crash(id);
    for (const id of c.ids) c.restart(id);
    expect(c.settle()).toBe(true);
    expect(c.leader()!.term).toBeGreaterThanOrEqual(termBefore);
    expect(cli.exec({ kind: "get", key: "k3" })).toEqual({ ok: true, value: "v3" });
    expect(c.violations).toEqual([]);
  });

  it("vote persist: restart er por same term e onno candidate ke vote dey na (double vote = duijon leader)", () => {
    const c = new Cluster(3, 11);
    const rv = (candidateId: number) => ({ type: "RequestVote" as const, term: 5, candidateId, lastLogIndex: 0, lastLogTerm: 0 });
    c.nodes.get(1)!.receive(2, rv(2));
    expect(c.storages.get(1)).toMatchObject({ term: 5, votedFor: 2 });   // disk e likha
    c.crash(1); c.restart(1);
    c.nodes.get(1)!.receive(3, rv(3));
    expect(c.storages.get(1)!.votedFor).toBe(2);                         // 3 ke vote dey ni
    expect(c.nodes.get(1)!.votedFor).toBe(2);
  });
});

describe("hostile network", () => {
  it("20% drop, 10% duplicate, 1-30ms latency (reorder): tobu safe ar progress kore", () => {
    const c = new Cluster(5, 12);
    c.net = { minLatency: 1, maxLatency: 30, dropRate: 0.2, dupRate: 0.1 };
    const cli = new Client("a", c);
    for (let i = 0; i < 20; i++) expect(cli.exec({ kind: "append", key: "log", suffix: `${i},` }, 20_000)).toMatchObject({ ok: true });
    expect(c.settle()).toBe(true);
    const expected = Array.from({ length: 20 }, (_, i) => `${i},`).join("");
    for (const n of c.live()) expect(n.kv.get("log")).toBe(expected);
    c.checkLogMatching();
    expect(c.violations).toEqual([]);
  });
});

describe("Raft paper Figure 8 (directed scenario)", () => {
  // Leader nijer term er kono entry commit na kore, purono term er entry shudhu "majority te ache" dekhe commit korle,
  // porer leader oi "committed" entry mushe dite pare. (§5.4.2). Random chaos eta dhorte pare na (onek rare), tai hate gore scenario.
  const put = (term: number, v: string): LogEntry => ({ term, command: { type: "kv", clientId: "c", seq: term, op: { kind: "put", key: "k", value: v } } });
  const base: LogEntry = { term: 1, command: { type: "noop" } };

  it("purono term er entry replica gune commit hoy na; porer leader overwrite korleo safety bhange na", () => {
    const c = new Cluster(5, 1, { ...DEFAULT_CONFIG, leaderNoop: false }, {
      1: { term: 2, votedFor: 1, log: [base, put(2, "x")] },     // S1: term 2 er leader chhilo, "x" shudhu S2 te replicate korte perechhilo
      2: { term: 2, votedFor: 1, log: [base, put(2, "x")] },
      3: { term: 3, votedFor: 5, log: [base] },
      4: { term: 3, votedFor: 5, log: [base] },
      5: { term: 3, votedFor: 5, log: [base, put(3, "y")] },     // S5: term 3 er leader chhilo, "y" kauke pathate pare ni
    });
    c.crash(5);
    expect(c.runUntil(() => c.leader() !== undefined, 5000)).toBe(true);
    const L = c.leader()!;
    expect([1, 2]).toContain(L.id);                              // shudhu S1/S2 ei up-to-date
    expect(c.runUntil(() => c.live().filter((n) => n.log.length >= 2).length >= 3, 2000)).toBe(true);   // "x" ekhon majority te
    c.run(300);
    expect(L.commitIndex).toBe(0);                               // KEY: majority te thakleo commit na, karon ei term er kono entry nai

    const other = c.live().find((n) => n.id !== L.id && n.id !== 5 && n.log.length >= 2)!.id;
    c.crash(L.id);
    c.crash(other);              // baki 2 node (last term 2) nijera majority pay na (3 vote lage), S5 ke vote dite hobe
    c.restart(5);
    expect(c.runUntil(() => c.leader()?.id === 5, 8000)).toBe(true);   // S5 jite (tar log beshi up-to-date: last term 3 > 2) ar "x" mushe dey
    const cli = new Client("z", c);
    expect(cli.exec({ kind: "put", key: "k", value: "z" })).toMatchObject({ ok: true });
    c.run(500);
    expect(c.violations).toEqual([]);
    for (const n of c.live()) expect(n.kv.get("k")).toBe("z");
  });
});
