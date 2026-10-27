# Distributed Computing lab experiments in RateGuard-DC

Each experiment is a working part of the rate limiter, not a separate toy program. The **Show it** column says how to
demonstrate it from the console (http://localhost:9000) after `run.bat`.

| # | Experiment | Where it lives | Show it |
|---|---|---|---|
| 1 | Client–server with **gRPC (unary RPC)** and **RMI** | Unary gRPC: `TokenStore.Lease / Give / Acquire / Peek / WhoIsLeader` (edge → coordinator) in `edge/LeaderClient.java`, served by `coord/CoordNode.java`. RMI: `launcher/ClusterControl.java`, exported by `Launcher` on the registry at `:1099`, called by the console and the load balancer | Every allowed request is decided from tokens leased with the unary `Lease` RPC. Every button in the Test traffic drawer (kill, start, scale, slow) is an RMI call |
| 2 | **gRPC streaming**: server, client, bidirectional | Server streaming: `Replication.Replicate` (leader → follower bucket log) and `Peer.WatchStats` (edge → console, once a second). Client streaming: `TokenStore.ReportUsage` (an edge flushes queued give-backs and outage debts in one stream). Bidirectional: `TokenStore.Control` (edges send heartbeats up, the leader pushes policy and membership down) | The Edge nodes table is fed by `WatchStats` streams. Change a limit with Edit: the leader pushes it to every edge over `Control` within a second. Followers' bucket counts track the leader through `Replicate` |
| 4 | **Non-token mutual exclusion** (Ricart–Agrawala) | `edge/RicartAgrawala.java`: while no coordinator leader exists, edges update a replicated shared bucket inside an RA critical section (REQUEST with a Lamport timestamp to N−1 peers, wait for N−1 REPLYs, deferred replies on exit). Replies carry the bucket replica so the next holder sees the newest state | Run the **Mutual exclusion overhead** check: it scales edges to 3, 5 and 7, kills all coordinators, and measures messages per critical section ≈ **2(N−1)**. Manually: set *When no leader* to Ricart–Agrawala, kill all coordinators, start traffic |
| 5 | **Token-based mutual exclusion** (Suzuki–Kasami) | `edge/SuzukiKasami.java`: one token per resource carries the shared bucket, LN array and queue; requests carry RN sequence numbers | Same check: messages per critical section **≤ N**, and 0 while the token stays at one node |
| 6 | **Chandy–Misra–Haas deadlock detection** | `edge/PoolLocks.java`: in *pool* mode each tenant pool and key pool is homed on one edge; a refill locks both. With lock ordering off, even and odd edges lock in opposite orders, so wait-for cycles form. Blocked edges send edge-chasing PROBE(initiator, path) messages; a probe that returns to its initiator proves a cycle and the youngest transaction aborts | Run the **Deadlock detection** check: crossing locks give deadlocks > 0 (with probes and aborted victims); ordered locking gives 0 |
| 7 | **Load balancing** with at least 3 strategies under dynamic load | `lb/LoadBalancer.java`: round robin, least connections, least response time (EWMA × in-flight); backends discovered through RMI | Run the **Load balancing** check: edge-2 is slowed by 30 ms and each strategy is measured (throughput, p99, share sent to the slow node). Manually: slow an edge in the drawer and switch the strategy selector while traffic runs |
| 8 | **Consistency models** | `edge/Limiter.java`: *strong* (one synchronous `Acquire` RPC per request, linearizable), *bounded staleness* (leases: RateGuard's default), *eventual* (local view of the global bucket, synced once a second) | Run the **Consistency models** check: strong and bounded never exceed the limit; eventual overshoots because every edge starts from a full bucket. Manually: switch the Consistency selector during traffic and watch the coordinator-calls KPI |
| 9 | **Leader election** (ZooKeeper-style) | `coord/BullyElection.java` among 3 coordinator replicas: ELECTION to higher ids, OK, COORDINATOR announcement, heartbeats; the highest live id wins and terms only increase | Run the **Leader election** check, or press *Kill leader* in the drawer: the Coordinators table shows a new leader within about a second, traffic keeps flowing on local fallback limits, and the old leader takes over again when restarted |

## Measured on this machine (Run all checks)

| # | Result |
|---|---|
| 1, 2 | 0.58 coordinator calls per request with leases vs 1.00 with a unary RPC per request; +1.8 ms p50 / +4.3 ms p99 added |
| 4 | Ricart–Agrawala: 4.0, 8.0 and 12.0 messages per critical section for N = 3, 5, 7 (exactly 2(N−1)) |
| 5 | Suzuki–Kasami: 2.2, 4.1 and 6.7 messages per critical section for N = 3, 5, 7 (≤ N) |
| 6 | Crossing lock order: 19 deadlocks detected with 189 probes, 20 victims aborted; ordered locking: 0 |
| 7 | edge-2 slowed by 30 ms: p99 125 ms round robin (33 % of traffic to it), 125 ms least connections (5 %), 38.5 ms least response time (1 %) |
| 8 | strong −0.3 %, bounded −1.0 %, eventual +48 % vs the limit |
| 9 | Leader killed → new leader in 1.0 s with traffic flowing; the restarted highest id took over again in 4.5 s |

Not covered: experiment 3 (MPI master/worker speedup), the HDFS half of experiment 9, and experiment 10 (CUDA). These
don't map naturally onto a rate limiter.

## Unit tests per experiment

| Test | Experiment |
|---|---|
| `coord/BullyElectionTest` | 9: highest id wins; re-election after a crash; a returning higher id takes over with a higher term |
| `edge/DistributedAlgorithmsTest.ricartAgrawalaCosts2NMinus1MessagesPerEntry` | 4: exactly 2(N−1) messages for N = 3 and 5 |
| `edge/DistributedAlgorithmsTest.suzukiKasamiCostsNMessagesOrZero` | 5: 0 messages with the token held, N otherwise |
| `edge/DistributedAlgorithmsTest.bothAlgorithmsNeverOverAdmitUnderConcurrency` | 4, 5: 16 concurrent requesters on 4 nodes never take more than the pool |
| `edge/DistributedAlgorithmsTest.chandyMisraHaasDetectsAndResolvesCrossingLocks` | 6: crossing orders deadlock and are resolved; ordered locking never deadlocks |
| `coord/BucketStoreTest` | the token-bucket and sliding-window formulas shared by every mode |
