# RateGuard-DC design

RateGuard-DC is the cloud RateGuard (edge gateways + Redis on Kubernetes) rebuilt as plain JVM processes. Redis is
replaced by a replicated coordinator ensemble, the Kubernetes Service by a load balancer, and the Kubernetes API by an
RMI control object. Everything else (API keys, three-level limits, leases, the console) is the same product.

## Processes

```
                 console :9000 ──RMI──► launcher :1099 (ClusterControl: kill / start / scale / slow)
                    │  WatchStats (server stream)          │ starts every JVM
 clients ──► lb :8080 ──► edge-1..N  HTTP :8101+ ──► upstream :8201-8203
           rr | leastconn | leasttime     │  ▲ peer gRPC :9101+ (RA, Suzuki-Kasami, pool locks + CMH)
                                          │  │
               Lease/Give/Acquire/Peek ───┘  └── Control (bidi: hello up, policy + membership down)
               ReportUsage (client stream)
                                          ▼
                     coord-1  coord-2  coord-3   gRPC :9201-9203
                     Bully election; leader streams bucket changes to followers (Replicate)
```

## Request path (edge)

1. **Authenticate** locally: HMAC-SHA256 API key `tenant.key.sig` or an HS256 JWT. A bad credential is answered with
   401 before any coordinator call. Verified claims are cached in a sharded LRU.
2. **Rate-limit** at three levels: tenant, API key, client IP. Each level is a token bucket
   `tokens = min(cap, tokens + Δt·rate)` plus a weighted two-window sliding cap (the same formula as the cloud
   version's Lua scripts, now in `coord/BucketStore.java`). Denials return 429 with `X-RateLimit-Level`.
3. **Proxy** to an upstream replica.

## Consistency models (Exp 8)

| Model | How an edge decides | Coordinator calls / request | Accuracy |
|---|---|---|---|
| Strong | one synchronous `Acquire` RPC per request; the leader checks all levels atomically | 1.00 | linearizable: never over |
| **Bounded staleness** (default) | edges lease token batches (adaptive 1→50, TTL 500 ms); unused tokens are given back on expiry | ≈ 0.02 to 0.6 | never over; can under-admit by the tokens parked in leases |
| Eventual | each edge spends from a local view of the global bucket and syncs once a second (`Peek`) | ≈ 0.005 | overshoots: every edge starts from a full bucket |

## Coordinator ensemble (Exp 9, Exp 2)

* **Bully election** among 3 replicas: ELECTION to higher ids, OK, COORDINATOR; followers heartbeat the leader and
  start an election after 2 misses; terms only increase (a winner asks every peer's term first).
* A leader that can still reach a higher id hands over (so a missed announcement cannot leave two leaders), and the
  election RPCs reset gRPC's reconnect backoff, so a restarted replica is seen at once.
* The leader streams every bucket change to followers over `Replicate` (server streaming, snapshot first).
* **Policy durability**: every replica writes the policy to `data/coord-N.policy`; a new leader adopts the highest
  version held by any live peer. Limits survive a restart of one replica or the whole ensemble.
* The leader pushes policy and edge membership over the `Control` bidirectional stream once a second. A new leader
  waits 2.5 s (2.5 heartbeats) before sending membership, so edges never adopt a half-filled list; a demoted leader
  closes its streams so edges reconnect to the new one.

## No leader: outage modes (Exp 4, 5, 6)

| Mode | Mechanism | Cost |
|---|---|---|
| local | each edge enforces cap ÷ N on its own | 0 messages, approximate |
| ra | **Ricart–Agrawala**: a replicated shared bucket per resource, updated inside a critical section (REQUEST with Lamport timestamp, N−1 REPLYs, deferred replies). Replies carry the bucket replica and echo the request's timestamp | 2(N−1) messages per entry |
| token | **Suzuki–Kasami**: the bucket travels inside the token (RN / LN / queue) | ≤ N messages, 0 while the token stays |
| pool | tenant and key pools homed on edges (hash % N); a refill locks both. With lock ordering off, even/odd edges lock in opposite orders. **Chandy–Misra–Haas** probes along the wait-for chain detect cycles; the youngest transaction aborts | probes only while blocked |

When the leader returns, edges charge it for what they admitted locally (drift compensation) and leave fallback.

## Load balancing (Exp 7)

Round robin, least connections, and least response time (EWMA latency × (in-flight + 1)). Backends are discovered from
the launcher over RMI every second; passive health checks drop a failing edge for 1 s.

## Measured results

One laptop (Windows 10, Java 21) runs all ~12 JVMs plus the load generator. Taken from **Run all checks**.

| Check | Result |
|---|---|
| Unauthenticated traffic | 12.2k forged requests → 12.2k × 401 at the edge, 0 coordinator calls |
| Burst accuracy | 4,627 allowed vs 4,814 permitted (−3.9 %), never over |
| Hierarchy | tenant, API key and IP tests each stopped by their own level (1,997 / 2,003; 497 / 500; 595 / 600) |
| Added latency | +1.80 ms p50, +4.30 ms p99 vs direct; 0.58 vs 1.00 coordinator calls per request (strong) |
| Consistency models | strong −0.3 % (1.000 calls/req) · bounded −1.0 % (0.021) · eventual +48 % (0.005) |
| Leader election | coord-3 killed → coord-2 elected in 1.0 s, traffic kept flowing; coord-3 took leadership back 4.5 s after restart |
| Mutual exclusion | messages per critical section: N=3 RA 4.0 / token 2.2 · N=5 RA 8.0 / token 4.1 · N=7 RA 12.0 / token 6.7 |
| Deadlock detection | crossing lock order: 19 deadlocks detected, 189 probes, 20 victims aborted; ordered locking: 0 |
| Load balancing (edge-2 +30 ms) | p99: round robin 125 ms (33 % to slow node) · least connections 125 ms (5 %) · least response time 38.5 ms (1 %) |
| Throughput | 6,936 req/s through LB + auth + 3 limits + proxy (p50 16.5 ms, p99 54.5 ms) |

## Bugs found by running it live

The unit tests use an in-memory wire; the live cluster exposed problems they could not:

* **Peer replies were silently dropped.** A message sent from inside a gRPC handler inherits that call's `Context`,
  which is cancelled when the handler returns, so every RA REPLY, forwarded token and CMH probe was cancelled.
  Peer sends now run in `Context.ROOT`, and failed sends are counted and logged.
* **Split brain after a restart.** Peers' channels to a restarted replica were still in reconnect backoff, so a lower
  id won while the higher one also believed it led. Fixed with backoff reset + retry and the leader hand-over check.
* **Policy reset to v1** when coordinators restarted. Fixed with the policy file and highest-version adoption.
* **Edges stuck on a demoted leader's Control stream** missed policy changes. Fixed on both sides.
