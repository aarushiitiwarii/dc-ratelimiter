# RateGuard-DC: edge-based distributed rate limiting, built on distributed-computing algorithms

The same product as the cloud version of RateGuard (same limits, same API keys, same operations console), rebuilt
**without Docker or Kubernetes**. Every process is a plain JVM on your machine, and the cluster is held together by the
algorithms from the Distributed Computing lab: gRPC (unary and streaming), Java RMI, Bully leader election,
Ricart–Agrawala and Suzuki–Kasami mutual exclusion, Chandy–Misra–Haas deadlock detection, load-balancing strategies and
consistency models. See [EXPERIMENTS.md](EXPERIMENTS.md) for the mapping to each lab experiment and
[DESIGN.md](DESIGN.md) for the architecture and measured results.

## Run it

Needs **Java 21** and **Maven** (the first build downloads gRPC, Protobuf and `protoc` from Maven Central).

```bat
run.bat
```

The first run builds `target\rateguard-dc.jar`. The launcher then starts the cluster and opens http://localhost:9000.
Press **Ctrl+C** in that window to stop everything (`stop.bat` kills any leftover RateGuard JVMs). Logs are in `logs\`;
each coordinator keeps the current policy in `data\coord-N.policy` (delete `data\` to go back to the default limits).

| Process | Port | Role |
|---|---|---|
| launcher | RMI `1099` | starts and supervises every process; `ClusterControl` remote object (kill / start / scale / slow) |
| coord-1..3 | gRPC `9201-9203` | token-bucket state store; Bully election; leader replicates to followers |
| edge-1..N | HTTP `8101+`, gRPC `9101+` | gateway: auth → tenant / key / IP limits → proxy; mutual exclusion and deadlock detection among edges |
| lb | HTTP `8080` | load balancer in front of the edges (round robin / least connections / least response time) |
| upstream | HTTP `8201-8203` | the protected API |
| console | HTTP `9000` | operations dashboard, load generator, verification checks |

Send a request yourself:

```bat
java -jar target\rateguard-dc.jar mint acme key0
curl -H "X-API-Key: <key from above>" http://localhost:8080/
```

## Using the console

| Area | What it does |
|---|---|
| Header | Edge-node health, current coordinator leader, **Test traffic**, **Run all checks** |
| KPI tiles | Requests / allowed / throttled / blocked per second, coordinator calls per request, added latency p99 |
| Traffic + Edge nodes + Coordinators | Live chart; each edge's state and RPC rate; each coordinator's role (leader / follower / down) and term |
| Rate-limit policies | Tenant, API key and IP limits (Edit applies to every edge via the leader), plus selectors for the **consistency model**, the **no-leader mode** (local / Ricart–Agrawala / Suzuki–Kasami / pool locks), **lock ordering** and the **load-balancing strategy** |
| Verification | One row per problem-statement requirement and lab experiment, each with a Run button; **Run all checks** takes about 6 minutes |
| Test traffic drawer | Manual traffic; kill the leader, kill or start all coordinators, scale edges to 3 / 5 / 7, kill / start / slow one edge |

## Tests

```bat
mvn test
```

Unit tests cover the token-bucket math, the Bully election (highest live id wins, re-election after a crash, take-over on
return), Ricart–Agrawala (exactly 2(N−1) messages per critical section), Suzuki–Kasami (N messages, or 0 when the token
is held), mutual exclusion under concurrency (never hands out more than the pool) and Chandy–Misra–Haas (crossing lock
orders deadlock and are detected and resolved; ordered locking never deadlocks).

## Layout

```
src/main/proto/rateguard.proto      gRPC services: TokenStore, Replication, Peer
src/main/java/com/rateguard/dc/
  coord/    BucketStore (token bucket + sliding window), BullyElection, CoordNode
  edge/     Limiter, LeaderClient, PeerNet, RicartAgrawala, SuzukiKasami, PoolLocks (CMH), EdgeNode
  lb/       LoadBalancer (3 strategies)
  launcher/ Launcher, ClusterControl (RMI)
  console/  ConsoleServer, ClusterView, LoadEngine, Checks
  auth/     Authenticator (HMAC API keys, HS256 JWT)    cache/ ShardedLru
src/main/resources/web/index.html   the dashboard
```
