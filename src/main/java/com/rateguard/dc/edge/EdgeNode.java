package com.rateguard.dc.edge;

import com.rateguard.dc.auth.Authenticator;
import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Policies;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.*;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Edge gateway node. HTTP pipeline per request:
 *   1. authenticate (local HMAC / JWT; bad credential -> 401, no coordinator call),
 *   2. three-level rate limit (tenant, API key, client IP) -> 429 + X-RateLimit-Level,
 *   3. proxy to the upstream API.
 * Also serves the Peer gRPC service (edge-to-edge algorithms) and a WatchStats stream.
 */
public final class EdgeNode {
    private final int id;
    private final String name;
    private final Authenticator auth;
    private final boolean trustXff;
    private final AtomicReference<Policy> policy = new AtomicReference<>(Policies.defaults());
    private final PeerNet net;
    private final LeaderClient leader;
    private final Limiter limiter;
    private final RicartAgrawala ra;
    private final SuzukiKasami sk;
    private final PoolLocks pool;
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(1)).executor(Executors.newVirtualThreadPerTaskExecutor()).build();
    private final AtomicInteger upstreamRr = new AtomicInteger();
    private final AtomicLong unauthorized = new AtomicLong();
    private volatile long slowMs;

    public EdgeNode(int id, int initialEdges, String secret, boolean trustXff) {
        this.id = id;
        this.name = Ports.edgeName(id);
        this.auth = new Authenticator(secret, 50_000, 300_000);
        this.trustXff = trustXff;
        this.net = new PeerNet(name);
        List<String> init = new ArrayList<>();
        for (int i = 1; i <= initialEdges; i++) init.add(Ports.edgeName(i));
        net.setMembers(init);
        this.leader = new LeaderClient(name, Ports.HOST + ":" + Ports.edgePeer(id), this::onControl,
                () -> limiter().allowed.get(), () -> limiter().denied.get());
        this.limiter = new Limiter(leader, policy::get, net::size, 50, 500, 100_000);
        this.ra = new RicartAgrawala(net);
        this.sk = new SuzukiKasami(net);
        this.pool = new PoolLocks(net, policy::get);
        limiter.registerOutage("ra", ra);
        limiter.registerOutage("token", sk);
        limiter.registerPool(pool);
        net.onMessage(this::onPeer);
    }

    private Limiter limiter() { return limiter; }

    /** Policy + membership pushed by the leader over the bidirectional Control stream. */
    private void onControl(ControlMsg m) {
        Policy old = policy.get();
        if (m.getPolicy().getVersion() != old.getVersion()) {
            policy.set(m.getPolicy());
            Log.info("policy v%d applied (consistency=%s outage=%s)", m.getPolicy().getVersion(), m.getPolicy().getConsistency(), m.getPolicy().getOutageMode());
        }
        if (m.getMembersCount() > 0) {
            List<String> names = new ArrayList<>();
            for (String s : m.getMembersList()) names.add(s.contains("@") ? s.substring(0, s.indexOf('@')) : s);
            net.setMembers(names);
        }
    }

    private void onPeer(PeerMsg m) {
        switch (m.getBodyCase()) {
            case RA_REQUEST, RA_REPLY -> ra.onMessage(m);
            case SK_REQUEST, SK_TOKEN -> sk.onMessage(m);
            default -> pool.onMessage(m);
        }
    }

    public void start() throws Exception {
        Server grpc = Grpc.newServerBuilderForPort(Ports.edgePeer(id), InsecureServerCredentials.create())
                .addService(new PeerImpl()).executor(Executors.newVirtualThreadPerTaskExecutor()).build().start();
        HttpServer srv = HttpServer.create(new InetSocketAddress(Ports.HOST, Ports.edgeHttp(id)), 2048);
        srv.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        srv.createContext("/", this::handle);
        srv.start();
        leader.start();
        Thread d = new Thread(() -> {
            while (true) {
                try { Thread.sleep(250); limiter.sweep(); } catch (InterruptedException e) { return; }
                catch (Exception e) { Log.info("sweep error: %s", e); }
            }
        }, "sync-daemon");
        d.setDaemon(true);
        d.start();
        Log.info("%s http :%d, peer gRPC :%d", name, Ports.edgeHttp(id), Ports.edgePeer(id));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { grpc.shutdownNow(); srv.stop(0); }));
        grpc.awaitTermination();
    }

    // ------------------------------------------------------------ HTTP
    private void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            switch (path) {
                case "/healthz" -> { reply(ex, 200, "ok\n"); return; }
                case "/admin/slow" -> {
                    String q = ex.getRequestURI().getQuery();
                    slowMs = q != null && q.startsWith("ms=") ? Long.parseLong(q.substring(3)) : 0;
                    reply(ex, 200, "slow " + slowMs + " ms\n");
                    return;
                }
                default -> {}
            }
            if (slowMs > 0) Thread.sleep(slowMs);                          // injected load for the LB experiment
            ex.getResponseHeaders().set("X-Served-By", name);
            long now = System.currentTimeMillis();
            Authenticator.Claims c = auth.authenticate(ex.getRequestHeaders().getFirst("Authorization"),
                    ex.getRequestHeaders().getFirst("X-API-Key"), now);
            if (c == null) { unauthorized.incrementAndGet(); reply(ex, 401, "unauthorized\n"); return; }
            String ip = clientIp(ex);
            int d = limiter.allow(new String[]{"t:" + c.tenant(), "k:" + c.tenant() + ":" + c.keyId(), "i:" + ip});
            if (d >= 0) {
                ex.getResponseHeaders().set("Retry-After", "1");
                ex.getResponseHeaders().set("X-RateLimit-Level", Limiter.LEVEL[d]);
                reply(ex, 429, "too many requests\n");
                return;
            }
            ex.getRequestHeaders().set("X-Tenant-ID", c.tenant());
            int u = 1 + Math.floorMod(upstreamRr.getAndIncrement(), 3);
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://" + Ports.HOST + ":" + Ports.upstream(u) + "/"))
                    .timeout(Duration.ofSeconds(2)).header("X-Tenant-ID", c.tenant()).GET().build(), HttpResponse.BodyHandlers.ofString());
            reply(ex, r.statusCode(), r.body());
        } catch (Exception e) {
            try { reply(ex, 502, "bad gateway: " + e.getMessage() + "\n"); } catch (Exception ignored) {}
        }
    }

    private String clientIp(HttpExchange ex) {
        if (trustXff) {
            String xf = ex.getRequestHeaders().getFirst("X-Forwarded-For");
            if (xf != null && !xf.isEmpty()) { int i = xf.indexOf(','); return (i >= 0 ? xf.substring(0, i) : xf).trim(); }
        }
        return ex.getRemoteAddress().getAddress().getHostAddress();
    }

    static void reply(HttpExchange ex, int code, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    // ------------------------------------------------------------ stats
    EdgeStats stats() {
        Policy p = policy.get();
        return EdgeStats.newBuilder().setNode(name).setMode(limiter.mode())
                .setAllowed(limiter.allowed.get()).setDenied(limiter.denied.get()).setUnauthorized(unauthorized.get())
                .setDeniedTenant(limiter.deniedBy[0].get()).setDeniedKey(limiter.deniedBy[1].get()).setDeniedIp(limiter.deniedBy[2].get())
                .setRpcCalls(leader.rpcCalls.get()).setFallbackEvents(limiter.fallbackEvents.get()).setReconciled(limiter.reconciled.get())
                .setBuckets(limiter.buckets()).setTokensReturned(limiter.tokensReturned.get())
                .setMutexMsgs(ra.messages.get() + sk.messages.get() + pool.messages.get())
                .setCsEntries(ra.csEntries.get() + sk.csEntries.get() + pool.csEntries.get())
                .setProbes(pool.probes.get()).setDeadlocks(pool.deadlocks.get()).setAborts(pool.aborts.get())
                .setLeader(leader.leaderId() == 0 ? "" : Ports.coordName(leader.leaderId()))
                .setConsistency(p.getConsistency()).setOutageMode(p.getOutageMode())
                .setSlowMs(slowMs).setPeers(net.size()).build();
    }

    final class PeerImpl extends PeerGrpc.PeerImplBase {
        @Override public void deliver(PeerMsg m, StreamObserver<Empty> out) {
            out.onNext(Empty.getDefaultInstance());
            out.onCompleted();
            net.deliver(m);
        }

        /** Exp 2 server streaming: one stats message per second until the console disconnects. */
        @Override public void watchStats(Empty r, StreamObserver<EdgeStats> out) {
            ServerCallStreamObserver<EdgeStats> so = (ServerCallStreamObserver<EdgeStats>) out;
            try {
                while (!so.isCancelled()) {
                    so.onNext(stats());
                    Thread.sleep(1000);
                }
            } catch (Exception ignored) {}
        }
    }

    public static void main(int id, int edges, String secret, boolean trustXff) throws Exception {
        Log.setName(Ports.edgeName(id));
        new EdgeNode(id, edges, secret, trustXff).start();
    }
}
