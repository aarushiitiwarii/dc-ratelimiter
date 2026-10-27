package com.rateguard.dc.console;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.launcher.ClusterControl;
import com.rateguard.dc.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.rmi.registry.LocateRegistry;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/** The console's live picture of the cluster, refreshed once a second. */
final class ClusterView {

    static final class EdgeView {
        final String name;
        volatile EdgeStats last, prev; volatile long seen, prevAt;
        volatile Map<String, Double> rates = Map.of();
        volatile boolean up; volatile String mode = "";
        volatile ClientCall<?, ?> call;
        EdgeView(String n) { name = n; }
    }

    static final class CoordView {
        final int id; volatile boolean up; volatile CoordStatus status; volatile CoordStatus prev;
        CoordView(int id) { this.id = id; }
    }

    record Event(long t, String kind, String msg) {}

    final Map<String, EdgeView> edges = new ConcurrentHashMap<>();
    final Map<Integer, CoordView> coords = new ConcurrentHashMap<>();
    private final Map<Integer, ManagedChannel> coordCh = new ConcurrentHashMap<>();
    private final Map<String, ManagedChannel> edgeCh = new ConcurrentHashMap<>();
    volatile int leader;
    volatile Policy policy = com.rateguard.dc.common.Policies.defaults();
    volatile Map<String, Object> lb = Map.of();
    volatile List<ClusterControl.ProcInfo> processes = List.of();
    volatile boolean rmiOk;
    final List<Map<String, Object>> history = Collections.synchronizedList(new ArrayList<>());
    final List<Event> events = Collections.synchronizedList(new ArrayList<>());
    private volatile ClusterControl cc;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofMillis(500)).build();
    private final Gson gson = new Gson();
    private volatile LoadEngine.LoadRun currentRun;
    private double lastLeaderOps; private long lastLeaderOpsAt;

    ClusterView() { for (int i = 1; i <= Ports.COORDS; i++) coords.put(i, new CoordView(i)); }

    void setCurrentRun(LoadEngine.LoadRun r) { currentRun = r; }

    void event(String kind, String fmt, Object... a) {
        events.add(new Event(System.currentTimeMillis(), kind, String.format(fmt, a)));
        synchronized (events) { while (events.size() > 200) events.remove(0); }
    }

    // ------------------------------------------------------------ RMI control plane
    ClusterControl rmi() {
        ClusterControl c = cc;
        if (c != null) return c;
        try {
            c = (ClusterControl) LocateRegistry.getRegistry(Ports.HOST, Ports.RMI_REGISTRY).lookup(ClusterControl.NAME);
            cc = c;
            return c;
        } catch (Exception e) { return null; }
    }

    boolean kill(String name) { try { ClusterControl c = rmi(); return c != null && c.kill(name); } catch (Exception e) { cc = null; return false; } }
    boolean startProc(String name) { try { ClusterControl c = rmi(); return c != null && c.start(name); } catch (Exception e) { cc = null; return false; } }
    int scale(int n) { try { ClusterControl c = rmi(); return c == null ? -1 : c.scaleEdges(n); } catch (Exception e) { cc = null; return -1; } }
    boolean slow(String edge, long ms) { try { ClusterControl c = rmi(); return c != null && c.slow(edge, ms); } catch (Exception e) { cc = null; return false; } }

    // ------------------------------------------------------------ coordinators
    private ManagedChannel coordChannel(int id) {
        return coordCh.computeIfAbsent(id, i -> Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.coord(i), InsecureChannelCredentials.create()).build());
    }

    /** Sends a policy change to the cluster (any coordinator forwards it to the leader). */
    Policy setPolicy(Policy p) {
        List<Integer> order = new ArrayList<>();
        if (leader != 0) order.add(leader);
        for (int i = 1; i <= Ports.COORDS; i++) if (!order.contains(i)) order.add(i);
        for (int id : order) {
            try {
                Policy r = TokenStoreGrpc.newBlockingStub(coordChannel(id)).withDeadlineAfter(1500, TimeUnit.MILLISECONDS).setPolicy(p);
                policy = r;
                return r;
            } catch (StatusRuntimeException ignored) {}
        }
        return null;
    }

    // ------------------------------------------------------------ edge stats (server streaming)
    private void ensureSubscription(EdgeView v) {
        if (v.call != null) return;
        int id = Integer.parseInt(v.name.substring(v.name.lastIndexOf('-') + 1));
        ManagedChannel ch = edgeCh.computeIfAbsent(v.name, n -> Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.edgePeer(id), InsecureChannelCredentials.create()).build());
        ClientCall<Empty, EdgeStats> call = ch.newCall(PeerGrpc.getWatchStatsMethod(), CallOptions.DEFAULT);
        v.call = call;
        io.grpc.stub.ClientCalls.asyncServerStreamingCall(call, Empty.getDefaultInstance(), new StreamObserver<>() {
            public void onNext(EdgeStats s) { v.last = s; v.seen = System.currentTimeMillis(); }
            public void onError(Throwable t) { v.call = null; }
            public void onCompleted() { v.call = null; }
        });
    }

    // ------------------------------------------------------------ poll loop
    void start() {
        Thread t = new Thread(() -> {
            while (true) {
                long t0 = System.currentTimeMillis();
                try { poll(); } catch (Exception e) { /* keep polling */ }
                try { Thread.sleep(Math.max(50, 1000 - (System.currentTimeMillis() - t0))); } catch (InterruptedException e) { return; }
            }
        }, "cluster-poll");
        t.setDaemon(true);
        t.start();
    }

    private void poll() {
        long now = System.currentTimeMillis();
        // processes + edge membership from the RMI naming service
        List<String> edgeNames = new ArrayList<>();
        ClusterControl c = rmi();
        boolean wasOk = rmiOk;
        try {
            if (c != null) { processes = c.processes(); edgeNames = c.edges(); rmiOk = true; } else rmiOk = false;
        } catch (Exception e) { cc = null; rmiOk = false; }
        if (wasOk && !rmiOk) event("warn", "launcher (RMI) unreachable");
        Set<String> known = new HashSet<>(edgeNames);
        for (ClusterControl.ProcInfo p : processes) if (p.role().equals("edge")) known.add(p.name());
        for (String n : known) edges.computeIfAbsent(n, EdgeView::new);
        edges.keySet().removeIf(n -> !known.contains(n) && now - edges.get(n).seen > 5000);
        for (EdgeView v : edges.values()) ensureSubscription(v);

        // coordinators
        int newLeader = 0;
        for (CoordView v : coords.values()) {
            boolean was = v.up;
            try {
                CoordStatus s = TokenStoreGrpc.newBlockingStub(coordChannel(v.id)).withDeadlineAfter(400, TimeUnit.MILLISECONDS).status(Empty.getDefaultInstance());
                v.prev = v.status; v.status = s; v.up = true;
                if (s.getLeader()) { newLeader = v.id; policy = s.getPolicy(); }
            } catch (StatusRuntimeException e) { v.up = false; }
            if (was && !v.up) event("warn", "coord-%d stopped answering", v.id);
            if (!was && v.up && v.status != null && history.size() > 2) event("ok", "coord-%d is up", v.id);
        }
        if (newLeader != leader) {
            if (newLeader == 0) event("warn", "no coordinator leader: edges switch to outage mode (%s)", policy.getOutageMode());
            else event("ok", "coord-%d elected leader (Bully, term %d)", newLeader, coords.get(newLeader).status.getTerm());
            leader = newLeader;
        }

        // load balancer
        try {
            HttpResponse<String> r = http.send(HttpRequest.newBuilder(URI.create("http://" + Ports.HOST + ":" + Ports.LB + "/lb/stats")).timeout(Duration.ofMillis(500)).build(),
                    HttpResponse.BodyHandlers.ofString());
            lb = gson.fromJson(r.body(), new TypeToken<Map<String, Object>>() {}.getType());
        } catch (Exception e) { lb = Map.of("strategy", "?", "backends", List.of()); }

        // edge rates + mode changes
        Map<String, Object> s = new LinkedHashMap<>();
        double allowed = 0, denied = 0, dt = 0, dk = 0, di = 0, unauth = 0, rpc = 0, mutex = 0;
        int up = 0, fb = 0;
        Map<String, Double> perPod = new LinkedHashMap<>();
        for (EdgeView v : edges.values()) {
            EdgeStats st = v.last;
            boolean wasUp = v.up;
            v.up = st != null && now - v.seen < 2500;
            if (wasUp && !v.up) event("warn", "%s stopped answering", v.name);
            if (!wasUp && v.up && history.size() > 2) event("ok", "%s is answering", v.name);
            if (!v.up || st == null) { v.rates = Map.of(); continue; }
            if (!v.mode.isEmpty() && !v.mode.equals(st.getMode()))
                event(st.getMode().equals("fallback") ? "warn" : "ok", "%s -> %s (%s)", v.name, st.getMode().toUpperCase(),
                        st.getMode().equals("fallback") ? "no leader, outage mode " + st.getOutageMode() : "leader reachable, usage reconciled");
            v.mode = st.getMode();
            EdgeStats p = v.prev;
            Map<String, Double> rates = new HashMap<>();
            if (p != null && v.seen > v.prevAt) {
                double sec = (v.seen - v.prevAt) / 1000.0;
                rates.put("allowed", Math.max(0, st.getAllowed() - p.getAllowed()) / sec);
                rates.put("denied", Math.max(0, st.getDenied() - p.getDenied()) / sec);
                rates.put("denied_tenant", Math.max(0, st.getDeniedTenant() - p.getDeniedTenant()) / sec);
                rates.put("denied_key", Math.max(0, st.getDeniedKey() - p.getDeniedKey()) / sec);
                rates.put("denied_ip", Math.max(0, st.getDeniedIp() - p.getDeniedIp()) / sec);
                rates.put("unauthorized", Math.max(0, st.getUnauthorized() - p.getUnauthorized()) / sec);
                rates.put("rpc_calls", Math.max(0, st.getRpcCalls() - p.getRpcCalls()) / sec);
                rates.put("mutex_msgs", Math.max(0, st.getMutexMsgs() - p.getMutexMsgs()) / sec);
                v.rates = rates;
            }
            if (p == null || v.seen > v.prevAt) { v.prev = st; v.prevAt = v.seen; }
            Map<String, Double> r = v.rates;
            up++;
            if (st.getMode().equals("fallback")) fb++;
            allowed += r.getOrDefault("allowed", 0.0); denied += r.getOrDefault("denied", 0.0);
            dt += r.getOrDefault("denied_tenant", 0.0); dk += r.getOrDefault("denied_key", 0.0); di += r.getOrDefault("denied_ip", 0.0);
            unauth += r.getOrDefault("unauthorized", 0.0); rpc += r.getOrDefault("rpc_calls", 0.0); mutex += r.getOrDefault("mutex_msgs", 0.0);
            perPod.put(v.name, r.getOrDefault("allowed", 0.0));
        }
        // coordinator ops/s (leader's leases + gives + acquires)
        double ops = 0;
        if (leader != 0) {
            CoordStatus ls = coords.get(leader).status;
            double total = ls.getLeases() + ls.getGives() + ls.getAcquires() + ls.getUsageReports();
            if (lastLeaderOpsAt > 0 && total >= lastLeaderOps) ops = (total - lastLeaderOps) / Math.max(0.2, (now - lastLeaderOpsAt) / 1000.0);
            lastLeaderOps = total; lastLeaderOpsAt = now;
        } else lastLeaderOpsAt = 0;

        s.put("t", now);
        s.put("allowed", allowed); s.put("denied", denied);
        s.put("den_tenant", dt); s.put("den_key", dk); s.put("den_ip", di);
        s.put("unauth", unauth); s.put("redis_calls", rpc); s.put("redis_ops", ops); s.put("mutex_msgs", mutex);
        s.put("fallback_pods", fb); s.put("pods_up", up); s.put("leader", leader); s.put("per_pod", perPod);
        LoadEngine.LoadRun cur = currentRun;
        if (cur != null && cur.running()) {
            Hist h = cur.win.getAndSet(new Hist());
            long n = h.count();
            s.put("load_rps", (double) n);
            if (n > 0) { double[] q = h.pcts(.5, .99); s.put("load_p50", q[0]); s.put("load_p99", q[1]); }
        }
        history.add(s);
        synchronized (history) { while (history.size() > 300) history.remove(0); }
    }

    // ------------------------------------------------------------ sums over edges (for checks)
    long sum(java.util.function.ToLongFunction<EdgeStats> f) {
        long n = 0;
        for (EdgeView v : edges.values()) if (v.last != null && v.up) n += f.applyAsLong(v.last);
        return n;
    }

    /** Fresh per-edge counters read directly over the stream snapshots (waits for the next tick). */
    void waitTick() { try { Thread.sleep(1100); } catch (InterruptedException ignored) {} }

    /** True when every answering edge's latest stats satisfy the condition. */
    boolean allEdges(java.util.function.Predicate<EdgeStats> f) {
        int n = 0;
        for (EdgeView v : edges.values()) if (v.up && v.last != null) { if (!f.test(v.last)) return false; n++; }
        return n > 0;
    }

    int upEdges() { int n = 0; for (EdgeView v : edges.values()) if (v.up) n++; return n; }

    int liveCoords() { int n = 0; for (CoordView v : coords.values()) if (v.up) n++; return n; }
}
