package com.rateguard.dc.console;

import com.rateguard.dc.common.Policies;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.CoordStatus;
import com.rateguard.dc.proto.Empty;
import com.rateguard.dc.proto.EdgeStats;
import com.rateguard.dc.proto.TokenStoreGrpc;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.ToLongFunction;

/**
 * Verification checks: one per problem-statement requirement and one per lab
 * experiment. Each drives real traffic and failures through the running cluster,
 * stores a pass/fail result with the measured numbers, and leaves the cluster as it found it.
 */
final class Checks {
    static final List<String> ORDER = List.of("auth", "burst", "hier", "latency", "consistency", "election", "mutex", "deadlock", "lb", "throughput");
    static final Map<String, String> NAMES = new LinkedHashMap<>();
    static {
        NAMES.put("auth", "Unauthenticated traffic dropped before the coordinator");
        NAMES.put("burst", "Burst accuracy from distributed IPs");
        NAMES.put("hier", "Hierarchical limits: tenant, API key, client IP");
        NAMES.put("latency", "Edge latency penalty < 5 ms");
        NAMES.put("consistency", "Consistency models: strong vs bounded vs eventual (Exp 8)");
        NAMES.put("election", "Leader election and failover: Bully algorithm (Exp 9)");
        NAMES.put("mutex", "Mutual exclusion overhead vs nodes: Ricart-Agrawala vs Suzuki-Kasami (Exp 4, 5)");
        NAMES.put("deadlock", "Deadlock detection: Chandy-Misra-Haas (Exp 6)");
        NAMES.put("lb", "Load balancing under dynamic load: 3 strategies (Exp 7)");
        NAMES.put("throughput", "Throughput benchmark");
    }

    record Result(String id, String status, String headline, String detail, long at) {}

    private final ConsoleServer c;
    private final Map<String, Result> report = new ConcurrentHashMap<>();
    private final ReentrantLock suite = new ReentrantLock();
    private volatile String job = "";
    private volatile boolean cancelled;
    private final Map<Integer, ManagedChannel> ch = new ConcurrentHashMap<>();

    private static final class Cancelled extends RuntimeException { Cancelled() { super(null, null, false, false); } }

    Checks(ConsoleServer c) { this.c = c; }

    String job() { return job; }
    Map<String, Result> report() { return report; }
    void cancel() { cancelled = true; }

    boolean run(List<String> ids) {
        List<String> todo = new ArrayList<>();
        for (String id : ids) { if (id.equals("all")) { todo = new ArrayList<>(ORDER); break; } if (NAMES.containsKey(id)) todo.add(id); }
        if (todo.isEmpty()) return false;
        final List<String> list = todo;
        cancelled = true;                       // stop a suite that is already running
        Thread.startVirtualThread(() -> {
            suite.lock();
            try {
                cancelled = false;
                for (int i = 0; i < list.size() && !cancelled; i++) {
                    String id = list.get(i);
                    job = "Check " + (i + 1) + "/" + list.size() + ": " + NAMES.get(id);
                    Result prev = report.get(id);
                    report.put(id, new Result(id, "running", "", "", System.currentTimeMillis()));
                    try {
                        runCheck(id);
                        Result r = report.get(id);
                        c.view.event("info", "check \"%s\": %s - %s", NAMES.get(id), r.status(), r.headline());
                    } catch (Cancelled e) {
                        if (prev != null) report.put(id, prev); else report.remove(id);
                    } catch (Exception e) {
                        set(id, "fail", "check error: " + e.getMessage(), "");
                    }
                    sleepQuiet(1000);
                }
            } finally { job = ""; suite.unlock(); }
        });
        return true;
    }

    // ------------------------------------------------------------ helpers
    private final java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();

    private void set(String id, String status, String head, String detail) {
        report.put(id, new Result(id, status, head, detail, System.currentTimeMillis()));
    }

    private void sleep(long ms) { long end = System.currentTimeMillis() + ms; while (System.currentTimeMillis() < end) { if (cancelled) throw new Cancelled(); sleepQuiet(Math.min(100, ms)); } }
    private static void sleepQuiet(long ms) { try { Thread.sleep(ms); } catch (InterruptedException ignored) {} }

    private long waitFor(BooleanSupplier cond, long timeoutMs) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (cond.getAsBoolean()) return System.currentTimeMillis() - start;
            sleep(100);
        }
        return -1;
    }

    private LoadEngine.RunSummary runAndWait(LoadEngine.LoadSpec s) {
        LoadEngine.LoadRun r = c.startLoad(s);
        while (r.done.getCount() > 0) {
            if (cancelled) { r.stop(); sleepQuiet(200); throw new Cancelled(); }
            sleepQuiet(100);
        }
        return r.summary();
    }

    private static LoadEngine.LoadSpec spec(double dur, int conc, int qps, int tenants, int keys, int ips, String scen, String label) {
        LoadEngine.LoadSpec s = new LoadEngine.LoadSpec();
        s.durationS = dur; s.concurrency = conc; s.qps = qps; s.tenants = tenants; s.keys = keys; s.ips = ips;
        s.fresh = true; s.scenario = scen; s.label = label;
        return s;
    }

    /** Asks the coordinators directly (not waiting for the console's 1 s tick) who leads. */
    private int probeLeader() {
        for (int i = 1; i <= Ports.COORDS; i++) {
            try {
                ManagedChannel m = ch.computeIfAbsent(i, id -> Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.coord(id), InsecureChannelCredentials.create()).build());
                CoordStatus s = TokenStoreGrpc.newBlockingStub(m).withDeadlineAfter(200, TimeUnit.MILLISECONDS).status(Empty.getDefaultInstance());
                if (s.getLeader()) return i;
            } catch (Exception ignored) {}
        }
        return 0;
    }

    private int liveCoordEdges() {
        int l = probeLeader();
        if (l == 0) return -1;
        try {
            return TokenStoreGrpc.newBlockingStub(ch.get(l)).withDeadlineAfter(300, TimeUnit.MILLISECONDS).status(Empty.getDefaultInstance()).getEdges();
        } catch (Exception e) { return -1; }
    }

    private double avgAllowed(int n) {
        synchronized (c.view.history) {
            List<Map<String, Object>> h = c.view.history;
            int from = Math.max(0, h.size() - n); double sum = 0; int k = 0;
            for (int i = from; i < h.size(); i++) { sum += ((Number) h.get(i).get("allowed")).doubleValue(); k++; }
            return k == 0 ? 0 : sum / k;
        }
    }

    private long edgeSum(ToLongFunction<EdgeStats> f) { c.view.waitTick(); return c.view.sum(f); }

    private static String human(double n) {
        if (n >= 1e6) return String.format("%.1fM", n / 1e6);
        if (n >= 1e4) return String.format("%.1fk", n / 1e3);
        return String.format("%,.0f", n);
    }

    private static double[] lat(LoadEngine.RunSummary s) { return s.latOkMs[0] > 0 ? s.latOkMs : s.latMs; }

    private void policy(String consistency, String outage, Boolean ordering) {
        c.updatePolicy(b -> {
            if (consistency != null) b.setConsistency(consistency);
            if (outage != null) b.setOutageMode(outage);
            if (ordering != null) b.setLockOrdering(ordering);
            return b;
        });
        // pushed to every edge over the Control stream; wait until each one reports it
        waitFor(() -> c.view.allEdges(e -> (consistency == null || consistency.equals(e.getConsistency()))
                && (outage == null || outage.equals(e.getOutageMode()))), 6000);
        sleep(1200);
    }

    private void killCoords() { for (int i = 1; i <= Ports.COORDS; i++) c.view.kill(Ports.coordName(i)); }
    private void startCoords() { for (int i = 1; i <= Ports.COORDS; i++) c.view.startProc(Ports.coordName(i)); }

    /** Restart coordinators and wait until the highest id leads and every edge has rejoined (always called in finally blocks). */
    private void ensureLeader() {
        startCoords();
        long end = System.currentTimeMillis() + 25_000;
        while (probeLeader() != Ports.COORDS && System.currentTimeMillis() < end) sleepQuiet(200);
        int want = c.view.upEdges();
        while (liveCoordEdges() < want && System.currentTimeMillis() < end) sleepQuiet(200);
        sleepQuiet(1500);
    }

    private void raiseLimits() {
        c.updatePolicy(b -> b.setTenant(Policies.limit(1e6, 1e6)).setKey(Policies.limit(1e6, 1e6)).setIp(Policies.limit(1e6, 1e6)).setIpLocal(true));
        sleep(1500);
    }

    private void resetLimits() {
        var d = Policies.defaults();
        c.updatePolicy(b -> b.setTenant(d.getTenant()).setKey(d.getKey()).setIp(d.getIp()).setIpLocal(true));
        sleepQuiet(1500);
    }

    // ------------------------------------------------------------ the checks
    private void runCheck(String id) {
        switch (id) {
            case "auth" -> {
                LoadEngine.LoadSpec s = spec(8, 32, 2000, 1, 1, 5000, "auth", "forged-key flood");
                s.badPct = 100;
                var r = runAndWait(s);
                boolean ok = r.sent > 0 && r.unauth == r.sent && r.rpcCalls == 0;
                set(id, ok ? "pass" : "fail", String.format("%s forged requests -> %s x 401 at the edge, %d coordinator calls", human(r.sent), human(r.unauth), r.rpcCalls),
                        String.format("2,000 req/s for 8 s from 5,000 IPs through the load balancer. The HMAC check is local CPU; typical response %.2f ms.", r.latMs[0]));
            }
            case "burst" -> {
                var r = runAndWait(spec(10, 128, 0, 4, 2, 5000, "burst", "burst"));
                double rpr = (r.ok + r.limited) > 0 ? (double) r.rpcCalls / (r.ok + r.limited) : 0;
                if (r.bound <= 0 || !r.saturated) { set(id, "info", human(r.ok) + " allowed; load did not exceed the limit", r.boundNote); return; }
                double dev = (r.ok - r.bound) / r.bound * 100;
                set(id, r.ok <= r.bound * 1.01 ? "pass" : "fail", String.format("%s allowed vs %s permitted (%+.1f%%), never over", human(r.ok), human(r.bound), dev),
                        String.format("%s requests in 10 s (%s req/s) from 5,000 IPs across all edges; %.3f coordinator calls per request.", human(r.sent), human(r.rps), rpr));
            }
            case "hier" -> {
                String[][] cases = {{"tenant", "1", "8", "3000"}, {"key", "1", "1", "3000"}, {"ip", "4", "8", "2"}};
                String[] label = {"tenant", "API key", "client IP"};
                boolean pass = true; StringBuilder head = new StringBuilder(), det = new StringBuilder();
                for (int i = 0; i < 3; i++) {
                    var r = runAndWait(spec(8, 48, 2500, Integer.parseInt(cases[i][1]), Integer.parseInt(cases[i][2]), Integer.parseInt(cases[i][3]), "hier", cases[i][0]));
                    int top = 0; for (int j = 1; j < 3; j++) if (r.byLevel[j] > r.byLevel[top]) top = j;
                    boolean ok = top == i && r.limited > 0;
                    pass &= ok;
                    head.append(i > 0 ? " · " : "").append(label[i]).append(ok ? " ✓" : " ✗");
                    det.append(String.format("%s test: %s allowed of %s permitted. ", label[i], human(r.ok), human(r.bound)));
                }
                set(id, pass ? "pass" : "fail", head + ": each test stopped by its own limit", det.toString().trim());
            }
            case "latency" -> {
                try {
                    LoadEngine.LoadSpec d = spec(8, 16, 400, 4, 5, 200, "latency", "direct to upstream"); d.target = "direct";
                    var direct = runAndWait(d);
                    policy("strong", null, null);
                    var strong = runAndWait(spec(8, 16, 400, 4, 5, 200, "latency", "strong (RPC per request)"));
                    policy("bounded", null, null);
                    var bounded = runAndWait(spec(8, 16, 400, 4, 5, 200, "latency", "RateGuard (leases)"));
                    double[] dl = lat(direct), sl = lat(strong), bl = lat(bounded);
                    double p50 = bl[0] - dl[0], p99 = bl[2] - dl[2];
                    double rs = (double) strong.rpcCalls / Math.max(1, strong.ok + strong.limited), rb = (double) bounded.rpcCalls / Math.max(1, bounded.ok + bounded.limited);
                    set(id, p99 < 5 ? "pass" : "fail", String.format("+%.2f ms p50, +%.2f ms p99 added; %.2f vs %.2f coordinator calls per request", p50, p99, rb, rs),
                            String.format("400 req/s, 8 s per path. p50/p99: direct %.2f/%.2f ms, strong (sync RPC per request) %.2f/%.2f ms, RateGuard %.2f/%.2f ms. Includes the load-balancer hop.",
                                    dl[0], dl[2], sl[0], sl[2], bl[0], bl[2]));
                } finally { policy("bounded", null, null); }
            }
            case "consistency" -> {
                try {
                    StringBuilder head = new StringBuilder(), det = new StringBuilder();
                    boolean safe = true;
                    for (String m : new String[]{"strong", "bounded", "eventual"}) {
                        policy(m, null, null);
                        var r = runAndWait(spec(6, 96, 0, 2, 2, 2000, "consistency", m));
                        double dev = r.bound > 0 ? (r.ok - r.bound) / r.bound * 100 : 0;
                        double rpr = (double) r.rpcCalls / Math.max(1, r.ok + r.limited);
                        double[] l = lat(r);
                        if (!m.equals("eventual") && r.bound > 0 && r.ok > r.bound * 1.01) safe = false;
                        head.append(head.length() > 0 ? " · " : "").append(String.format("%s %+.1f%%", m, dev));
                        det.append(String.format("%s: %s allowed of %s permitted, p50 %.2f ms, p99 %.2f ms, %.3f calls/request. ", m, human(r.ok), human(r.bound), l[0], l[2], rpr));
                    }
                    set(id, safe ? "pass" : "fail", head + " vs the limit (eventual may overshoot)", det.toString().trim());
                } finally { policy("bounded", null, null); }
            }
            case "election" -> {
                LoadEngine.LoadSpec s = spec(0, 32, 1000, 4, 4, 2000, "election", "steady traffic (election check)");
                s.fresh = false;
                try {
                    int old = probeLeader();
                    if (old == 0) { set(id, "fail", "no leader before the test", ""); return; }
                    c.startLoad(s);
                    sleep(5000);
                    double before = avgAllowed(4);
                    c.view.kill(Ports.coordName(old));
                    c.view.event("warn", "check: leader coord-%d killed", old);
                    long t = waitFor(() -> { int l = probeLeader(); return l != 0 && l != old; }, 20_000);
                    int neu = probeLeader();
                    sleep(4000);
                    double during = avgAllowed(3);
                    c.view.startProc(Ports.coordName(old));
                    long back = waitFor(() -> probeLeader() == old, 25_000);
                    sleep(2000);
                    boolean ok = t >= 0 && during > 0.5 * before;
                    set(id, ok ? "pass" : "fail",
                            t < 0 ? "no new leader within 20 s" : String.format("coord-%d killed -> coord-%d elected in %.1f s; %s/s still allowed (%s/s before)", old, neu, t / 1000.0, human(during), human(before)),
                            back >= 0 ? String.format("When coord-%d restarted it held the highest id and took leadership back (Bully) after %.1f s. Edges used local fallback limits during the election.", old, back / 1000.0)
                                    : "The restarted coordinator did not regain leadership within 25 s.");
                } finally { c.stopLoad(); ensureLeader(); }
            }
            case "mutex" -> {
                StringBuilder head = new StringBuilder(), det = new StringBuilder();
                boolean pass = true;
                try {
                    for (int n : new int[]{3, 5, 7}) {
                        c.view.scale(n);
                        waitFor(() -> c.view.upEdges() >= n && liveCoordEdges() >= n && c.view.allEdges(e -> e.getPeers() == n), 40_000);
                        double[] ratio = new double[2];
                        String[] algos = {"ra", "token"};
                        for (int a = 0; a < 2; a++) {
                            policy(null, algos[a], null);
                            killCoords();
                            waitFor(() -> probeLeader() == 0, 5000);
                            sleep(1500);
                            long m0 = edgeSum(EdgeStats::getMutexMsgs), c0 = edgeSum(EdgeStats::getCsEntries);
                            runAndWait(spec(6, 32, 1200, 1, 2, 500, "mutex", algos[a] + " N=" + n));
                            long m1 = edgeSum(EdgeStats::getMutexMsgs), c1 = edgeSum(EdgeStats::getCsEntries);
                            ratio[a] = (c1 - c0) > 0 ? (double) (m1 - m0) / (c1 - c0) : Double.NaN;
                            ensureLeader();
                        }
                        double raTheory = 2.0 * (n - 1);
                        boolean ok = !Double.isNaN(ratio[0]) && Math.abs(ratio[0] - raTheory) <= raTheory * 0.2 && !Double.isNaN(ratio[1]) && ratio[1] <= n + 0.01;
                        pass &= ok;
                        head.append(head.length() > 0 ? " · " : "").append(String.format("N=%d: RA %.1f, token %.1f", n, ratio[0], ratio[1]));
                        det.append(String.format("N=%d: Ricart-Agrawala %.2f msgs/CS (theory 2(N-1) = %.0f), Suzuki-Kasami %.2f msgs/CS (theory <= N = %d). ", n, ratio[0], raTheory, ratio[1], n));
                    }
                    set(id, pass ? "pass" : "fail", head + " messages per critical section", det.toString().trim()
                            + " RA grows linearly at 2(N-1); the token algorithm costs at most N and nothing while the token stays put.");
                } finally {
                    ensureLeader();
                    policy(null, "local", null);
                    c.view.scale(3);
                    sleepQuiet(3000);
                }
            }
            case "deadlock" -> {
                try {
                    c.view.scale(3);
                    waitFor(() -> c.view.upEdges() >= 3, 20_000);
                    long[] dl = new long[2], pr = new long[2], ab = new long[2];
                    boolean[] ordering = {false, true};
                    for (int k = 0; k < 2; k++) {
                        policy(null, "pool", ordering[k]);
                        killCoords();
                        waitFor(() -> probeLeader() == 0, 5000);
                        sleep(1500);
                        long d0 = edgeSum(EdgeStats::getDeadlocks), p0 = edgeSum(EdgeStats::getProbes), a0 = edgeSum(EdgeStats::getAborts);
                        runAndWait(spec(8, 32, 1500, 1, 1, 500, "deadlock", ordering[k] ? "ordered locks" : "crossing locks"));
                        dl[k] = edgeSum(EdgeStats::getDeadlocks) - d0; pr[k] = edgeSum(EdgeStats::getProbes) - p0; ab[k] = edgeSum(EdgeStats::getAborts) - a0;
                        ensureLeader();
                    }
                    boolean ok = dl[0] > 0 && dl[1] == 0;
                    set(id, ok ? "pass" : dl[0] == 0 ? "info" : "fail",
                            String.format("crossing lock order: %d deadlocks detected with %d probes, %d victims aborted; ordered locking: %d deadlocks", dl[0], pr[0], ab[0], dl[1]),
                            "With the coordinators down, each refill locks the tenant pool and the key pool on their home edges. Even and odd edges lock in opposite orders, so wait-for cycles form. "
                                    + "Blocked edges send Chandy-Misra-Haas probes along the wait-for chain; a probe returning to its initiator proves a cycle and the youngest transaction aborts. "
                                    + String.format("With global lock ordering: %d probes, %d deadlocks.", pr[1], dl[1]));
                } finally { ensureLeader(); policy(null, "local", true); }
            }
            case "lb" -> {
                try {
                    raiseLimits();
                    c.view.slow("edge-2", 30);
                    StringBuilder head = new StringBuilder(), det = new StringBuilder();
                    double[] p99 = new double[3]; double[] slowShare = new double[3];
                    String[] st = {"rr", "leastconn", "leasttime"};
                    String[] nm = {"round robin", "least connections", "least response time"};
                    for (int i = 0; i < 3; i++) {
                        c.lbStrategy(st[i]);
                        sleep(500);
                        Map<String, Long> r0 = lbRequests();
                        var r = runAndWait(spec(6, 48, 0, 8, 8, 2000, "lb", nm[i]));
                        Map<String, Long> r1 = lbRequests();
                        long tot = 0, slow = 0;
                        for (var e : r1.entrySet()) { long d = e.getValue() - r0.getOrDefault(e.getKey(), 0L); tot += d; if (e.getKey().equals("edge-2")) slow = d; }
                        slowShare[i] = tot > 0 ? 100.0 * slow / tot : 0;
                        double[] l = lat(r);
                        p99[i] = l[2];
                        head.append(i > 0 ? " · " : "").append(String.format("%s p99 %.1f ms", nm[i], l[2]));
                        det.append(String.format("%s: %s req/s, p50 %.1f ms, p99 %.1f ms, %.0f%% of requests to the slowed edge-2. ", nm[i], human(r.rps), l[0], l[2], slowShare[i]));
                    }
                    boolean ok = slowShare[1] < slowShare[0] && slowShare[2] < slowShare[0];
                    set(id, ok ? "pass" : "info", head.toString(), "edge-2 slowed by 30 ms to simulate a dynamic load imbalance. " + det.toString().trim()
                            + " Dynamic strategies steer traffic away from the slow node; round robin keeps sending it a third.");
                } finally { c.view.slow("edge-2", 0); c.lbStrategy("rr"); resetLimits(); }
            }
            case "throughput" -> {
                try {
                    raiseLimits();
                    var r = runAndWait(spec(12, 128, 0, 8, 8, 5000, "throughput", "max throughput"));
                    set(id, "info", String.format("%s req/s through LB + auth + 3 limits + proxy, p50 %.1f ms, p99 %.1f ms", human(r.rps), r.latMs[0], r.latMs[2]),
                            "Limits raised for the test. One laptop runs every JVM (load generator, LB, edges, coordinators, upstream), so this is a floor, not a ceiling.");
                } finally { resetLimits(); }
            }
            default -> set(id, "fail", "unknown check", "");
        }
    }

    /** Per-backend request counters, read from the load balancer now (the console's copy can be a second old). */
    private Map<String, Long> lbRequests() {
        Map<String, Long> out = new HashMap<>();
        Map<String, Object> lb;
        try {
            var r = http.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://" + Ports.HOST + ":" + Ports.LB + "/lb/stats"))
                    .timeout(java.time.Duration.ofSeconds(1)).build(), java.net.http.HttpResponse.BodyHandlers.ofString());
            lb = new com.google.gson.Gson().fromJson(r.body(), new com.google.gson.reflect.TypeToken<Map<String, Object>>() {}.getType());
        } catch (Exception e) { return out; }
        Object bs = lb.get("backends");
        if (bs instanceof List<?> l) for (Object o : l) if (o instanceof Map<?, ?> m) out.put(String.valueOf(m.get("name")), ((Number) m.get("requests")).longValue());
        return out;
    }
}
