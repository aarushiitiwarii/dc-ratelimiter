package com.rateguard.dc.console;

import com.google.gson.*;
import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Policies;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.Policy;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Executors;

/**
 * Operations console: serves the dashboard and a JSON API.
 *   GET  /api/state          live cluster, policy, history, runs, events, verification report
 *   POST /api/load           start test traffic          POST /api/load/stop
 *   POST /api/verify         run verification checks     POST /api/limits, /api/limits/reset
 *   POST /api/policy         consistency / outage mode / lock ordering
 *   POST /api/lb             load-balancing strategy     POST /api/cluster  kill|start|scale|slow (via RMI)
 */
public final class ConsoleServer {
    final ClusterView view = new ClusterView();
    final LoadEngine engine;
    final Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).serializeSpecialFloatingPointValues().create();
    final List<LoadEngine.LoadRun> runs = Collections.synchronizedList(new ArrayList<>());
    volatile LoadEngine.LoadRun current;
    private int nextId;
    private volatile long lastEnd;
    final Checks checks;
    private final HttpClient http = HttpClient.newHttpClient();

    ConsoleServer(String secret) {
        engine = new LoadEngine(secret);
        checks = new Checks(this);
    }

    // ------------------------------------------------------------ load control
    LoadEngine.LoadRun startLoad(LoadEngine.LoadSpec spec) {
        LoadEngine.LoadRun prev = current;
        if (prev != null && prev.running()) { prev.stop(); try { prev.done.await(); } catch (InterruptedException ignored) {} }
        long settle = lastEnd + 1500 - System.currentTimeMillis();     // let the previous traffic's syncs land first
        if (settle > 0) try { Thread.sleep(settle); } catch (InterruptedException ignored) {}
        int id;
        synchronized (this) { id = ++nextId; }
        long rpcStart = view.sum(s -> s.getRpcCalls());
        LoadEngine.LoadRun r = engine.start(id, spec, view.policy, Math.max(1, view.upEdges()), rpcStart, run -> {
            try { Thread.sleep(400); } catch (InterruptedException ignored) {}
            view.waitTick();
            run.rpcEnd.set(view.sum(s -> s.getRpcCalls()));
            lastEnd = System.currentTimeMillis();
            LoadEngine.RunSummary s = run.summary();
            view.event("load", "run #%d finished: %d sent, %d ok, %d limited, %d unauthorized, %.0f req/s, p99 %.2f ms",
                    s.id, s.sent, s.ok, s.limited, s.unauth, s.rps, s.latMs[2]);
        });
        current = r;
        view.setCurrentRun(r);
        runs.add(r);
        synchronized (runs) { while (runs.size() > 40) runs.remove(0); }
        view.event("load", "run #%d started -> %s: %s, %d workers, %s req/s, %d tenants x %d keys, %d IPs", id, r.spec.target,
                r.spec.durationS > 0 ? String.format("%.0fs", r.spec.durationS) : "until stopped", r.spec.concurrency,
                r.spec.qps > 0 ? r.spec.qps : "max", r.spec.tenants, r.spec.keys, r.spec.ips);
        return r;
    }

    void stopLoad() {
        LoadEngine.LoadRun r = current;
        if (r != null && r.running()) { r.stop(); try { r.done.await(); } catch (InterruptedException ignored) {} }
    }

    // ------------------------------------------------------------ policy helpers
    Policy updatePolicy(java.util.function.UnaryOperator<Policy.Builder> f) {
        Policy p = view.setPolicy(f.apply(view.policy.toBuilder()).build());
        LoadEngine.LoadRun r = current;
        if (p != null && r != null && r.running()) r.limitsMoved.set(true);
        return p;
    }

    static Limit limitFrom(JsonObject o, Limit def) {
        Limit.Builder b = def.toBuilder();
        if (o.has("cap")) b.setCap(o.get("cap").getAsDouble());
        if (o.has("rate")) b.setRate(o.get("rate").getAsDouble());
        if (o.has("window_ms") && o.get("window_ms").getAsLong() > 0) b.setWindowMs(o.get("window_ms").getAsLong());
        if (o.has("window_limit")) b.setWindowLimit(o.get("window_limit").getAsLong());
        return b.build();
    }

    // ------------------------------------------------------------ HTTP
    void start() throws IOException {
        view.start();
        HttpServer srv = HttpServer.create(new InetSocketAddress(Ports.HOST, Ports.CONSOLE), 256);
        srv.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        srv.createContext("/", this::handle);
        srv.start();
        Log.info("console on http://localhost:%d", Ports.CONSOLE);
        view.event("info", "console started");
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        try {
            if (!path.startsWith("/api/")) { serveStatic(ex); return; }
            if (path.equals("/api/state")) { json(ex, 200, state()); return; }
            JsonObject body = readJson(ex);
            switch (path) {
                case "/api/load" -> {
                    LoadEngine.LoadSpec spec = gson.fromJson(body, LoadEngine.LoadSpec.class);
                    json(ex, 200, startLoad(spec).summary());
                }
                case "/api/load/stop" -> { checks.cancel(); stopLoad(); view.event("load", "traffic stopped by user"); json(ex, 200, Map.of("ok", true)); }
                case "/api/verify" -> {
                    List<String> ids = new ArrayList<>();
                    if (body.has("tests")) for (JsonElement e : body.getAsJsonArray("tests")) ids.add(e.getAsString());
                    if (!checks.run(ids)) { text(ex, 400, "tests must name checks: " + String.join(", ", Checks.ORDER) + " or all"); return; }
                    json(ex, 200, Map.of("ok", true));
                }
                case "/api/limits" -> {
                    String lvl = body.has("level") ? body.get("level").getAsString() : "";
                    Policy p = updatePolicy(b -> switch (lvl) {
                        case "tenant" -> b.setTenant(limitFrom(body, b.getTenant()));
                        case "key" -> b.setKey(limitFrom(body, b.getKey()));
                        case "ip" -> { b.setIp(limitFrom(body, b.getIp())); if (body.has("local")) b.setIpLocal(body.get("local").getAsBoolean()); yield b; }
                        default -> b;
                    });
                    if (p == null) { text(ex, 503, "no coordinator reachable"); return; }
                    view.event("limit", "%s limit -> cap %s, %s/s (pushed by the leader to every edge over the Control stream)", lvl,
                            body.has("cap") ? body.get("cap").getAsString() : "-", body.has("rate") ? body.get("rate").getAsString() : "-");
                    json(ex, 200, Map.of("version", p.getVersion()));
                }
                case "/api/limits/reset" -> {
                    Policy d = Policies.defaults();
                    Policy p = updatePolicy(b -> b.setTenant(d.getTenant()).setKey(d.getKey()).setIp(d.getIp()).setIpLocal(d.getIpLocal()));
                    if (p == null) { text(ex, 503, "no coordinator reachable"); return; }
                    view.event("limit", "limits reset to defaults");
                    json(ex, 200, Map.of("ok", true));
                }
                case "/api/policy" -> {
                    Policy p = updatePolicy(b -> {
                        if (body.has("consistency")) b.setConsistency(body.get("consistency").getAsString());
                        if (body.has("outage_mode")) b.setOutageMode(body.get("outage_mode").getAsString());
                        if (body.has("lock_ordering")) b.setLockOrdering(body.get("lock_ordering").getAsBoolean());
                        return b;
                    });
                    if (p == null) { text(ex, 503, "no coordinator reachable"); return; }
                    view.event("limit", "policy v%d: consistency=%s, outage mode=%s, lock ordering=%s", p.getVersion(), p.getConsistency(), p.getOutageMode(), p.getLockOrdering() ? "on" : "off");
                    json(ex, 200, Map.of("version", p.getVersion()));
                }
                case "/api/lb" -> {
                    String s = body.has("strategy") ? body.get("strategy").getAsString() : "rr";
                    lbStrategy(s);
                    view.event("limit", "load-balancing strategy -> %s", s);
                    json(ex, 200, Map.of("ok", true));
                }
                case "/api/cluster" -> {
                    String action = body.has("action") ? body.get("action").getAsString() : "";
                    String name = body.has("name") ? body.get("name").getAsString() : "";
                    boolean ok = switch (action) {
                        case "kill" -> { boolean k = view.kill(name); if (k) view.event("warn", "%s killed (failure injection)", name); yield k; }
                        case "start" -> { boolean k = view.startProc(name); if (k) view.event("info", "%s started", name); yield k; }
                        case "kill-leader" -> { int l = view.leader; boolean k = l != 0 && view.kill(Ports.coordName(l)); if (k) view.event("warn", "leader coord-%d killed (failure injection)", l); yield k; }
                        case "kill-coords" -> { boolean any = false; for (int i = 1; i <= Ports.COORDS; i++) any |= view.kill(Ports.coordName(i)); if (any) view.event("warn", "all coordinators killed: no leader"); yield any; }
                        case "start-coords" -> { boolean any = false; for (int i = 1; i <= Ports.COORDS; i++) any |= view.startProc(Ports.coordName(i)); if (any) view.event("info", "coordinators starting"); yield any; }
                        case "scale" -> { int n = view.scale(body.get("n").getAsInt()); if (n > 0) view.event("info", "edges scaled to %d", n); yield n > 0; }
                        case "slow" -> { long ms = body.has("ms") ? body.get("ms").getAsLong() : 0; boolean k = view.slow(name, ms); if (k) view.event("info", "%s slowed by %d ms", name, ms); yield k; }
                        default -> false;
                    };
                    if (!ok) { text(ex, 409, "action " + action + " had no effect"); return; }
                    json(ex, 200, Map.of("ok", true));
                }
                case "/api/events/clear" -> { view.events.clear(); runs.clear(); json(ex, 200, Map.of("ok", true)); }
                default -> text(ex, 404, "not found");
            }
        } catch (Exception e) {
            text(ex, 500, "error: " + e.getMessage());
        }
    }

    void lbStrategy(String s) {
        try { http.send(HttpRequest.newBuilder(URI.create("http://" + Ports.HOST + ":" + Ports.LB + "/lb/strategy?name=" + s)).build(), HttpResponse.BodyHandlers.discarding()); }
        catch (Exception ignored) {}
    }

    private Map<String, Object> state() {
        Map<String, Object> st = new LinkedHashMap<>();
        st.put("now", System.currentTimeMillis());
        List<Map<String, Object>> pods = new ArrayList<>();
        List<ClusterView.EdgeView> evs = new ArrayList<>(view.edges.values());
        evs.sort(Comparator.comparingInt(v -> Integer.parseInt(v.name.substring(v.name.lastIndexOf('-') + 1))));
        for (ClusterView.EdgeView v : evs) {
            Map<String, Object> p = new LinkedHashMap<>();
            p.put("name", v.name); p.put("up", v.up); p.put("mode", v.last == null ? "" : v.last.getMode());
            Map<String, Object> stats = new LinkedHashMap<>();
            if (v.last != null) {
                var s = v.last;
                stats.put("allowed", s.getAllowed()); stats.put("denied", s.getDenied()); stats.put("unauthorized", s.getUnauthorized());
                stats.put("rpc_calls", s.getRpcCalls()); stats.put("fallback_events", s.getFallbackEvents()); stats.put("reconciled", s.getReconciled());
                stats.put("buckets", s.getBuckets()); stats.put("mutex_msgs", s.getMutexMsgs()); stats.put("cs_entries", s.getCsEntries());
                stats.put("probes", s.getProbes()); stats.put("deadlocks", s.getDeadlocks()); stats.put("aborts", s.getAborts());
                stats.put("leader", s.getLeader()); stats.put("slow_ms", s.getSlowMs()); stats.put("peers", s.getPeers());
            }
            p.put("stats", stats);
            p.put("rates", v.rates);
            pods.add(p);
        }
        st.put("pods", pods);
        List<Map<String, Object>> cs = new ArrayList<>();
        for (int i = 1; i <= Ports.COORDS; i++) {
            ClusterView.CoordView c = view.coords.get(i);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", i); m.put("name", Ports.coordName(i)); m.put("up", c.up);
            if (c.up && c.status != null) {
                m.put("leader", c.status.getLeader()); m.put("term", c.status.getTerm()); m.put("buckets", c.status.getBuckets());
                m.put("elections", c.status.getElections()); m.put("replicated", c.status.getReplicated()); m.put("edges", c.status.getEdges());
            }
            cs.add(m);
        }
        st.put("coords", cs);
        st.put("leader", view.leader);
        Map<String, Object> store = new LinkedHashMap<>();
        Map<String, Object> last = view.history.isEmpty() ? Map.of() : view.history.get(view.history.size() - 1);
        store.put("up", view.leader != 0); store.put("leader", view.leader); store.put("live", view.liveCoords());
        store.put("ops_sec", last.getOrDefault("redis_ops", 0.0));
        st.put("redis", store);
        Policy p = view.policy;
        st.put("limits", List.of(limitJson(p.getTenant(), false), limitJson(p.getKey(), false), limitJson(p.getIp(), p.getIpLocal())));
        st.put("policy", Map.of("consistency", p.getConsistency(), "outage_mode", p.getOutageMode(), "lock_ordering", p.getLockOrdering(), "version", p.getVersion()));
        st.put("lb", view.lb);
        List<Map<String, Object>> procs = new ArrayList<>();
        for (var pi : view.processes) procs.add(Map.of("name", pi.name(), "role", pi.role(), "port", pi.port(), "alive", pi.alive(), "pid", pi.pid()));
        st.put("processes", procs);
        st.put("kube", view.rmiOk);
        synchronized (view.history) { st.put("history", new ArrayList<>(view.history)); }
        List<Map<String, Object>> evs2 = new ArrayList<>();
        synchronized (view.events) { for (var e : view.events) evs2.add(Map.of("t", e.t(), "kind", e.kind(), "msg", e.msg())); }
        st.put("events", evs2);
        List<LoadEngine.RunSummary> rs = new ArrayList<>();
        synchronized (runs) { for (var r : runs) rs.add(r.summary()); }
        st.put("runs", rs);
        LoadEngine.LoadRun cur = current;
        st.put("current", cur == null ? null : cur.summary());
        st.put("job", checks.job());
        st.put("report", checks.report());
        st.put("checks", Checks.ORDER);
        st.put("check_names", Checks.NAMES);
        return st;
    }

    private static Map<String, Object> limitJson(Limit l, boolean local) {
        return Map.of("cap", l.getCap(), "rate", l.getRate(), "window_ms", l.getWindowMs(), "window_limit", l.getWindowLimit(), "local", local);
    }

    private void serveStatic(HttpExchange ex) throws IOException {
        try (InputStream in = ConsoleServer.class.getResourceAsStream("/web/index.html")) {
            byte[] b = in == null ? "missing web/index.html".getBytes(StandardCharsets.UTF_8) : in.readAllBytes();
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.getResponseHeaders().set("Cache-Control", "no-cache");
            ex.sendResponseHeaders(200, b.length);
            try (OutputStream os = ex.getResponseBody()) { os.write(b); }
        }
    }

    private JsonObject readJson(HttpExchange ex) throws IOException {
        String s = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        if (s.isBlank()) return new JsonObject();
        JsonElement e = JsonParser.parseString(s);
        return e.isJsonObject() ? e.getAsJsonObject() : new JsonObject();
    }

    private void json(HttpExchange ex, int code, Object o) throws IOException {
        byte[] b = gson.toJson(o).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    private static void text(HttpExchange ex, int code, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(code, b.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(b); }
    }

    public static void main(String secret) throws Exception {
        Log.setName("console");
        new ConsoleServer(secret).start();
        Thread.currentThread().join();
    }
}
