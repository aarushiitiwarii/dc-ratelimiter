package com.rateguard.dc.lb;

import com.google.gson.Gson;
import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.launcher.ClusterControl;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.rmi.registry.LocateRegistry;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Front load balancer for the edge nodes (lab experiment 7), replacing the
 * Kubernetes Service. Strategies, switchable at runtime:
 *   rr         round robin: next backend in turn (static)
 *   leastconn  least connections: fewest requests in flight (dynamic)
 *   leasttime  least response time: lowest EWMA latency x (in-flight + 1) (dynamic)
 * Backends come from the launcher's RMI registry, so scaling edges is picked up live.
 */
public final class LoadBalancer {

    static final class Backend {
        final String name; final int port;
        final AtomicInteger inflight = new AtomicInteger();
        final AtomicLong requests = new AtomicLong(), errors = new AtomicLong();
        volatile double ewmaMs = 1;
        volatile long downUntil;
        Backend(String name, int port) { this.name = name; this.port = port; }
    }

    public interface Strategy { Backend pick(List<Backend> live); }

    static final class RoundRobin implements Strategy {
        private final AtomicInteger i = new AtomicInteger();
        public Backend pick(List<Backend> live) { return live.get(Math.floorMod(i.getAndIncrement(), live.size())); }
    }

    static final class LeastConnections implements Strategy {
        public Backend pick(List<Backend> live) {
            Backend best = null; int bestN = Integer.MAX_VALUE; int ties = 0;
            for (Backend b : live) {
                int n = b.inflight.get();
                if (n < bestN) { best = b; bestN = n; ties = 1; }
                else if (n == bestN && ThreadLocalRandom.current().nextInt(++ties) == 0) best = b;   // random among ties
            }
            return best;
        }
    }

    static final class LeastResponseTime implements Strategy {
        public Backend pick(List<Backend> live) {
            Backend best = null; double bestScore = Double.MAX_VALUE;
            for (Backend b : live) {
                double s = b.ewmaMs * (b.inflight.get() + 1);
                if (s < bestScore) { best = b; bestScore = s; }
            }
            return best;
        }
    }

    static Strategy strategy(String name) {
        return switch (name) {
            case "leastconn" -> new LeastConnections();
            case "leasttime" -> new LeastResponseTime();
            default -> new RoundRobin();
        };
    }

    private final Map<String, Backend> backends = new ConcurrentHashMap<>();
    private final List<Backend> order = new CopyOnWriteArrayList<>();
    private volatile String strategyName = "rr";
    private volatile Strategy strategy = new RoundRobin();
    private final HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(500)).executor(Executors.newVirtualThreadPerTaskExecutor()).build();
    private final Gson gson = new Gson();

    public void setStrategy(String name) {
        strategyName = switch (name) { case "leastconn", "leasttime" -> name; default -> "rr"; };
        strategy = strategy(strategyName);
        Log.info("strategy -> %s", strategyName);
    }

    private void setBackends(List<String> names) {
        for (String n : names) backends.computeIfAbsent(n, k -> {
            Backend b = new Backend(k, Ports.edgeHttp(Integer.parseInt(k.substring(k.lastIndexOf('-') + 1))));
            order.add(b);
            return b;
        });
        order.removeIf(b -> !names.contains(b.name));
        backends.keySet().retainAll(names);
    }

    /** Membership from the launcher's RMI registry (the cluster's naming service). */
    private void discoveryLoop() {
        while (true) {
            try {
                ClusterControl cc = (ClusterControl) LocateRegistry.getRegistry(Ports.HOST, Ports.RMI_REGISTRY).lookup(ClusterControl.NAME);
                setBackends(cc.edges());
            } catch (Exception e) {
                if (order.isEmpty()) setBackends(List.of("edge-1", "edge-2", "edge-3"));
            }
            try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
        }
    }

    public void start() throws IOException {
        HttpServer srv = HttpServer.create(new InetSocketAddress(Ports.HOST, Ports.LB), 4096);
        srv.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        srv.createContext("/", this::handle);
        srv.start();
        Thread d = new Thread(this::discoveryLoop, "lb-discovery");
        d.setDaemon(true);
        d.start();
        Log.info("load balancer on :%d (strategy %s)", Ports.LB, strategyName);
    }

    private void handle(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.equals("/lb/stats")) { json(ex, stats()); return; }
        if (path.equals("/lb/strategy")) {
            String q = ex.getRequestURI().getQuery();
            if (q != null && q.startsWith("name=")) setStrategy(q.substring(5));
            json(ex, stats());
            return;
        }
        long now = System.currentTimeMillis();
        List<Backend> live = new ArrayList<>();
        for (Backend b : order) if (b.downUntil < now) live.add(b);
        if (live.isEmpty()) { send(ex, 503, "no edge nodes available\n", null); return; }

        for (int attempt = 0; attempt < 2; attempt++) {
            Backend b = strategy.pick(live);
            b.inflight.incrementAndGet();
            long t0 = System.nanoTime();
            try {
                HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create("http://" + Ports.HOST + ":" + b.port + ex.getRequestURI()))
                        .timeout(Duration.ofSeconds(5)).GET();
                for (String h : new String[]{"X-API-Key", "Authorization"}) {
                    String v = ex.getRequestHeaders().getFirst(h);
                    if (v != null) rb.header(h, v);
                }
                String xff = ex.getRequestHeaders().getFirst("X-Forwarded-For");
                rb.header("X-Forwarded-For", xff != null ? xff : ex.getRemoteAddress().getAddress().getHostAddress());
                HttpResponse<byte[]> r = http.send(rb.build(), HttpResponse.BodyHandlers.ofByteArray());
                double ms = (System.nanoTime() - t0) / 1e6;
                b.ewmaMs = 0.8 * b.ewmaMs + 0.2 * ms;
                b.requests.incrementAndGet();
                Map<String, String> hs = new HashMap<>();
                for (String h : new String[]{"X-RateLimit-Level", "X-Served-By", "Retry-After"})
                    r.headers().firstValue(h).ifPresent(v -> hs.put(h, v));
                send(ex, r.statusCode(), r.body(), hs);
                return;
            } catch (Exception e) {
                b.errors.incrementAndGet();
                b.downUntil = System.currentTimeMillis() + 1000;    // passive health check
                live.remove(b);
                if (live.isEmpty()) break;
            } finally {
                b.inflight.decrementAndGet();
            }
        }
        send(ex, 502, "edge unavailable\n", null);
    }

    Map<String, Object> stats() {
        List<Map<String, Object>> bs = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (Backend b : order) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", b.name); m.put("inflight", b.inflight.get()); m.put("requests", b.requests.get());
            m.put("errors", b.errors.get()); m.put("ewma_ms", Math.round(b.ewmaMs * 100) / 100.0); m.put("up", b.downUntil < now);
            bs.add(m);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("strategy", strategyName);
        out.put("backends", bs);
        return out;
    }

    private void json(HttpExchange ex, Object o) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "application/json");
        send(ex, 200, gson.toJson(o), null);
    }

    private static void send(HttpExchange ex, int code, String body, Map<String, String> hs) throws IOException {
        send(ex, code, body.getBytes(StandardCharsets.UTF_8), hs);
    }

    private static void send(HttpExchange ex, int code, byte[] body, Map<String, String> hs) throws IOException {
        if (hs != null) hs.forEach((k, v) -> ex.getResponseHeaders().set(k, v));
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(body); }
    }

    public static void main(String strategy) throws Exception {
        Log.setName("lb");
        LoadBalancer lb = new LoadBalancer();
        lb.setStrategy(strategy);
        lb.start();
        Thread.currentThread().join();
    }
}
