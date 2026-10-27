package com.rateguard.dc.console;

import com.rateguard.dc.auth.Authenticator;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.Policy;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/** In-process load generator: signed API keys from many spoofed client IPs, sent through the load balancer. */
final class LoadEngine {

    /** What to send (JSON from the UI: duration_s, concurrency, qps, tenants, keys, ips, bad_pct, fresh, skew). */
    static final class LoadSpec {
        String target = "gateway";   // gateway (through the LB) | direct (upstream, no limiter)
        double durationS = 10;
        int concurrency = 64, qps, tenants = 1, keys = 1, ips = 1, badPct;
        boolean skew, fresh;
        String scenario = "manual", label = "";

        void normalize() {
            if (!"direct".equals(target)) target = "gateway";
            concurrency = clamp(concurrency == 0 ? 64 : concurrency, 1, 2048);
            tenants = clamp(tenants == 0 ? 1 : tenants, 1, 1000);
            keys = clamp(keys == 0 ? 1 : keys, 1, 1000);
            ips = clamp(ips == 0 ? 1 : ips, 1, 1_000_000);
            qps = Math.max(0, qps);
            badPct = clamp(badPct, 0, 100);
            if (durationS < 0 || durationS > 900) durationS = 900;
        }
        private static int clamp(int v, int lo, int hi) { return Math.max(lo, Math.min(hi, v)); }
    }

    /** Summary sent to the UI. */
    static final class RunSummary {
        int id; String scenario, label; LoadSpec spec; boolean running; long started; double elapsedS;
        long sent, ok, limited, unauth, other, errors; long[] byLevel = new long[3]; double rps;
        double[] latMs = new double[4], latOkMs = new double[4];
        double bound; String boundNote = ""; boolean saturated, limitsChanged; long rpcCalls;
    }

    final class LoadRun {
        final int id; final LoadSpec spec; final long start = System.currentTimeMillis();
        final AtomicLong end = new AtomicLong(), ok = new AtomicLong(), limited = new AtomicLong(), unauth = new AtomicLong(),
                other = new AtomicLong(), errs = new AtomicLong(), rpcStart = new AtomicLong(), rpcEnd = new AtomicLong(-1);
        final AtomicLong[] byLevel = {new AtomicLong(), new AtomicLong(), new AtomicLong()};
        final Hist all = new Hist(), okH = new Hist();
        final AtomicReference<Hist> win = new AtomicReference<>(new Hist());
        final AtomicBoolean stopped = new AtomicBoolean(), finalized = new AtomicBoolean(), limitsMoved = new AtomicBoolean();
        final CountDownLatch done = new CountDownLatch(1);
        final Policy policyAtStart; final int membersAtStart;
        final List<String> creds = new ArrayList<>();

        LoadRun(int id, LoadSpec spec, Policy p, int members) { this.id = id; this.spec = spec; this.policyAtStart = p; this.membersAtStart = members; }

        void stop() { stopped.set(true); }
        boolean running() { return !finalized.get(); }
        double elapsed() { long e = end.get(); return ((e != 0 ? e : System.currentTimeMillis()) - start) / 1000.0; }

        RunSummary summary() {
            RunSummary s = new RunSummary();
            s.id = id; s.scenario = spec.scenario; s.label = spec.label; s.spec = spec; s.running = running();
            s.started = start; s.elapsedS = elapsed();
            s.ok = ok.get(); s.limited = limited.get(); s.unauth = unauth.get(); s.other = other.get(); s.errors = errs.get();
            for (int i = 0; i < 3; i++) s.byLevel[i] = byLevel[i].get();
            s.sent = s.ok + s.limited + s.unauth + s.other;
            if (s.elapsedS > 0) s.rps = s.sent / s.elapsedS;
            s.latMs = all.pcts(.5, .95, .99, .999);
            s.latOkMs = okH.pcts(.5, .95, .99, .999);
            s.limitsChanged = limitsMoved.get();
            if ("gateway".equals(spec.target)) {
                bound(s);
                s.saturated = s.bound > 0 && (s.ok + s.limited) > s.bound * 1.05;
                if (rpcEnd.get() >= 0) s.rpcCalls = rpcEnd.get() - rpcStart.get();
            }
            return s;
        }

        /** Most requests the limits permit: min(sum over tenants of min(tenant, keys x key), IPs x IP). */
        private void bound(RunSummary s) {
            if (!spec.fresh) { s.boundNote = "re-used buckets (not fresh): no exact bound"; return; }
            if (limitsMoved.get()) { s.boundNote = "limits changed during the run: no exact bound"; return; }
            double d = s.elapsedS;
            Policy p = policyAtStart;
            double keyB = budget(p.getKey(), d), tenB = budget(p.getTenant(), d), total = 0;
            String binding = "key";
            for (int t = 0; t < spec.tenants; t++) {
                double k = spec.keys * keyB;
                if (tenB < k) { k = tenB; binding = "tenant"; }
                total += k;
            }
            double ipTotal = spec.ips * budget(p.getIp(), d);
            if (ipTotal < total) { total = ipTotal; binding = "ip"; }
            s.bound = Math.floor(total);
            s.boundNote = binding + " level binds";
        }

        private static double budget(Limit l, double d) {
            double b = l.getCap() + l.getRate() * d;
            if (l.getWindowMs() > 0 && l.getWindowLimit() > 0) b = Math.min(b, l.getWindowLimit() * (d * 1000 / l.getWindowMs() + 1));
            return b;
        }
    }

    private static final String[] TENANTS = {"acme", "globex", "initech", "umbrella", "hooli", "stark", "wayne", "wonka",
            "tyrell", "cyberdyne", "soylent", "aperture", "oscorp", "wernham", "dunder", "vandelay"};
    static String tenant(int i) { return i < TENANTS.length ? TENANTS[i] : "tenant" + i; }
    static String ipOf(int i) { return (11 + i / 62500 % 200) + "." + (i / 250 % 250) + "." + (i % 250) + "." + (1 + i % 7); }

    private final Authenticator minter;

    LoadEngine(String secret) { this.minter = new Authenticator(secret, 16, 1000); }

    LoadRun start(int id, LoadSpec spec, Policy policy, int members, long rpcStart, Consumer<LoadRun> onDone) {
        spec.normalize();
        LoadRun r = new LoadRun(id, spec, policy, members);
        r.rpcStart.set(rpcStart);
        String suffix = spec.fresh ? "-r" + id : "";
        for (int t = 0; t < spec.tenants; t++)
            for (int k = 0; k < spec.keys; k++) r.creds.add(minter.mintKey(tenant(t) + suffix, "key" + k));
        String bad = "forged.key0.00000000000000000000000000000000";
        String[] ips = new String[spec.ips];
        for (int i = 0; i < ips.length; i++) ips[i] = ipOf(i);
        URI uri = URI.create("direct".equals(spec.target) ? "http://" + Ports.HOST + ":" + Ports.upstream(1) + "/" : "http://" + Ports.HOST + ":" + Ports.LB + "/");
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(2))
                .executor(Executors.newVirtualThreadPerTaskExecutor()).build();
        long intervalNs = spec.qps > 0 ? (long) (1e9 * spec.concurrency / spec.qps) : 0;
        long deadline = r.start + (long) ((spec.durationS > 0 ? spec.durationS : 900) * 1000);
        CountDownLatch workers = new CountDownLatch(spec.concurrency);
        for (int w = 0; w < spec.concurrency; w++) {
            final int wid = w;
            Thread.startVirtualThread(() -> {
                Random rng = new Random(wid * 7919L + System.nanoTime());
                long next = System.nanoTime() + (intervalNs > 0 ? (long) (rng.nextDouble() * intervalNs) : 0);
                try {
                    while (!r.stopped.get() && System.currentTimeMillis() < deadline) {
                        if (intervalNs > 0) {
                            long wait = next - System.nanoTime();
                            if (wait > 0) Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
                            next += intervalNs;
                            if (System.nanoTime() - next > 1_000_000_000L) next = System.nanoTime();   // don't burst to catch up
                        }
                        int ci = rng.nextInt(r.creds.size());
                        if (spec.skew && rng.nextInt(100) < 80) ci = 0;
                        String key = spec.badPct > 0 && rng.nextInt(100) < spec.badPct ? bad : r.creds.get(ci);
                        HttpRequest req = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                                .header("X-API-Key", key).header("X-Forwarded-For", ips[rng.nextInt(ips.length)]).GET().build();
                        long t0 = System.nanoTime();
                        try {
                            HttpResponse<Void> resp = client.send(req, HttpResponse.BodyHandlers.discarding());
                            long us = (System.nanoTime() - t0) / 1000;
                            r.all.add(us); r.win.get().add(us);
                            switch (resp.statusCode()) {
                                case 200 -> { r.ok.incrementAndGet(); r.okH.add(us); }
                                case 429 -> {
                                    r.limited.incrementAndGet();
                                    switch (resp.headers().firstValue("X-RateLimit-Level").orElse("")) {
                                        case "tenant" -> r.byLevel[0].incrementAndGet();
                                        case "key" -> r.byLevel[1].incrementAndGet();
                                        case "ip" -> r.byLevel[2].incrementAndGet();
                                        default -> {}
                                    }
                                }
                                case 401 -> r.unauth.incrementAndGet();
                                default -> r.other.incrementAndGet();
                            }
                        } catch (Exception e) {
                            r.errs.incrementAndGet();
                            Thread.sleep(20);
                        }
                    }
                } catch (InterruptedException ignored) {
                } finally { workers.countDown(); }
            });
        }
        Thread.startVirtualThread(() -> {
            try { workers.await(); } catch (InterruptedException ignored) {}
            r.end.set(System.currentTimeMillis());
            if (onDone != null) onDone.accept(r);
            r.finalized.set(true);
            r.done.countDown();
        });
        return r;
    }
}
