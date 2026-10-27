package com.rateguard.dc.console;

import java.util.concurrent.atomic.AtomicLongArray;

/** Lock-free latency histogram in microseconds (about 1-10% resolution up to 5 s). */
final class Hist {
    private static final int N = 380;
    private final AtomicLongArray b = new AtomicLongArray(N);

    private static int idx(long us) {
        if (us < 1000) return (int) (us / 10);
        if (us < 10_000) return 100 + (int) ((us - 1000) / 100);
        if (us < 100_000) return 190 + (int) ((us - 10_000) / 1000);
        if (us < 5_000_000) return 280 + (int) ((us - 100_000) / 50_000);
        return N - 1;
    }

    /** Midpoint of bucket i in milliseconds. */
    private static double val(int i) {
        if (i < 100) return (i * 10 + 5) / 1000.0;
        if (i < 190) return (1000 + (i - 100) * 100 + 50) / 1000.0;
        if (i < 280) return (10_000 + (i - 190) * 1000 + 500) / 1000.0;
        if (i < N - 1) return (100_000 + (i - 280) * 50_000 + 25_000) / 1000.0;
        return 5000;
    }

    void add(long us) { b.incrementAndGet(idx(Math.max(0, us))); }

    long count() { long n = 0; for (int i = 0; i < N; i++) n += b.get(i); return n; }

    /** Percentiles (0..1) in ms. */
    double[] pcts(double... ps) {
        long[] c = new long[N]; long n = 0;
        for (int i = 0; i < N; i++) { c[i] = b.get(i); n += c[i]; }
        double[] out = new double[ps.length];
        if (n == 0) return out;
        for (int j = 0; j < ps.length; j++) {
            long target = Math.max(1, (long) Math.ceil(ps[j] * n)), acc = 0;
            for (int i = 0; i < N; i++) { acc += c[i]; if (acc >= target) { out[j] = val(i); break; } }
        }
        return out;
    }
}
