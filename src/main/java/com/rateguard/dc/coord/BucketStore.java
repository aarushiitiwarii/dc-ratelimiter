package com.rateguard.dc.coord;

import com.rateguard.dc.proto.BucketUpdate;
import com.rateguard.dc.proto.Limit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Authoritative token buckets held by the coordinator leader. Port of the cloud
 * version's lease.lua / give.lua:
 *   Tokens_new = min(Capacity, Tokens_old + dt * RefillRate)
 * plus a weighted two-window sliding counter as a hard cap. Each bucket has its
 * own lock, so operations on different keys never contend. Every mutation is
 * emitted to {@code onChange}, which the leader replicates to its followers.
 */
public final class BucketStore {

    static final class Bucket {
        final ReentrantLock lock = new ReentrantLock();
        double tokens;
        long ts, wstart, wcur, wprev, touched, ttlMs;
    }

    private final ConcurrentHashMap<String, Bucket> map = new ConcurrentHashMap<>();
    private volatile Consumer<BucketUpdate> onChange = u -> {};

    public void onChange(Consumer<BucketUpdate> c) { onChange = c; }

    private Bucket bucket(String key, Limit l, long now) {
        return map.computeIfAbsent(key, k -> {
            Bucket b = new Bucket();
            b.tokens = l.getCap();
            b.ts = now;
            return b;
        });
    }

    private static void refill(Bucket b, Limit l, long now) {
        b.tokens = Math.min(l.getCap(), b.tokens + Math.max(0, now - b.ts) / 1000.0 * l.getRate());
        b.ts = now;
        b.touched = now;
        long ttl = (long) Math.ceil(l.getCap() / Math.max(l.getRate(), 0.001) * 2000);
        if (l.getWindowMs() > 0) ttl = Math.max(ttl, l.getWindowMs() * 3);
        b.ttlMs = Math.max(ttl, 10_000);
    }

    /** Remaining room in the sliding window (Long.MAX_VALUE when the window is off). Rolls the window. */
    private static long windowRoom(Bucket b, Limit l, long now) {
        long win = l.getWindowMs(), lim = l.getWindowLimit();
        if (win <= 0 || lim <= 0) return Long.MAX_VALUE;
        long aligned = now - (now % win);
        if (b.wstart == 0) b.wstart = aligned;
        if (b.wstart < aligned) {
            b.wprev = (aligned - b.wstart >= 2 * win) ? 0 : b.wcur;
            b.wcur = 0;
            b.wstart = aligned;
        }
        double weight = 1 - (double) (now - b.wstart) / win;
        long room = (long) Math.floor(lim - (b.wprev * weight + b.wcur));
        return Math.max(0, room);
    }

    private void emit(String key, Bucket b) {
        onChange.accept(BucketUpdate.newBuilder().setKey(key).setTokens(b.tokens).setTs(b.ts)
                .setWstart(b.wstart).setWcur(b.wcur).setWprev(b.wprev).build());
    }

    /** Grants up to {@code want} tokens: min(tokens, window room, want). */
    public long lease(String key, Limit l, long want, long now) {
        Bucket b = bucket(key, l, now);
        b.lock.lock();
        try {
            refill(b, l, now);
            long granted = (long) Math.floor(Math.min(b.tokens, want));
            long room = windowRoom(b, l, now);
            if (granted > room) granted = room;
            if (granted < 0) granted = 0;
            b.wcur += granted;
            b.tokens -= granted;
            emit(key, b);
            return granted;
        } finally { b.lock.unlock(); }
    }

    /** Returns unused tokens (drift compensation), never above capacity. */
    public void give(String key, Limit l, long n, long now) {
        Bucket b = map.get(key);
        if (b == null || n <= 0) return;
        b.lock.lock();
        try {
            refill(b, l, now);
            b.tokens = Math.min(l.getCap(), b.tokens + n);
            if (l.getWindowMs() > 0 && b.wstart == now - (now % l.getWindowMs())) b.wcur = Math.max(0, b.wcur - n);
            emit(key, b);
        } finally { b.lock.unlock(); }
    }

    /** Charges tokens admitted elsewhere (outage reconciliation / eventual-consistency usage). */
    public double charge(String key, Limit l, long debt, long now) {
        Bucket b = bucket(key, l, now);
        b.lock.lock();
        try {
            refill(b, l, now);
            b.tokens = Math.max(0, b.tokens - debt);
            emit(key, b);
            return b.tokens;
        } finally { b.lock.unlock(); }
    }

    /**
     * Strong consistency: take one token at every level atomically, or none.
     * Buckets are locked in sorted key order so concurrent calls cannot deadlock.
     * Returns -1 when allowed, else the index of the first level that refused.
     */
    public int acquire(String[] keys, Limit[] limits, long now) {
        Integer[] order = new Integer[keys.length];
        for (int i = 0; i < order.length; i++) order[i] = i;
        Arrays.sort(order, (a, c) -> keys[a].compareTo(keys[c]));
        Bucket[] bs = new Bucket[keys.length];
        for (int i = 0; i < keys.length; i++) bs[i] = bucket(keys[i], limits[i], now);
        for (Integer i : order) bs[i].lock.lock();
        try {
            for (int i = 0; i < keys.length; i++) {
                refill(bs[i], limits[i], now);
                if (bs[i].tokens < 1 || windowRoom(bs[i], limits[i], now) < 1) return i;
            }
            for (int i = 0; i < keys.length; i++) {
                bs[i].tokens -= 1;
                if (limits[i].getWindowMs() > 0 && limits[i].getWindowLimit() > 0) bs[i].wcur += 1;
                emit(keys[i], bs[i]);
            }
            return -1;
        } finally {
            for (int j = order.length - 1; j >= 0; j--) bs[order[j]].lock.unlock();
        }
    }

    /** Applies a replicated update (followers). */
    public void apply(BucketUpdate u) {
        Bucket b = map.computeIfAbsent(u.getKey(), k -> new Bucket());
        b.lock.lock();
        try {
            b.tokens = u.getTokens(); b.ts = u.getTs();
            b.wstart = u.getWstart(); b.wcur = u.getWcur(); b.wprev = u.getWprev();
            b.touched = System.currentTimeMillis();
            if (b.ttlMs == 0) b.ttlMs = 60_000;
        } finally { b.lock.unlock(); }
    }

    /** Full state, sent first to a follower that (re)connects. */
    public List<BucketUpdate> snapshot() {
        List<BucketUpdate> out = new ArrayList<>();
        map.forEach((k, b) -> {
            b.lock.lock();
            try {
                out.add(BucketUpdate.newBuilder().setKey(k).setTokens(b.tokens).setTs(b.ts)
                        .setWstart(b.wstart).setWcur(b.wcur).setWprev(b.wprev).build());
            } finally { b.lock.unlock(); }
        });
        return out;
    }

    public void expire(long now) { map.entrySet().removeIf(e -> now - e.getValue().touched > e.getValue().ttlMs); }

    public int size() { return map.size(); }

    /** Current token count (tests and diagnostics). */
    public double tokens(String key) {
        Bucket b = map.get(key);
        return b == null ? Double.NaN : b.tokens;
    }
}
