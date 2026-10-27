package com.rateguard.dc.edge;

import com.rateguard.dc.cache.ShardedLru;
import com.rateguard.dc.common.Policies;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.Policy;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Edge-side rate limiter (port of the cloud version's gateway/limiter.go).
 *
 * Every request needs one token at three levels: tenant, API key and client IP.
 * How a token is obtained depends on the cluster policy:
 *  - consistency "bounded" (default): spend tokens reserved locally ("leases") from
 *    the coordinator leader; lease size adapts to demand; unused tokens are returned.
 *  - consistency "strong": one synchronous Acquire RPC per request (linearizable, exact, slower).
 *  - consistency "eventual": spend from a local view of the global bucket that is
 *    synced once a second (fast, but nodes can overshoot between syncs).
 * When no coordinator leader is reachable, buckets switch to an outage mode:
 *  - "local": each node enforces cap/N and rate/N on its own (approximation),
 *  - "ra" / "token": a shared outage bucket updated under Ricart-Agrawala or
 *    Suzuki-Kasami mutual exclusion among the edge nodes (exact),
 *  - "pool": tenant and key pools locked together on their home nodes (deadlocks
 *    detected with Chandy-Misra-Haas when lock ordering is off).
 */
public final class Limiter {
    public static final int TENANT = 0, KEY = 1, IP = 2;
    public static final String[] LEVEL = {"tenant", "key", "ip"};

    /** Access to the coordinator leader. */
    public interface Leader {
        long lease(String key, Limit lim, long want) throws LeaderClient.NoLeader;
        int acquire(String[] keys, Limit[] lims) throws LeaderClient.NoLeader;
        double peek(String key, Limit lim, long consume) throws LeaderClient.NoLeader;
        void give(String key, Limit lim, long n);
        void debt(String key, Limit lim, long n);
        boolean alive();
    }

    /** Shared outage bucket for one resource (Ricart-Agrawala or Suzuki-Kasami). */
    public interface Outage {
        long take(String resource, Limit lim, long want) throws LeaderClient.NoLeader;
    }

    /** Pool mode: take the same number of tokens from two pools inside one transaction. */
    public interface PoolOutage {
        long takeBoth(String r1, Limit l1, String r2, Limit l2, long want) throws LeaderClient.NoLeader;
    }

    static final class Bucket {
        final ReentrantLock lock = new ReentrantLock();
        final String key; final int lvl;
        long tokens, expiresAt, nextRefill, lastGrant, lastUsed, leaseN = 1;
        boolean fallback, everLeased; String mode = "local";
        double localTokens; long lastFill; long fbConsumed;
        double view = Double.NaN; long consumed, lastSync;     // eventual consistency
        Bucket(String key, int lvl) { this.key = key; this.lvl = lvl; }
    }

    private final Leader leader;
    private final Supplier<Policy> policy;
    private final IntSupplier members;
    private final Map<String, Outage> outages = new ConcurrentHashMap<>();
    private volatile PoolOutage pool;
    private final ShardedLru<Bucket> buckets;
    private final long batchMax, leaseTtl;

    public final AtomicLong allowed = new AtomicLong(), denied = new AtomicLong(), rpcCalls = new AtomicLong(),
            fallbackKeys = new AtomicLong(), fallbackEvents = new AtomicLong(), reconciled = new AtomicLong(),
            tokensReturned = new AtomicLong();
    public final AtomicLong[] deniedBy = {new AtomicLong(), new AtomicLong(), new AtomicLong()};

    public Limiter(Leader leader, Supplier<Policy> policy, IntSupplier members, long batchMax, long leaseTtlMs, int cacheSize) {
        this.leader = leader; this.policy = policy; this.members = members;
        this.batchMax = batchMax; this.leaseTtl = leaseTtlMs;
        this.buckets = new ShardedLru<>(cacheSize, 16, (k, b) -> {
            long n; b.lock.lock(); try { n = b.tokens; b.tokens = 0; } finally { b.lock.unlock(); }
            if (!b.fallback && n > 0) { leader.give(k, Policies.level(policy.get(), b.lvl), n); tokensReturned.addAndGet(n); }
        });
    }

    public void registerOutage(String mode, Outage o) { outages.put(mode, o); }
    public void registerPool(PoolOutage p) { pool = p; }
    public int buckets() { return buckets.size(); }

    private int n() { return Math.max(1, members.getAsInt()); }
    private Bucket bucket(int lvl, String key) { return buckets.getOrCreate(key, () -> new Bucket(key, lvl)); }

    /** Returns -1 when allowed, else the level (0 tenant, 1 key, 2 IP) that refused. */
    public int allow(String[] keys) {
        Policy p = policy.get();
        Limit[] lims = {p.getTenant(), p.getKey(), p.getIp()};
        boolean[] local = {false, false, p.getIpLocal()};
        Bucket[] bs = {bucket(TENANT, keys[0]), bucket(KEY, keys[1]), bucket(IP, keys[2])};
        int d = switch (p.getConsistency()) {
            case "strong" -> allowStrong(keys, lims, local, bs, p);
            case "eventual" -> allowEventual(lims, local, bs);
            default -> allowBounded(lims, local, bs, p);
        };
        if (d < 0) allowed.incrementAndGet(); else { denied.incrementAndGet(); deniedBy[d].incrementAndGet(); }
        return d;
    }

    // ------------------------------------------------------------ bounded staleness (leases)
    private int allowBounded(Limit[] lims, boolean[] local, Bucket[] bs, Policy p) {
        long now = System.currentTimeMillis();
        for (int i = 0; i < 3; i++) prepare(bs[i], lims[i], local[i], now, p);
        // pool mode refills tenant and key together inside one transaction
        if (pool != null && "pool".equals(p.getOutageMode()) && bs[TENANT].fallback && bs[KEY].fallback
                && (bs[TENANT].tokens == 0 || bs[KEY].tokens == 0) && now >= bs[TENANT].nextRefill) {
            poolRefill(bs[TENANT], lims[TENANT], bs[KEY], lims[KEY], now);
        }
        return checkAndTake(lims, local, bs, System.currentTimeMillis());
    }

    private int checkAndTake(Limit[] lims, boolean[] local, Bucket[] bs, long now) {
        for (Bucket b : bs) b.lock.lock();
        try {
            for (int i = 0; i < 3; i++) if (!canTake(bs[i], lims[i], local[i], now)) return i;
            for (int i = 0; i < 3; i++) take(bs[i], local[i]);
            for (Bucket b : bs) b.lastUsed = now;
            return -1;
        } finally { for (int i = 2; i >= 0; i--) bs[i].lock.unlock(); }
    }

    /** Makes sure the bucket holds tokens if the shared bucket has any (refill is synchronous: concurrent requests wait for it). */
    private void prepare(Bucket b, Limit lim, boolean local, long now, Policy p) {
        if (local) return;
        b.lock.lock();
        try {
            if (!b.fallback && b.tokens > 0 && now > b.expiresAt) {        // lease expired: give back (drift compensation)
                leader.give(b.key, lim, b.tokens);
                tokensReturned.addAndGet(b.tokens);
                b.tokens = 0;
                b.leaseN = Math.max(1, b.leaseN / 2);
            }
            if (b.tokens > 0 || now < b.nextRefill) return;
            if (b.fallback) { outageRefill(b, lim, now); return; }
            refill(b, lim, now, p);
        } finally { b.lock.unlock(); }
    }

    private long maxLease(Limit lim) { return Math.max(1, Math.min(batchMax, (long) Math.ceil(lim.getCap() / n()))); }
    private long nextLease(Bucket b, Limit lim) { return Math.max(1, Math.min(b.leaseN, maxLease(lim))); }

    /** Leases a batch from the leader. Caller holds b.lock. */
    private void refill(Bucket b, Limit lim, long now, Policy p) {
        long want = nextLease(b, lim);
        boolean grow = b.tokens <= want / 4 && now - b.lastGrant < leaseTtl;
        boolean sparse = now - b.lastGrant > 2 * leaseTtl;
        try {
            rpcCalls.incrementAndGet();
            long g = leader.lease(b.key, lim, want);
            b.everLeased = true;
            if (g == 0) { b.nextRefill = now + 100; return; }       // shared bucket empty: back off
            b.tokens += g;
            b.expiresAt = now + leaseTtl;
            b.lastGrant = now;
            b.leaseN = grow ? want * 2 : sparse ? Math.max(1, want / 2) : want;
        } catch (LeaderClient.NoLeader e) {
            enterFallback(b, lim, now, p.getOutageMode());
        }
    }

    private void enterFallback(Bucket b, Limit lim, long now, String mode) {
        if (b.fallback) return;
        b.fallback = true;
        b.mode = mode;
        b.fbConsumed = 0;
        b.lastFill = now;
        double share = lim.getCap() / n();
        b.localTokens = b.everLeased ? Math.min(b.tokens, share) : share;   // cold start fails open with this node's share
        if ("local".equals(mode)) b.tokens = 0;
        b.nextRefill = now;
        fallbackKeys.incrementAndGet();
        fallbackEvents.incrementAndGet();
    }

    /** Outage refill through mutual exclusion among edge nodes. Caller holds b.lock. */
    private void outageRefill(Bucket b, Limit lim, long now) {
        Outage o = outages.get(b.mode);
        if (o == null) return;                                       // "local" and "pool" handled elsewhere
        try {
            long g = o.take(b.key, lim, Math.max(1, Math.min(nextLease(b, lim), maxLease(lim))));
            b.tokens += g;
            if (g == 0) b.nextRefill = now + 100;
        } catch (LeaderClient.NoLeader e) {
            b.nextRefill = now + 200;
        }
    }

    private void poolRefill(Bucket t, Limit tl, Bucket k, Limit kl, long now) {
        try {
            long g = pool.takeBoth(t.key, tl, k.key, kl, Math.max(1, Math.min(k.leaseN, maxLease(kl))));
            t.lock.lock(); k.lock.lock();
            try {
                t.tokens += g; k.tokens += g;
                if (g == 0) { t.nextRefill = now + 100; }
            } finally { k.lock.unlock(); t.lock.unlock(); }
        } catch (LeaderClient.NoLeader e) {
            t.nextRefill = now + 200;
        }
    }

    private boolean usesLocalShare(Bucket b, boolean local) {
        return local || (b.fallback && ("local".equals(b.mode) || ("pool".equals(b.mode) && b.lvl == IP)));
    }

    /** Caller holds b.lock. */
    private boolean canTake(Bucket b, Limit lim, boolean local, long now) {
        if (usesLocalShare(b, local)) {
            double share = lim.getCap() / n();
            if (b.lastFill == 0) { b.localTokens = share; b.lastFill = now; }
            b.localTokens = Math.min(share, b.localTokens + (now - b.lastFill) / 1000.0 * lim.getRate() / n());
            b.lastFill = now;
            return b.localTokens >= 1;
        }
        return b.tokens > 0;
    }

    private void take(Bucket b, boolean local) {
        if (usesLocalShare(b, local)) b.localTokens -= 1; else b.tokens--;
        if (b.fallback) b.fbConsumed++;
    }

    // ------------------------------------------------------------ strong consistency
    private int allowStrong(String[] keys, Limit[] lims, boolean[] local, Bucket[] bs, Policy p) {
        long now = System.currentTimeMillis();
        if (bs[TENANT].fallback || bs[KEY].fallback) return allowBounded(lims, local, bs, p);   // no leader: outage path
        if (local[IP]) {
            bs[IP].lock.lock();
            try { if (!canTake(bs[IP], lims[IP], true, now)) return IP; } finally { bs[IP].lock.unlock(); }
        }
        int nk = local[IP] ? 2 : 3;
        String[] k = java.util.Arrays.copyOf(keys, nk);
        Limit[] l = java.util.Arrays.copyOf(lims, nk);
        try {
            rpcCalls.incrementAndGet();
            int d = leader.acquire(k, l);
            if (d >= 0) return d;
            if (local[IP]) { bs[IP].lock.lock(); try { take(bs[IP], true); } finally { bs[IP].lock.unlock(); } }
            return -1;
        } catch (LeaderClient.NoLeader e) {
            for (int i = 0; i < nk; i++) { bs[i].lock.lock(); try { enterFallback(bs[i], lims[i], now, p.getOutageMode()); } finally { bs[i].lock.unlock(); } }
            return allowBounded(lims, local, bs, p);
        }
    }

    // ------------------------------------------------------------ eventual consistency
    private int allowEventual(Limit[] lims, boolean[] local, Bucket[] bs) {
        long now = System.currentTimeMillis();
        for (Bucket b : bs) b.lock.lock();
        try {
            for (int i = 0; i < 3; i++) {
                if (local[i]) { if (!canTake(bs[i], lims[i], true, now)) return i; continue; }
                if (Double.isNaN(bs[i].view)) bs[i].view = lims[i].getCap();   // optimistic: assume a full bucket
                if (bs[i].view < 1) return i;
            }
            for (int i = 0; i < 3; i++) {
                if (local[i]) take(bs[i], true);
                else { bs[i].view -= 1; bs[i].consumed++; }
                bs[i].lastUsed = now;
            }
            return -1;
        } finally { for (int i = 2; i >= 0; i--) bs[i].lock.unlock(); }
    }

    /** Eventual consistency sync: push local consumption, pull the global token count. */
    private void syncEventual(Bucket b, Limit lim, long now) {
        long consumed;
        b.lock.lock();
        try { consumed = b.consumed; b.consumed = 0; } finally { b.lock.unlock(); }
        try {
            rpcCalls.incrementAndGet();
            double global = leader.peek(b.key, lim, consumed);
            b.lock.lock();
            try { b.view = global - b.consumed; b.lastSync = now; } finally { b.lock.unlock(); }
        } catch (LeaderClient.NoLeader e) {
            b.lock.lock();
            try { b.consumed += consumed; } finally { b.lock.unlock(); }
        }
    }

    // ------------------------------------------------------------ background sync daemon
    private long lastEventualSync;

    /** One daemon pass: return expired leases, prefetch hot buckets, sync eventual views, recover from outages. */
    public void sweep() {
        Policy p = policy.get();
        long now = System.currentTimeMillis();
        boolean eventual = "eventual".equals(p.getConsistency());
        boolean syncNow = eventual && now - lastEventualSync >= 1000;
        if (syncNow) lastEventualSync = now;
        for (Bucket b : buckets.values()) {
            Limit lim = Policies.level(p, b.lvl);
            if (syncNow && !(b.lvl == IP && p.getIpLocal()) && now - b.lastUsed < 5000) { syncEventual(b, lim, now); continue; }
            b.lock.lock();
            try {
                if (!b.fallback && b.tokens > 0 && now > b.expiresAt) {
                    leader.give(b.key, lim, b.tokens);
                    tokensReturned.addAndGet(b.tokens);
                    b.tokens = 0;
                    b.leaseN = Math.max(1, b.leaseN / 2);
                }
            } finally { b.lock.unlock(); }
            boolean hot = !b.fallback && !eventual && b.leaseN > 1 && b.tokens <= nextLease(b, lim) / 4
                    && now - b.lastUsed < 2 * leaseTtl && now >= b.nextRefill && !(b.lvl == IP && p.getIpLocal());
            if (hot) Thread.startVirtualThread(() -> {
                b.lock.lock();
                try { if (b.tokens <= nextLease(b, lim) / 4) refill(b, lim, System.currentTimeMillis(), p); }
                finally { b.lock.unlock(); }
            });
        }
        if (fallbackKeys.get() > 0 && leader.alive()) recover(p);
    }

    /** Leader is back: charge it for what was admitted locally (drift compensation) and leave fallback. */
    private void recover(Policy p) {
        for (Bucket b : buckets.values()) {
            b.lock.lock();
            try {
                if (!b.fallback) continue;
                Limit lim = Policies.level(p, b.lvl);
                if (b.fbConsumed > 0 && !(b.lvl == IP && p.getIpLocal())) { leader.debt(b.key, lim, b.fbConsumed); reconciled.addAndGet(b.fbConsumed); }
                b.fallback = false;
                b.fbConsumed = 0;
                b.tokens = 0;
                b.nextRefill = 0;
                fallbackKeys.decrementAndGet();
            } finally { b.lock.unlock(); }
        }
    }

    public String mode() {
        return fallbackKeys.get() > 0 ? "fallback" : "normal";
    }
}
