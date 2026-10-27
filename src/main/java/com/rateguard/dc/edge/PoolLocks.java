package com.rateguard.dc.edge;

import com.rateguard.dc.proto.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * Pool outage mode with Chandy-Misra-Haas deadlock detection (lab experiment 6).
 *
 * While no coordinator leader exists, every tenant pool and key pool is homed on
 * one edge (hash of its name). A refill is a transaction that locks BOTH pools
 * (tenant and key) on their home nodes, takes tokens from both, then releases them.
 *
 * With lock ordering on, every node locks pools in name order: no cycles, no deadlock.
 * With lock ordering off, even and odd nodes lock in opposite orders, so two
 * transactions can each hold one pool and wait for the other: a deadlock.
 *
 * Detection (edge chasing, AND model): a node blocked longer than 200 ms sends
 * PROBE(initiator, path) to the node holding the lock it waits for. A blocked
 * receiver forwards the probe to the node it waits for; an idle receiver drops it.
 * If a probe returns to its initiator there is a cycle. Resolution: the youngest
 * transaction on the cycle aborts (releases its locks, backs off, retries).
 */
public final class PoolLocks implements Limiter.PoolOutage {
    private final PeerNet net;
    private final Supplier<Policy> policy;

    // ---- home side: lock table for the pools homed on this node
    private record Waiter(String node, long txn) {}
    private static final class Home {
        String holder; long holderTxn;
        final Deque<Waiter> queue = new ArrayDeque<>();
        final PeerNet.Shared bucket = new PeerNet.Shared();
    }
    private final Map<String, Home> homes = new ConcurrentHashMap<>();

    // ---- requester side: one transaction at a time per node
    private final ReentrantLock txnLock = new ReentrantLock();
    private long txnSeq;
    private volatile long curTxn, curStarted;
    private volatile String waitingFor, grantRes;
    private volatile CompletableFuture<SharedBucket> grant;
    private volatile boolean abort;

    public final AtomicLong probes = new AtomicLong(), deadlocks = new AtomicLong(), aborts = new AtomicLong(),
            csEntries = new AtomicLong(), messages = new AtomicLong();

    private static final class Aborted extends Exception { Aborted() { super(null, null, false, false); } }

    public PoolLocks(PeerNet net, Supplier<Policy> policy) { this.net = net; this.policy = policy; }

    String homeOf(String resource) {
        List<String> m = net.members();
        return m.get(Math.floorMod(resource.hashCode(), m.size()));
    }

    private void send(String to, PeerMsg.Builder m) { if (!to.equals(net.self)) messages.incrementAndGet(); net.send(to, m); }

    @Override public long takeBoth(String r1, Limit l1, String r2, Limit l2, long want) throws LeaderClient.NoLeader {
        txnLock.lock();
        try {
            long txn = ++txnSeq;
            curTxn = txn; curStarted = System.currentTimeMillis(); abort = false;
            String[] order;
            if (policy.get().getLockOrdering()) order = r1.compareTo(r2) < 0 ? new String[]{r1, r2} : new String[]{r2, r1};
            else order = PeerNet.idOf(net.self) % 2 == 0 ? new String[]{r2, r1} : new String[]{r1, r2};   // crossing order
            Map<String, Limit> lim = Map.of(r1, l1, r2, l2);
            List<String> held = new ArrayList<>();
            try {
                Map<String, PeerNet.Shared> s = new HashMap<>();
                for (String r : order) { s.put(r, toShared(acquire(r, txn))); held.add(r); }
                // critical section: both pools locked
                long now = System.currentTimeMillis();
                PeerNet.Shared a = s.get(r1), b = s.get(r2);
                a.ensure(l1, now); b.ensure(l2, now);
                long ga = a.take(l1, want, now);                                 // refill + take
                long g = Math.min(ga, b.take(l2, ga, now));
                if (g < ga) a.tokens += ga - g;                                  // keep both pools equal
                csEntries.incrementAndGet();
                for (String r : held) release(r, s.get(r), txn);
                return g;
            } catch (Aborted e) {
                aborts.incrementAndGet();
                for (String r : held) release(r, null, txn);
                try { Thread.sleep(ThreadLocalRandom.current().nextLong(20, 80)); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                return 0;
            } finally {
                waitingFor = null; curTxn = 0; grant = null; grantRes = null;
            }
        } finally { txnLock.unlock(); }
    }

    private static PeerNet.Shared toShared(SharedBucket b) {
        PeerNet.Shared s = new PeerNet.Shared();
        if (b.getTokens() >= 0) { s.tokens = b.getTokens(); s.ts = b.getTs(); s.version = b.getVersion(); s.init = true; }
        return s;
    }

    private SharedBucket acquire(String resource, long txn) throws Aborted {
        CompletableFuture<SharedBucket> f = new CompletableFuture<>();
        grant = f; grantRes = resource; waitingFor = null;
        String home = homeOf(resource);
        send(home, PeerMsg.newBuilder().setResource(resource).setLockRequest(LockRequest.newBuilder().setTxn(txn).setStarted(curStarted)));
        long start = System.currentTimeMillis(), lastProbe = start;
        while (true) {
            try { return f.get(50, TimeUnit.MILLISECONDS); }
            catch (TimeoutException e) {
                long now = System.currentTimeMillis();
                if (abort || now - start > 5000) {
                    send(home, PeerMsg.newBuilder().setResource(resource).setLockAbort(LockAbort.newBuilder().setTxn(txn)));
                    throw new Aborted();
                }
                String w = waitingFor;
                if (w != null && !w.equals(net.self) && now - lastProbe >= 200) {        // blocked: start edge chasing
                    lastProbe = now;
                    probes.incrementAndGet();
                    send(w, PeerMsg.newBuilder().setProbe(CmhProbe.newBuilder().setInitiator(net.self).setInitiatorTxn(txn)
                            .addPath(net.self).addStarted(curStarted)));
                }
            } catch (Exception e) { throw new Aborted(); }
        }
    }

    private void release(String resource, PeerNet.Shared s, long txn) {
        LockRelease.Builder r = LockRelease.newBuilder().setTxn(txn)
                .setBucket(s != null ? s.proto() : SharedBucket.newBuilder().setTokens(-1).build());
        send(homeOf(resource), PeerMsg.newBuilder().setResource(resource).setLockRelease(r));
    }

    // ------------------------------------------------------------ message handling
    public void onMessage(PeerMsg m) {
        String res = m.getResource(), from = m.getFrom();
        switch (m.getBodyCase()) {
            case LOCK_REQUEST -> {
                Home h = homes.computeIfAbsent(res, k -> new Home());
                long txn = m.getLockRequest().getTxn();
                PeerMsg.Builder reply;
                synchronized (h) {
                    if (h.holder == null) {
                        h.holder = from; h.holderTxn = txn;
                        reply = PeerMsg.newBuilder().setResource(res).setLockGrant(LockGrant.newBuilder().setTxn(txn).setBucket(h.bucket.proto()));
                    } else {
                        h.queue.addLast(new Waiter(from, txn));
                        reply = PeerMsg.newBuilder().setResource(res).setLockQueued(LockQueued.newBuilder().setHolder(h.holder).setHolderTxn(h.holderTxn));
                    }
                }
                send(from, reply);
            }
            case LOCK_RELEASE -> {
                Home h = homes.computeIfAbsent(res, k -> new Home());
                SharedBucket b = m.getLockRelease().getBucket();
                handOver(res, h, from, m.getLockRelease().getTxn(), b);
            }
            case LOCK_ABORT -> {
                Home h = homes.computeIfAbsent(res, k -> new Home());
                long txn = m.getLockAbort().getTxn();
                boolean wasHolder;
                synchronized (h) {
                    h.queue.removeIf(w -> w.node().equals(from) && w.txn() == txn);
                    wasHolder = from.equals(h.holder) && h.holderTxn == txn;
                }
                if (wasHolder) handOver(res, h, from, txn, SharedBucket.newBuilder().setTokens(-1).build());
            }
            case LOCK_GRANT -> {
                long txn = m.getLockGrant().getTxn();
                CompletableFuture<SharedBucket> g = grant;
                if (g != null && res.equals(grantRes) && txn == curTxn && !abort) g.complete(m.getLockGrant().getBucket());
                else release(res, null, txn);    // grant for a transaction that already aborted: give it back
            }
            case LOCK_QUEUED -> {
                if (res.equals(grantRes)) waitingFor = m.getLockQueued().getHolder();
            }
            case PROBE -> onProbe(m.getProbe());
            default -> {}
        }
    }

    private void handOver(String res, Home h, String from, long txn, SharedBucket updated) {
        Waiter next = null;
        PeerMsg.Builder g = null;
        synchronized (h) {
            if (!(from.equals(h.holder) && h.holderTxn == txn)) return;
            if (updated.getTokens() >= 0) { h.bucket.tokens = updated.getTokens(); h.bucket.ts = updated.getTs(); h.bucket.version = updated.getVersion(); h.bucket.init = true; }
            next = h.queue.pollFirst();
            if (next == null) { h.holder = null; h.holderTxn = 0; }
            else {
                h.holder = next.node(); h.holderTxn = next.txn();
                g = PeerMsg.newBuilder().setResource(res).setLockGrant(LockGrant.newBuilder().setTxn(next.txn()).setBucket(h.bucket.proto()));
            }
        }
        if (next != null) send(next.node(), g);
    }

    /** Chandy-Misra-Haas probe handling. */
    private void onProbe(CmhProbe p) {
        if (p.getInitiator().equals(net.self)) {
            if (p.getInitiatorTxn() != curTxn || waitingFor == null) return;   // stale: we are no longer blocked
            // cycle found: the youngest transaction on it (latest start, ties by higher id) aborts
            String victim = net.self; long youngest = curStarted;
            for (int i = 0; i < p.getPathCount(); i++) {
                long st = p.getStarted(i); String n = p.getPath(i);
                if (st > youngest || (st == youngest && PeerNet.idOf(n) > PeerNet.idOf(victim))) { youngest = st; victim = n; }
            }
            if (victim.equals(net.self) && !abort) { abort = true; deadlocks.incrementAndGet(); }
            return;
        }
        String w = waitingFor;
        if (w == null || p.getPathList().contains(net.self)) return;          // not blocked (or already on the path): drop
        probes.incrementAndGet();
        send(w, PeerMsg.newBuilder().setProbe(p.toBuilder().addPath(net.self).addStarted(curStarted)));
    }
}
