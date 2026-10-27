package com.rateguard.dc.edge;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.PeerMsg;
import com.rateguard.dc.proto.RaReply;
import com.rateguard.dc.proto.RaRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Ricart-Agrawala mutual exclusion (lab experiment 4, non-token based).
 *
 * Used while no coordinator leader exists: each edge keeps a replica of the
 * shared outage bucket for a resource (a tenant or API key). To take tokens a node
 * enters the critical section:
 *   1. increment its Lamport clock and send REQUEST(ts) to the other N-1 nodes,
 *   2. wait for N-1 REPLY messages,
 *   3. update the bucket (refill + take), 4. send the deferred replies.
 * A node receiving REQUEST replies at once unless it is itself requesting with a
 * smaller (ts, id); then it defers the reply until it leaves the critical section.
 * Replies carry the sender's bucket replica, so the next holder always sees the
 * newest state, and echo the request's timestamp so a late reply to an abandoned
 * request is not counted for the next one. Messages per critical section: 2(N-1).
 */
public final class RicartAgrawala implements Limiter.Outage {
    private final PeerNet net;
    private long clock;                                  // Lamport clock (guarded by this)
    private final Map<String, Res> res = new ConcurrentHashMap<>();
    public final AtomicLong csEntries = new AtomicLong(), messages = new AtomicLong(), timeouts = new AtomicLong();

    private static final class Res {
        final ReentrantLock local = new ReentrantLock();     // one local requester at a time
        final ReentrantLock mu = new ReentrantLock();        // guards the fields below (no synchronized: it pins virtual threads)
        final Condition replied = mu.newCondition();
        final PeerNet.Shared bucket = new PeerNet.Shared();
        final Map<String, Long> deferred = new HashMap<>();  // peer -> timestamp of its deferred request
        final Set<String> waiting = new HashSet<>();         // peers whose REPLY is still missing
        boolean requesting; long myTs;
    }

    public RicartAgrawala(PeerNet net) { this.net = net; }

    private Res res(String r) { return res.computeIfAbsent(r, k -> new Res()); }

    @Override public long take(String resource, Limit lim, long want) throws LeaderClient.NoLeader {
        Res r = res(resource);
        r.local.lock();
        try {
            List<String> others = net.others();
            long ts;
            synchronized (this) { ts = ++clock; }
            r.mu.lock(); try { r.requesting = true; r.myTs = ts; r.waiting.clear(); r.waiting.addAll(others); } finally { r.mu.unlock(); }
            for (String o : others) send(o, PeerMsg.newBuilder().setResource(resource).setRaRequest(RaRequest.newBuilder().setClock(ts)));
            boolean all;
            r.mu.lock(); try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                try {
                    while (!r.waiting.isEmpty() && System.nanoTime() < deadline) r.replied.awaitNanos(deadline - System.nanoTime());
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                all = r.waiting.isEmpty();
                if (!all && timeouts.incrementAndGet() % 20 == 1) Log.info("RA %s: no reply from %s within 2 s", resource, r.waiting);
            } finally { r.mu.unlock(); }
            long granted = 0;
            if (all) {
                r.mu.lock(); try { granted = r.bucket.take(lim, want, System.currentTimeMillis()); } finally { r.mu.unlock(); }   // critical section
                csEntries.incrementAndGet();
            }
            Map<String, Long> def;
            r.mu.lock(); try { r.requesting = false; r.waiting.clear(); def = new HashMap<>(r.deferred); r.deferred.clear(); } finally { r.mu.unlock(); }
            def.forEach((d, dts) -> reply(d, resource, dts, r));
            if (!all) throw new LeaderClient.NoLeader();       // a peer did not answer: give up this round
            return granted;
        } finally { r.local.unlock(); }
    }

    private void send(String to, PeerMsg.Builder m) { messages.incrementAndGet(); net.send(to, m); }

    private void reply(String to, String resource, long reqTs, Res r) {
        PeerMsg.Builder m;
        r.mu.lock(); try { m = PeerMsg.newBuilder().setResource(resource).setRaReply(RaReply.newBuilder().setBucket(r.bucket.proto()).setClock(reqTs)); } finally { r.mu.unlock(); }
        send(to, m);
    }

    /** Handles REQUEST and REPLY messages. */
    public void onMessage(PeerMsg m) {
        Res r = res(m.getResource());
        if (m.hasRaRequest()) {
            long ts = m.getRaRequest().getClock();
            boolean defer;
            synchronized (this) { clock = Math.max(clock, ts) + 1; }
            r.mu.lock(); try {
                // defer when our own pending request has priority: (myTs, self) < (ts, from)
                defer = r.requesting && (r.myTs < ts || (r.myTs == ts && PeerNet.idOf(net.self) < PeerNet.idOf(m.getFrom())));
                if (defer) r.deferred.put(m.getFrom(), ts);
            } finally { r.mu.unlock(); }
            if (!defer) reply(m.getFrom(), m.getResource(), ts, r);
        } else if (m.hasRaReply()) {
            r.mu.lock(); try {
                r.bucket.adoptIfNewer(m.getRaReply().getBucket());
                if (r.requesting && m.getRaReply().getClock() == r.myTs && r.waiting.remove(m.getFrom())) r.replied.signalAll();
            } finally { r.mu.unlock(); }
        }
    }
}
