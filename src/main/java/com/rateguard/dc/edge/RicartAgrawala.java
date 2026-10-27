package com.rateguard.dc.edge;

import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.PeerMsg;
import com.rateguard.dc.proto.RaReply;
import com.rateguard.dc.proto.RaRequest;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
 * newest state. Messages per critical section: 2(N-1).
 */
public final class RicartAgrawala implements Limiter.Outage {
    private final PeerNet net;
    private long clock;                                  // Lamport clock (guarded by this)
    private final Map<String, Res> res = new ConcurrentHashMap<>();
    public final AtomicLong csEntries = new AtomicLong(), messages = new AtomicLong();

    private static final class Res {
        final ReentrantLock local = new ReentrantLock();     // one local requester at a time
        final PeerNet.Shared bucket = new PeerNet.Shared();
        final Set<String> deferred = new HashSet<>();
        boolean requesting; long myTs;
        CountDownLatch replies = new CountDownLatch(0);
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
            synchronized (r) { r.requesting = true; r.myTs = ts; r.replies = new CountDownLatch(others.size()); }
            for (String o : others) send(o, PeerMsg.newBuilder().setResource(resource).setRaRequest(RaRequest.newBuilder().setClock(ts)));
            boolean all;
            try { all = r.replies.await(2, TimeUnit.SECONDS); }
            catch (InterruptedException e) { Thread.currentThread().interrupt(); all = false; }
            long granted = 0;
            if (all) {
                synchronized (r) { granted = r.bucket.take(lim, want, System.currentTimeMillis()); }   // critical section
                csEntries.incrementAndGet();
            }
            List<String> def;
            synchronized (r) { r.requesting = false; def = new ArrayList<>(r.deferred); r.deferred.clear(); }
            for (String d : def) reply(d, resource, r);
            if (!all) throw new LeaderClient.NoLeader();       // a peer did not answer: give up this round
            return granted;
        } finally { r.local.unlock(); }
    }

    private void send(String to, PeerMsg.Builder m) { messages.incrementAndGet(); net.send(to, m); }

    private void reply(String to, String resource, Res r) {
        PeerMsg.Builder m;
        synchronized (r) { m = PeerMsg.newBuilder().setResource(resource).setRaReply(RaReply.newBuilder().setBucket(r.bucket.proto())); }
        send(to, m);
    }

    /** Handles REQUEST and REPLY messages. */
    public void onMessage(PeerMsg m) {
        Res r = res(m.getResource());
        if (m.hasRaRequest()) {
            long ts = m.getRaRequest().getClock();
            boolean defer;
            synchronized (this) { clock = Math.max(clock, ts) + 1; }
            synchronized (r) {
                // defer when our own pending request has priority: (myTs, self) < (ts, from)
                defer = r.requesting && (r.myTs < ts || (r.myTs == ts && PeerNet.idOf(net.self) < PeerNet.idOf(m.getFrom())));
                if (defer) r.deferred.add(m.getFrom());
            }
            if (!defer) reply(m.getFrom(), m.getResource(), r);
        } else if (m.hasRaReply()) {
            synchronized (r) { r.bucket.adoptIfNewer(m.getRaReply().getBucket()); }
            r.replies.countDown();
        }
    }
}
