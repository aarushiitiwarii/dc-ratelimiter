package com.rateguard.dc.edge;

import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.PeerMsg;
import com.rateguard.dc.proto.SkRequest;
import com.rateguard.dc.proto.SkToken;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Suzuki-Kasami broadcast mutual exclusion (lab experiment 5, token based).
 *
 * One token per resource; the shared outage bucket travels inside the token, so
 * whoever holds the token has the authoritative state. The token starts at the
 * lowest-id node.
 *   - A node holding the idle token enters the critical section with 0 messages.
 *   - Otherwise it increments RN[self] and broadcasts REQUEST(sn) to N-1 nodes; the
 *     holder sends the token when it is idle and the request is outstanding
 *     (RN[j] = LN[j] + 1).
 *   - On exit: LN[self] = RN[self]; append every node with an outstanding request to
 *     the token queue; pass the token to the head of the queue.
 * Messages per critical section: N (N-1 requests + 1 token), or 0 when already held.
 */
public final class SuzukiKasami implements Limiter.Outage {
    private final PeerNet net;
    private final Map<String, Res> res = new ConcurrentHashMap<>();
    public final AtomicLong csEntries = new AtomicLong(), messages = new AtomicLong();

    private static final class Res {
        final ReentrantLock local = new ReentrantLock();
        final Map<String, Long> rn = new HashMap<>();      // highest request number seen per node
        boolean hasToken, inCs, created;
        Map<String, Long> ln = new HashMap<>();            // token: last served request per node
        Deque<String> queue = new ArrayDeque<>();          // token: waiting nodes
        final PeerNet.Shared bucket = new PeerNet.Shared();
        CompletableFuture<Void> tokenArrived;
    }

    public SuzukiKasami(PeerNet net) { this.net = net; }

    private Res res(String r) { return res.computeIfAbsent(r, k -> new Res()); }

    /** The token for a resource initially belongs to the lowest-id member. Caller holds r's monitor. */
    private void ensureInitialToken(Res r) {
        if (!r.created && net.members().get(0).equals(net.self)) { r.hasToken = true; r.created = true; }
    }

    @Override public long take(String resource, Limit lim, long want) throws LeaderClient.NoLeader {
        Res r = res(resource);
        r.local.lock();
        try {
            CompletableFuture<Void> wait = null;
            long seq = 0;
            synchronized (r) {
                ensureInitialToken(r);
                if (r.hasToken) r.inCs = true;
                else {
                    seq = r.rn.merge(net.self, 1L, Long::sum);
                    wait = r.tokenArrived = new CompletableFuture<>();
                }
            }
            if (wait != null) {
                for (String o : net.others()) send(o, PeerMsg.newBuilder().setResource(resource).setSkRequest(SkRequest.newBuilder().setSeq(seq)));
                try { wait.get(2, TimeUnit.SECONDS); }
                catch (Exception e) { throw new LeaderClient.NoLeader(); }
                synchronized (r) { r.inCs = true; }
            }
            long granted;
            synchronized (r) { granted = r.bucket.take(lim, want, System.currentTimeMillis()); }  // critical section
            csEntries.incrementAndGet();
            release(resource, r);
            return granted;
        } finally { r.local.unlock(); }
    }

    private void release(String resource, Res r) {
        String next = null; PeerMsg.Builder token = null;
        synchronized (r) {
            r.inCs = false;
            r.ln.put(net.self, r.rn.getOrDefault(net.self, 0L));
            for (String j : net.members()) {
                if (j.equals(net.self) || r.queue.contains(j)) continue;
                if (r.rn.getOrDefault(j, 0L) == r.ln.getOrDefault(j, 0L) + 1) r.queue.addLast(j);
            }
            if (!r.queue.isEmpty()) { next = r.queue.pollFirst(); token = tokenMsg(resource, r); r.hasToken = false; }
        }
        if (next != null) send(next, token);
    }

    private PeerMsg.Builder tokenMsg(String resource, Res r) {
        SkToken.Builder t = SkToken.newBuilder().setBucket(r.bucket.proto()).addAllQueue(r.queue);
        for (Map.Entry<String, Long> e : r.ln.entrySet()) { t.addOrder(e.getKey()); t.addLn(e.getValue()); }
        return PeerMsg.newBuilder().setResource(resource).setSkToken(t);
    }

    private void send(String to, PeerMsg.Builder m) { messages.incrementAndGet(); net.send(to, m); }

    /** Handles REQUEST and TOKEN messages. */
    public void onMessage(PeerMsg m) {
        Res r = res(m.getResource());
        if (m.hasSkRequest()) {
            String from = m.getFrom();
            PeerMsg.Builder token = null;
            synchronized (r) {
                r.rn.merge(from, m.getSkRequest().getSeq(), Math::max);
                ensureInitialToken(r);
                if (r.hasToken && !r.inCs && r.rn.get(from) == r.ln.getOrDefault(from, 0L) + 1) {
                    r.queue.remove(from);
                    token = tokenMsg(m.getResource(), r);
                    r.hasToken = false;
                }
            }
            if (token != null) send(from, token);
        } else if (m.hasSkToken()) {
            SkToken t = m.getSkToken();
            boolean waiting;
            synchronized (r) {
                r.hasToken = true; r.created = true;
                r.ln = new HashMap<>();
                for (int i = 0; i < t.getOrderCount(); i++) r.ln.put(t.getOrder(i), t.getLn(i));
                r.queue = new ArrayDeque<>(t.getQueueList());
                if (t.getBucket().getTokens() >= 0) {
                    r.bucket.tokens = t.getBucket().getTokens(); r.bucket.ts = t.getBucket().getTs();
                    r.bucket.version = t.getBucket().getVersion(); r.bucket.init = true;
                }
                waiting = r.tokenArrived != null && !r.tokenArrived.isDone();
                if (waiting) r.tokenArrived.complete(null);
            }
            // nobody here is waiting any more (the request timed out): pass the token on so others do not starve
            if (!waiting) release(m.getResource(), r);
        }
    }
}
