package com.rateguard.dc.edge;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * The edge node's link to the coordinator cluster.
 *  - Unary RPCs (Exp 1): Lease, Acquire, Peek; follows not_leader redirects and
 *    discovers the leader with WhoIsLeader.
 *  - Client streaming (Exp 2): queued give-backs and outage debts are flushed to the
 *    leader in batches over one ReportUsage stream (the background batch sync).
 *  - Bidirectional streaming (Exp 2): the Control stream sends heartbeats + stats up
 *    and receives policy changes + cluster membership down.
 */
public final class LeaderClient implements Limiter.Leader {
    public static final class NoLeader extends Exception {
        public NoLeader() { super("no coordinator leader reachable", null, false, false); }
    }

    private final String node, peerAddr;
    private final Map<Integer, ManagedChannel> ch = new ConcurrentHashMap<>();
    private volatile int leader;              // 0 = unknown
    private volatile long lastDiscoveryFail;
    private final BlockingQueue<UsageReport> usage = new LinkedBlockingQueue<>(100_000);
    public final AtomicLong rpcCalls = new AtomicLong(), usageBatches = new AtomicLong(), controlMsgs = new AtomicLong();
    private final Consumer<ControlMsg> onControl;
    private final LongSupplier allowedCount, deniedCount;
    private static final long RPC_DEADLINE_MS = 250;

    public LeaderClient(String node, String peerAddr, Consumer<ControlMsg> onControl, LongSupplier allowed, LongSupplier denied) {
        this.node = node; this.peerAddr = peerAddr; this.onControl = onControl;
        this.allowedCount = allowed; this.deniedCount = denied;
    }

    public void start() {
        daemon("usage-flush", this::flushLoop);
        daemon("control-stream", this::controlLoop);
    }

    private static void daemon(String n, Runnable r) { Thread t = new Thread(r, n); t.setDaemon(true); t.start(); }

    private ManagedChannel channel(int id) {
        return ch.computeIfAbsent(id, i -> Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.coord(i), InsecureChannelCredentials.create()).build());
    }

    private TokenStoreGrpc.TokenStoreBlockingStub stub(int id) {
        return TokenStoreGrpc.newBlockingStub(channel(id)).withDeadlineAfter(RPC_DEADLINE_MS, TimeUnit.MILLISECONDS);
    }

    public int leaderId() { return leader; }

    /** Asks every coordinator who the leader is. Rate-limited so an outage does not cause a storm. */
    private int discover() throws NoLeader {
        long now = System.currentTimeMillis();
        if (now - lastDiscoveryFail < 300) throw new NoLeader();
        for (int i = 1; i <= Ports.COORDS; i++) {
            try {
                rpcCalls.incrementAndGet();
                LeaderInfo li = TokenStoreGrpc.newBlockingStub(channel(i)).withDeadlineAfter(150, TimeUnit.MILLISECONDS).whoIsLeader(Empty.getDefaultInstance());
                if (li.getLeaderId() != 0) { leader = li.getLeaderId(); return leader; }
            } catch (StatusRuntimeException ignored) {}
        }
        lastDiscoveryFail = System.currentTimeMillis();
        throw new NoLeader();
    }

    private interface Call<T> { T run(TokenStoreGrpc.TokenStoreBlockingStub s); }
    private interface NotLeader<T> { boolean test(T reply); }

    private <T> T call(Call<T> c, NotLeader<T> notLeader) throws NoLeader {
        for (int attempt = 0; attempt < 3; attempt++) {
            int l = leader != 0 ? leader : discover();
            try {
                rpcCalls.incrementAndGet();
                T r = c.run(stub(l));
                if (notLeader.test(r)) { leader = 0; continue; }
                return r;
            } catch (StatusRuntimeException e) {
                leader = 0;
            }
        }
        throw new NoLeader();
    }

    @Override public long lease(String key, Limit lim, long want) throws NoLeader {
        LeaseReply r = call(s -> s.lease(LeaseRequest.newBuilder().setKey(key).setLimit(lim).setWant(want).setNode(node).build()), LeaseReply::getNotLeader);
        return r.getGranted();
    }

    @Override public int acquire(String[] keys, Limit[] lims) throws NoLeader {
        AcquireRequest.Builder b = AcquireRequest.newBuilder();
        for (int i = 0; i < keys.length; i++) { b.addKeys(keys[i]); b.addLimits(lims[i]); }
        AcquireReply r = call(s -> s.acquire(b.build()), AcquireReply::getNotLeader);
        return r.getAllowed() ? -1 : r.getDeniedLevel();
    }

    @Override public double peek(String key, Limit lim, long consume) throws NoLeader {
        PeekReply r = call(s -> s.peek(PeekRequest.newBuilder().setKey(key).setLimit(lim).setConsume(consume).build()), PeekReply::getNotLeader);
        return r.getTokens();
    }

    @Override public void give(String key, Limit lim, long n) {
        if (n > 0) usage.offer(UsageReport.newBuilder().setNode(node).setKey(key).setLimit(lim).setReturned(n).build());
    }

    @Override public void debt(String key, Limit lim, long n) {
        if (n > 0) usage.offer(UsageReport.newBuilder().setNode(node).setKey(key).setLimit(lim).setDebt(n).build());
    }

    @Override public boolean alive() {
        if (leader != 0) return true;
        try { discover(); return true; } catch (NoLeader e) { return false; }
    }

    // ---- Exp 2 client streaming: flush queued usage in one stream per batch
    private void flushLoop() {
        List<UsageReport> batch = new ArrayList<>();
        while (true) {
            try {
                batch.clear();
                batch.add(usage.take());
                Thread.sleep(150);                       // let more reports accumulate
                usage.drainTo(batch, 10_000);
                int l = leader != 0 ? leader : safeDiscover();
                if (l == 0) continue;                   // no leader: drop (best effort, like expired leases)
                CountDownLatch done = new CountDownLatch(1);
                StreamObserver<UsageReport> up = TokenStoreGrpc.newStub(channel(l)).withDeadlineAfter(2, TimeUnit.SECONDS)
                        .reportUsage(new StreamObserver<>() {
                            public void onNext(UsageAck a) {}
                            public void onError(Throwable t) { done.countDown(); }
                            public void onCompleted() { done.countDown(); }
                        });
                for (UsageReport u : batch) up.onNext(u);
                up.onCompleted();
                usageBatches.incrementAndGet();
                rpcCalls.incrementAndGet();
                done.await(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) { return; }
            catch (Exception e) { /* keep flushing */ }
        }
    }

    private int safeDiscover() { try { return discover(); } catch (NoLeader e) { return 0; } }

    // ---- Exp 2 bidirectional: Control stream with the leader
    private void controlLoop() {
        while (true) {
            try {
                int l = leader != 0 ? leader : safeDiscover();
                if (l == 0) { Thread.sleep(500); continue; }
                CountDownLatch closed = new CountDownLatch(1);
                StreamObserver<EdgeHello> up = TokenStoreGrpc.newStub(channel(l)).control(new StreamObserver<>() {
                    public void onNext(ControlMsg m) {
                        controlMsgs.incrementAndGet();
                        if (m.hasLeader() && m.getLeader().getLeaderId() != 0) leader = m.getLeader().getLeaderId();
                        if (m.hasPolicy()) onControl.accept(m);
                    }
                    public void onError(Throwable t) { closed.countDown(); }
                    public void onCompleted() { closed.countDown(); }
                });
                while (closed.getCount() > 0) {
                    up.onNext(EdgeHello.newBuilder().setNode(node).setPeerAddr(peerAddr)
                            .setAllowed(allowedCount.getAsLong()).setDenied(deniedCount.getAsLong()).build());
                    if (closed.await(1, TimeUnit.SECONDS)) break;
                }
                try { up.onCompleted(); } catch (Exception ignored) {}
                Thread.sleep(200);
            } catch (InterruptedException e) { return; }
            catch (Exception e) {
                Log.info("control stream error: %s", e.getMessage());
                try { Thread.sleep(500); } catch (InterruptedException ie) { return; }
            }
        }
    }
}
