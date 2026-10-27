package com.rateguard.dc.coord;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Policies;
import com.rateguard.dc.common.Ports;
import com.rateguard.dc.proto.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinator replica: the distributed state store that replaces Redis.
 *  - TokenStore service (leader only): Lease / Give / Acquire / Peek (Exp 1 unary),
 *    ReportUsage (Exp 2 client streaming), Control (Exp 2 bidirectional).
 *  - Replication service: Bully election (Exp 9) and Replicate (Exp 2 server streaming),
 *    the leader's bucket-change log streamed to followers.
 */
public final class CoordNode {
    final int id;
    final BucketStore store = new BucketStore();
    final BullyElection election;
    final AtomicReference<Policy> policy = new AtomicReference<>(Policies.defaults());
    private final Map<Integer, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final List<StreamObserver<BucketUpdate>> followers = new CopyOnWriteArrayList<>();
    private final List<StreamObserver<ControlMsg>> controls = new CopyOnWriteArrayList<>();
    private final Map<String, Member> members = new ConcurrentHashMap<>();
    private final BlockingQueue<BucketUpdate> replQueue = new LinkedBlockingQueue<>(200_000);
    final AtomicLong leases = new AtomicLong(), gives = new AtomicLong(), acquires = new AtomicLong(),
            replicated = new AtomicLong(), usageReports = new AtomicLong(), controlMsgs = new AtomicLong();
    private volatile StreamObserver<BucketUpdate> replicaStream; // follower side
    private volatile int replicatingFrom;
    private Server server;

    record Member(String name, String peerAddr, long lastSeen) {}

    public CoordNode(int id) {
        this.id = id;
        this.election = new BullyElection(id, Ports.COORDS, new GrpcTransport());
        store.onChange(u -> { if (election.isLeader() && !followers.isEmpty()) replQueue.offer(u); });
        election.onLeaderChange(l -> {
            Log.info("leader is now %s", l == 0 ? "unknown" : "coord-" + l);
            if (l == id) pushControl();
        });
    }

    // ------------------------------------------------------------ lifecycle
    public void start() throws Exception {
        server = Grpc.newServerBuilderForPort(Ports.coord(id), InsecureServerCredentials.create())
                .addService(new TokenStoreImpl()).addService(new ReplicationImpl())
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build().start();
        Log.info("coord-%d gRPC on :%d", id, Ports.coord(id));
        daemon("repl-fanout", this::fanOut);
        daemon("coord-maint", this::maintenance);
        election.start(300);
    }

    public void blockUntilShutdown() throws InterruptedException { server.awaitTermination(); }

    public void stop() {
        election.stop();
        server.shutdownNow();
        channels.values().forEach(ManagedChannel::shutdownNow);
    }

    private static void daemon(String name, Runnable r) { Thread t = new Thread(r, name); t.setDaemon(true); t.start(); }

    private ManagedChannel channel(int peer) {
        return channels.computeIfAbsent(peer, p -> Grpc.newChannelBuilderForAddress(Ports.HOST, Ports.coord(p), InsecureChannelCredentials.create()).build());
    }

    LeaderInfo leaderInfo() {
        int l = election.leader();
        return LeaderInfo.newBuilder().setLeaderId(l).setLeaderAddr(l == 0 ? "" : Ports.coordAddr(l)).setTerm(election.term()).build();
    }

    // ------------------------------------------------------------ leader -> followers fan-out (server streaming)
    private void fanOut() {
        List<BucketUpdate> batch = new ArrayList<>();
        while (true) {
            try {
                batch.clear();
                batch.add(replQueue.take());
                replQueue.drainTo(batch, 4095);
                for (StreamObserver<BucketUpdate> f : followers) {
                    try {
                        synchronized (f) { for (BucketUpdate u : batch) f.onNext(u); }
                        replicated.addAndGet(batch.size());
                    } catch (Exception e) { followers.remove(f); }
                }
            } catch (InterruptedException e) { return; }
        }
    }

    /** Followers: keep a Replicate stream open to the current leader. Everyone: prune members, expire buckets. */
    private void maintenance() {
        long lastPush = 0;
        while (true) {
            try { Thread.sleep(250); } catch (InterruptedException e) { return; }
            long now = System.currentTimeMillis();
            int l = election.leader();
            if (l != 0 && l != id && (replicaStream == null || replicatingFrom != l)) subscribe(l);
            if (l == id && replicaStream != null) { replicaStream = null; replicatingFrom = 0; }
            members.values().removeIf(m -> now - m.lastSeen() > 3000);
            store.expire(now);
            if (election.isLeader() && now - lastPush > 1000) { pushControl(); lastPush = now; }
        }
    }

    private void subscribe(int leaderId) {
        replicatingFrom = leaderId;
        StreamObserver<BucketUpdate> obs = new StreamObserver<>() {
            public void onNext(BucketUpdate u) {
                if (u.hasPolicy()) policy.set(u.getPolicy());
                else store.apply(u);
                replicated.incrementAndGet();
            }
            public void onError(Throwable t) { if (replicaStream == this) { replicaStream = null; replicatingFrom = 0; } }
            public void onCompleted() { onError(null); }
        };
        replicaStream = obs;
        ReplicationGrpc.newStub(channel(leaderId)).replicate(ElectionMsg.newBuilder().setFrom(id).setTerm(election.term()).build(), obs);
        Log.info("replicating from coord-%d", leaderId);
    }

    // ------------------------------------------------------------ leader -> edges (bidirectional Control stream)
    private ControlMsg controlMsg() {
        List<String> names = new ArrayList<>();
        members.values().forEach(m -> names.add(m.name() + "@" + m.peerAddr()));
        names.sort(null);
        return ControlMsg.newBuilder().setPolicy(policy.get()).addAllMembers(names).setLeader(leaderInfo()).build();
    }

    void pushControl() {
        if (controls.isEmpty()) return;
        ControlMsg msg = controlMsg();
        for (StreamObserver<ControlMsg> c : controls) {
            try { synchronized (c) { c.onNext(msg); } controlMsgs.incrementAndGet(); }
            catch (Exception e) { controls.remove(c); }
        }
    }

    Policy setPolicy(Policy p) {
        Policy cur = policy.get();
        Policy next = p.toBuilder().setVersion(cur.getVersion() + 1).build();
        policy.set(next);
        replQueue.offer(BucketUpdate.newBuilder().setPolicy(next).build());
        Log.info("policy v%d: consistency=%s outage=%s key=%s/%s", next.getVersion(), next.getConsistency(), next.getOutageMode(), next.getKey().getCap(), next.getKey().getRate());
        pushControl();
        return next;
    }

    // ------------------------------------------------------------ TokenStore service
    final class TokenStoreImpl extends TokenStoreGrpc.TokenStoreImplBase {
        @Override public void lease(LeaseRequest r, StreamObserver<LeaseReply> out) {
            if (!election.isLeader()) { out.onNext(LeaseReply.newBuilder().setNotLeader(true).setLeader(leaderInfo()).build()); out.onCompleted(); return; }
            leases.incrementAndGet();
            long g = store.lease(r.getKey(), r.getLimit(), r.getWant(), System.currentTimeMillis());
            out.onNext(LeaseReply.newBuilder().setGranted(g).build());
            out.onCompleted();
        }

        @Override public void give(GiveRequest r, StreamObserver<GiveReply> out) {
            if (!election.isLeader()) { out.onNext(GiveReply.newBuilder().setNotLeader(true).build()); out.onCompleted(); return; }
            gives.incrementAndGet();
            store.give(r.getKey(), r.getLimit(), r.getTokens(), System.currentTimeMillis());
            out.onNext(GiveReply.newBuilder().setOk(true).build());
            out.onCompleted();
        }

        @Override public void acquire(AcquireRequest r, StreamObserver<AcquireReply> out) {
            if (!election.isLeader()) { out.onNext(AcquireReply.newBuilder().setNotLeader(true).build()); out.onCompleted(); return; }
            acquires.incrementAndGet();
            int denied = store.acquire(r.getKeysList().toArray(new String[0]), r.getLimitsList().toArray(new Limit[0]), System.currentTimeMillis());
            out.onNext(AcquireReply.newBuilder().setAllowed(denied < 0).setDeniedLevel(denied).build());
            out.onCompleted();
        }

        @Override public void peek(PeekRequest r, StreamObserver<PeekReply> out) {
            if (!election.isLeader()) { out.onNext(PeekReply.newBuilder().setNotLeader(true).build()); out.onCompleted(); return; }
            leases.incrementAndGet();
            double t = store.charge(r.getKey(), r.getLimit(), r.getConsume(), System.currentTimeMillis());
            out.onNext(PeekReply.newBuilder().setTokens(t).build());
            out.onCompleted();
        }

        @Override public void whoIsLeader(Empty r, StreamObserver<LeaderInfo> out) { out.onNext(leaderInfo()); out.onCompleted(); }

        @Override public void setPolicy(Policy p, StreamObserver<Policy> out) {
            int l = election.leader();
            if (l == id) { out.onNext(CoordNode.this.setPolicy(p)); out.onCompleted(); return; }
            if (l == 0) { out.onError(Status.UNAVAILABLE.withDescription("no leader").asRuntimeException()); return; }
            try {
                out.onNext(TokenStoreGrpc.newBlockingStub(channel(l)).withDeadlineAfter(1, TimeUnit.SECONDS).setPolicy(p));
                out.onCompleted();
            } catch (StatusRuntimeException e) { out.onError(e); }
        }

        @Override public void status(Empty r, StreamObserver<CoordStatus> out) {
            out.onNext(CoordStatus.newBuilder().setId(id).setLeader(election.isLeader()).setLeaderId(election.leader())
                    .setTerm(election.term()).setBuckets(store.size()).setLeases(leases.get()).setGives(gives.get())
                    .setAcquires(acquires.get()).setElections(election.elections()).setReplicated(replicated.get())
                    .setEdges(members.size()).setPolicy(policy.get()).setUsageReports(usageReports.get())
                    .setControlMsgs(controlMsgs.get()).build());
            out.onCompleted();
        }

        /** Exp 2 client streaming: an edge sends many usage reports, the leader answers once. */
        @Override public StreamObserver<UsageReport> reportUsage(StreamObserver<UsageAck> out) {
            return new StreamObserver<>() {
                long reports, returned, debt;
                public void onNext(UsageReport u) {
                    if (!election.isLeader()) return;
                    long now = System.currentTimeMillis();
                    if (u.getReturned() > 0) store.give(u.getKey(), u.getLimit(), u.getReturned(), now);
                    if (u.getDebt() > 0) store.charge(u.getKey(), u.getLimit(), u.getDebt(), now);
                    reports++; returned += u.getReturned(); debt += u.getDebt();
                    usageReports.incrementAndGet();
                }
                public void onError(Throwable t) {}
                public void onCompleted() {
                    out.onNext(UsageAck.newBuilder().setReports(reports).setReturned(returned).setDebt(debt).build());
                    out.onCompleted();
                }
            };
        }

        /** Exp 2 bidirectional: edges stream heartbeats + stats, the leader streams policy + membership. */
        @Override public StreamObserver<EdgeHello> control(StreamObserver<ControlMsg> out) {
            if (!election.isLeader()) {
                out.onNext(ControlMsg.newBuilder().setLeader(leaderInfo()).build());
                out.onCompleted();
                return new StreamObserver<>() { public void onNext(EdgeHello h) {} public void onError(Throwable t) {} public void onCompleted() {} };
            }
            controls.add(out);
            return new StreamObserver<>() {
                boolean first = true;
                public void onNext(EdgeHello h) {
                    members.put(h.getNode(), new Member(h.getNode(), h.getPeerAddr(), System.currentTimeMillis()));
                    if (first) { first = false; pushControl(); }
                }
                public void onError(Throwable t) { controls.remove(out); }
                public void onCompleted() { controls.remove(out); try { out.onCompleted(); } catch (Exception ignored) {} }
            };
        }
    }

    // ------------------------------------------------------------ Replication service
    final class ReplicationImpl extends ReplicationGrpc.ReplicationImplBase {
        @Override public void election(ElectionMsg m, StreamObserver<ElectionReply> out) {
            boolean ok = election.onElection(m.getFrom(), m.getTerm());
            out.onNext(ElectionReply.newBuilder().setOk(ok).setLeaderId(election.leader()).setTerm(election.term()).build());
            out.onCompleted();
        }

        @Override public void coordinator(ElectionMsg m, StreamObserver<Empty> out) {
            election.onCoordinator(m.getFrom(), m.getTerm());
            out.onNext(Empty.getDefaultInstance());
            out.onCompleted();
        }

        @Override public void heartbeat(ElectionMsg m, StreamObserver<ElectionReply> out) {
            out.onNext(ElectionReply.newBuilder().setOk(election.onHeartbeat(m.getFrom())).setLeaderId(election.leader()).setTerm(election.term()).build());
            out.onCompleted();
        }

        /** Exp 2 server streaming: snapshot first, then every change, until the follower disconnects. */
        @Override public void replicate(ElectionMsg m, StreamObserver<BucketUpdate> out) {
            if (!election.isLeader()) { out.onCompleted(); return; }
            synchronized (out) {
                out.onNext(BucketUpdate.newBuilder().setPolicy(policy.get()).build());
                for (BucketUpdate u : store.snapshot()) out.onNext(u);
            }
            followers.add(out);
            Log.info("coord-%d subscribed to the replication stream", m.getFrom());
        }
    }

    // ------------------------------------------------------------ gRPC transport for the Bully algorithm
    final class GrpcTransport implements BullyElection.Transport {
        private ReplicationGrpc.ReplicationBlockingStub stub(int to) {
            return ReplicationGrpc.newBlockingStub(channel(to)).withDeadlineAfter(300, TimeUnit.MILLISECONDS);
        }
        public boolean election(int to, int from, long term) {
            try { return stub(to).election(ElectionMsg.newBuilder().setFrom(from).setTerm(term).build()).getOk(); }
            catch (StatusRuntimeException e) { return false; }
        }
        public void coordinator(int to, int from, long term) {
            try { stub(to).coordinator(ElectionMsg.newBuilder().setFrom(from).setTerm(term).build()); }
            catch (StatusRuntimeException ignored) {}
        }
        public boolean heartbeat(int to, int from, long term) {
            try { return stub(to).heartbeat(ElectionMsg.newBuilder().setFrom(from).setTerm(term).build()).getOk(); }
            catch (StatusRuntimeException e) { return false; }
        }
        public long termOf(int to) {
            try { return stub(to).heartbeat(ElectionMsg.newBuilder().setFrom(id).setTerm(0).build()).getTerm(); }
            catch (StatusRuntimeException e) { return -1; }
        }
    }

    public static void main(int id) throws Exception {
        Log.setName("coord-" + id);
        CoordNode n = new CoordNode(id);
        n.start();
        Runtime.getRuntime().addShutdownHook(new Thread(n::stop));
        n.blockUntilShutdown();
    }
}
