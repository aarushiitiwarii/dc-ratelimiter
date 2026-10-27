package com.rateguard.dc.coord;

import com.rateguard.dc.common.Log;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;

/**
 * Bully leader election (lab experiment 9) among coordinator replicas 1..n.
 *
 *  - A node that notices there is no leader sends ELECTION to every node with a
 *    higher id. If none answers OK it wins and announces COORDINATOR to all.
 *    If a higher node answers, that node takes over the election.
 *  - A node that receives ELECTION from a lower id answers OK and starts its own election.
 *  - Followers heartbeat the leader; two missed heartbeats start a new election.
 *  - When a higher-id node comes back, it "bullies" its way to leadership.
 */
public final class BullyElection {

    /** Message transport between replicas (gRPC in production, in-memory in tests). */
    public interface Transport {
        /** Sends ELECTION; true when the peer answered OK, false when unreachable. */
        boolean election(int to, int from, long term);
        /** Sends COORDINATOR (best effort). */
        void coordinator(int to, int from, long term);
        /** Heartbeat to the leader; true when it is alive and still leader. */
        boolean heartbeat(int to, int from, long term);
        /** The peer's current term, or -1 when unreachable (so a returning node never reuses an old term). */
        long termOf(int to);
    }

    private final int id, n;
    private final Transport t;
    private final ExecutorService worker = Executors.newSingleThreadExecutor(r -> { Thread th = new Thread(r, "bully"); th.setDaemon(true); return th; });
    private final Object announce = new Object();
    private volatile int leader;            // 0 = unknown
    private volatile long term;
    private volatile boolean electing;
    private volatile boolean running = true;
    private int missed;
    private final AtomicLong elections = new AtomicLong(), messages = new AtomicLong();
    private volatile IntConsumer onLeaderChange = l -> {};

    public BullyElection(int id, int n, Transport t) { this.id = id; this.n = n; this.t = t; }

    public void onLeaderChange(IntConsumer c) { onLeaderChange = c; }
    public int leader() { return leader; }
    public long term() { return term; }
    public boolean isLeader() { return leader == id; }
    public long elections() { return elections.get(); }
    public long messages() { return messages.get(); }

    /** Starts the failure detector (heartbeats every {@code periodMs}). */
    public void start(long periodMs) {
        Thread th = new Thread(() -> {
            triggerElection();
            while (running) {
                try { Thread.sleep(periodMs); } catch (InterruptedException e) { return; }
                tick();
            }
        }, "bully-monitor");
        th.setDaemon(true);
        th.start();
    }

    public void stop() { running = false; worker.shutdownNow(); }

    /** One failure-detector step (public for tests). */
    public void tick() {
        int l = leader;
        if (l == id) return;
        if (l == 0) { triggerElection(); return; }
        messages.incrementAndGet();
        if (t.heartbeat(l, id, term)) { missed = 0; return; }
        if (++missed >= 2) {
            Log.info("leader coord-%d missed %d heartbeats -> election", l, missed);
            missed = 0;
            setLeader(0, term);
            triggerElection();
        }
    }

    public void triggerElection() {
        if (electing || !running) return;
        electing = true;
        worker.execute(this::runElection);
    }

    private void runElection() {
        try {
            for (int attempt = 0; attempt < 5 && running; attempt++) {
                elections.incrementAndGet();
                boolean higherAlive = false;
                for (int h = id + 1; h <= n; h++) {
                    messages.incrementAndGet();
                    if (t.election(h, id, term)) higherAlive = true;
                }
                if (!higherAlive) { becomeLeader(); return; }
                // a higher node took over: wait for its COORDINATOR announcement
                synchronized (announce) {
                    long deadline = System.currentTimeMillis() + 2000;
                    while (leader == 0 && System.currentTimeMillis() < deadline) {
                        try { announce.wait(100); } catch (InterruptedException e) { return; }
                    }
                }
                if (leader != 0) return;
            }
        } finally { electing = false; }
    }

    private void becomeLeader() {
        long highest = term;
        for (int o = 1; o <= n; o++) if (o != id) { messages.incrementAndGet(); highest = Math.max(highest, t.termOf(o)); }
        long newTerm = highest + 1;
        setLeader(id, newTerm);
        Log.info("won election, coord-%d is leader (term %d)", id, newTerm);
        for (int o = 1; o <= n; o++) if (o != id) { messages.incrementAndGet(); t.coordinator(o, id, newTerm); }
    }

    private void setLeader(int l, long tm) {
        int old = leader;
        leader = l;
        term = Math.max(term, tm);
        synchronized (announce) { announce.notifyAll(); }
        if (old != l) onLeaderChange.accept(l);
    }

    // ---- incoming messages ----

    /** ELECTION from a lower id: answer OK and take over. */
    public boolean onElection(int from, long fromTerm) {
        if (from < id) { triggerElection(); return true; }
        return false;
    }

    /** COORDINATOR announcement. */
    public void onCoordinator(int from, long fromTerm) {
        if (from < id && running) {
            // a lower node claims leadership while we are alive: bully it
            triggerElection();
            return;
        }
        missed = 0;
        setLeader(from, fromTerm);
    }

    /** Heartbeat from a follower: true while this node is the leader. */
    public boolean onHeartbeat(int from) { return isLeader(); }

    public static void shutdown(ExecutorService e) {
        e.shutdownNow();
        try { e.awaitTermination(1, TimeUnit.SECONDS); } catch (InterruptedException ignored) {}
    }
}
