package com.rateguard.dc.edge;

import com.rateguard.dc.common.Policies;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.PeerMsg;
import com.rateguard.dc.proto.Policy;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Ricart-Agrawala, Suzuki-Kasami and Chandy-Misra-Haas over an in-memory network of N edges. */
class DistributedAlgorithmsTest {

    static final class Node {
        PeerNet net; RicartAgrawala ra; SuzukiKasami sk; PoolLocks pool;
    }

    static List<Node> cluster(int n, AtomicReference<Policy> policy) {
        Map<String, Node> byName = new ConcurrentHashMap<>();
        List<String> names = new ArrayList<>();
        for (int i = 1; i <= n; i++) names.add("edge-" + i);
        List<Node> nodes = new ArrayList<>();
        for (String name : names) {
            Node nd = new Node();
            nd.net = new PeerNet(name, (to, m) -> Thread.startVirtualThread(() -> byName.get(to).net.deliver(m)));
            nd.net.setMembers(names);
            nd.ra = new RicartAgrawala(nd.net);
            nd.sk = new SuzukiKasami(nd.net);
            nd.pool = new PoolLocks(nd.net, policy::get);
            nd.net.onMessage(m -> {
                switch (m.getBodyCase()) {
                    case RA_REQUEST, RA_REPLY -> nd.ra.onMessage(m);
                    case SK_REQUEST, SK_TOKEN -> nd.sk.onMessage(m);
                    default -> nd.pool.onMessage(m);
                }
            });
            byName.put(name, nd);
            nodes.add(nd);
        }
        return nodes;
    }

    private static final Limit NO_REFILL = Policies.limit(100, 0.0001);

    @Test void ricartAgrawalaCosts2NMinus1MessagesPerEntry() throws Exception {
        for (int n : new int[]{3, 5}) {
            List<Node> c = cluster(n, new AtomicReference<>(Policies.defaults()));
            assertEquals(10, c.get(1).ra.take("k", NO_REFILL, 10));
            Thread.sleep(100);
            long total = c.stream().mapToLong(x -> x.ra.messages.get()).sum();
            assertEquals(2L * (n - 1), total, "N=" + n);
        }
    }

    @Test void suzukiKasamiCostsNMessagesOrZero() throws Exception {
        int n = 5;
        List<Node> c = cluster(n, new AtomicReference<>(Policies.defaults()));
        c.get(0).sk.take("k", NO_REFILL, 1);                  // lowest id holds the token initially
        assertEquals(0, c.stream().mapToLong(x -> x.sk.messages.get()).sum());
        c.get(3).sk.take("k", NO_REFILL, 1);                  // N-1 requests + 1 token
        Thread.sleep(100);
        assertEquals(n, c.stream().mapToLong(x -> x.sk.messages.get()).sum());
    }

    @Test void bothAlgorithmsNeverOverAdmitUnderConcurrency() throws Exception {
        for (String algo : new String[]{"ra", "token"}) {
            List<Node> c = cluster(4, new AtomicReference<>(Policies.defaults()));
            AtomicLong granted = new AtomicLong();
            ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
            List<Future<?>> fs = new ArrayList<>();
            for (Node nd : c) for (int t = 0; t < 4; t++) fs.add(ex.submit(() -> {
                for (int i = 0; i < 15; i++) {
                    try { granted.addAndGet(algo.equals("ra") ? nd.ra.take("pool", NO_REFILL, 3) : nd.sk.take("pool", NO_REFILL, 3)); }
                    catch (LeaderClient.NoLeader ignored) {}
                }
                return null;
            }));
            for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
            assertEquals(100, granted.get(), algo + " must hand out exactly the pool (100), never more");
        }
    }

    @Test void chandyMisraHaasDetectsAndResolvesCrossingLocks() throws Exception {
        AtomicReference<Policy> pol = new AtomicReference<>(Policies.defaults().toBuilder().setLockOrdering(false).build());
        List<Node> c = cluster(2, pol);
        Limit big = Policies.limit(1_000_000, 1_000_000);
        runCrossing(c, big);
        long deadlocks = c.stream().mapToLong(x -> x.pool.deadlocks.get()).sum();
        long probes = c.stream().mapToLong(x -> x.pool.probes.get()).sum();
        assertTrue(deadlocks > 0, "crossing lock orders must deadlock and be detected");
        assertTrue(probes > 0);

        pol.set(pol.get().toBuilder().setLockOrdering(true).build());   // ordered locking: no cycles
        List<Node> c2 = cluster(2, pol);
        runCrossing(c2, big);
        assertEquals(0, c2.stream().mapToLong(x -> x.pool.deadlocks.get()).sum());
    }

    private static void runCrossing(List<Node> c, Limit lim) throws Exception {
        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        CyclicBarrier start = new CyclicBarrier(c.size());
        List<Future<?>> fs = new ArrayList<>();
        for (Node nd : c) fs.add(ex.submit(() -> {
            for (int i = 0; i < 15; i++) {
                start.await(5, TimeUnit.SECONDS);
                nd.pool.takeBoth("t:acme", lim, "k:acme:key0", lim, 5);
            }
            return null;
        }));
        for (Future<?> f : fs) f.get(90, TimeUnit.SECONDS);   // must finish: deadlocks get resolved
    }
}
