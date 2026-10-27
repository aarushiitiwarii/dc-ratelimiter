package com.rateguard.dc.coord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

class BullyElectionTest {

    /** In-memory network of replicas 1..n; nodes in {@code down} drop every message. */
    static final class Net implements BullyElection.Transport {
        final Map<Integer, BullyElection> nodes = new HashMap<>();
        final Set<Integer> down = ConcurrentHashMap.newKeySet();
        public boolean election(int to, int from, long term) { return !down.contains(to) && nodes.get(to).onElection(from, term); }
        public void coordinator(int to, int from, long term) { if (!down.contains(to)) nodes.get(to).onCoordinator(from, term); }
        public boolean heartbeat(int to, int from, long term) { return !down.contains(to) && nodes.get(to).onHeartbeat(from); }
        public long termOf(int to) { return down.contains(to) ? -1 : nodes.get(to).term(); }
    }

    private static Net cluster(int n) {
        Net net = new Net();
        for (int i = 1; i <= n; i++) net.nodes.put(i, new BullyElection(i, n, net));
        return net;
    }

    private static void settle(Net net, int rounds) throws InterruptedException {
        for (int r = 0; r < rounds; r++) {
            for (var e : net.nodes.entrySet()) if (!net.down.contains(e.getKey())) e.getValue().tick();
            Thread.sleep(60);
        }
    }

    private static int agreedLeader(Net net) {
        int l = -1;
        for (var e : net.nodes.entrySet()) {
            if (net.down.contains(e.getKey())) continue;
            int x = e.getValue().leader();
            if (l == -1) l = x; else if (l != x) return 0;
        }
        return l;
    }

    @Test void highestIdBecomesLeader() throws Exception {
        Net net = cluster(3);
        net.nodes.get(1).triggerElection();
        settle(net, 15);
        assertEquals(3, agreedLeader(net));
    }

    @Test void reElectsWhenLeaderDiesAndBulliesBackWhenItReturns() throws Exception {
        Net net = cluster(3);
        net.nodes.get(1).triggerElection();
        settle(net, 15);
        assertEquals(3, agreedLeader(net));

        net.down.add(3);                       // leader crashes
        settle(net, 30);
        assertEquals(2, agreedLeader(net));
        long term2 = net.nodes.get(2).term();

        net.down.remove(3);                    // it comes back and takes over
        net.nodes.get(3).triggerElection();
        settle(net, 30);
        assertEquals(3, agreedLeader(net));
        assertTrue(net.nodes.get(3).term() > term2, "term must increase on every new leader");
    }
}
