package com.rateguard.dc.edge;

import com.rateguard.dc.common.Policies;
import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.Policy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Chandy-Misra-Haas with 3 edges and several request threads per edge (like the live cluster). */
class CmhThreeNodeTest {
    @Test void detectsDeadlocksWithThreeNodesAndConcurrentThreads() throws Exception {
        AtomicReference<Policy> pol = new AtomicReference<>(Policies.defaults().toBuilder().setLockOrdering(false).build());
        List<DistributedAlgorithmsTest.Node> c = DistributedAlgorithmsTest.cluster(3, pol);
        Limit lim = Policies.limit(1_000_000, 1_000_000);
        ExecutorService ex = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<?>> fs = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 6000;
        for (var nd : c) for (int t = 0; t < 4; t++) fs.add(ex.submit(() -> {
            while (System.currentTimeMillis() < deadline) nd.pool.takeBoth("t:acme-r9", lim, "k:acme-r9:key0", lim, 5);
            return null;
        }));
        for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
        long deadlocks = c.stream().mapToLong(x -> x.pool.deadlocks.get()).sum();
        long aborts = c.stream().mapToLong(x -> x.pool.aborts.get()).sum();
        long probes = c.stream().mapToLong(x -> x.pool.probes.get()).sum();
        System.out.printf("3 nodes: deadlocks=%d aborts=%d probes=%d%n", deadlocks, aborts, probes);
        assertTrue(deadlocks > 0, "deadlocks must be detected");
        assertTrue(aborts <= deadlocks + 2, "aborts should come from detection, not from the 5 s timeout");
    }
}
