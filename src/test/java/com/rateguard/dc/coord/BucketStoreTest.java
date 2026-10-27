package com.rateguard.dc.coord;

import com.rateguard.dc.common.Policies;
import com.rateguard.dc.proto.Limit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketStoreTest {
    private final Limit lim = Policies.limit(10, 5); // cap 10, 5 tokens/s

    @Test void leaseNeverExceedsCapacity() {
        BucketStore s = new BucketStore();
        long t = 1_000_000;
        assertEquals(6, s.lease("k", lim, 6, t));
        assertEquals(4, s.lease("k", lim, 6, t));   // only 4 left
        assertEquals(0, s.lease("k", lim, 6, t));
    }

    @Test void refillFollowsTheFormula() {
        BucketStore s = new BucketStore();
        long t = 1_000_000;
        s.lease("k", lim, 10, t);                   // empty
        assertEquals(5, s.lease("k", lim, 100, t + 1000));  // 1 s * 5/s
        assertEquals(10, s.lease("k", lim, 100, t + 60_000)); // capped at capacity
    }

    @Test void giveReturnsTokensButNotAboveCapacity() {
        BucketStore s = new BucketStore();
        long t = 1_000_000;
        s.lease("k", lim, 8, t);
        s.give("k", lim, 5, t);
        assertEquals(7, s.tokens("k"), 1e-9);
        s.give("k", lim, 50, t);
        assertEquals(10, s.tokens("k"), 1e-9);
    }

    @Test void slidingWindowCapsGrants() {
        BucketStore s = new BucketStore();
        Limit w = Limit.newBuilder().setCap(1000).setRate(1000).setWindowMs(1000).setWindowLimit(20).build();
        long t = 5_000_000;
        assertEquals(20, s.lease("w", w, 100, t));
        assertEquals(0, s.lease("w", w, 100, t + 10));
    }

    @Test void acquireIsAllOrNothing() {
        BucketStore s = new BucketStore();
        Limit one = Policies.limit(1, 0.001);
        Limit many = Policies.limit(100, 1);
        String[] keys = {"t", "k", "i"};
        Limit[] lims = {many, one, many};
        long t = 1_000_000;
        assertEquals(-1, s.acquire(keys, lims, t));
        assertEquals(1, s.acquire(keys, lims, t));       // key level refuses
        assertEquals(99, s.tokens("t"), 1e-6);           // tenant not charged by the refused call
    }
}
