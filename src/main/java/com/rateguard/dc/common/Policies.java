package com.rateguard.dc.common;

import com.rateguard.dc.proto.Limit;
import com.rateguard.dc.proto.Policy;

/** Default limits: identical to the cloud version (gateway.yaml). */
public final class Policies {
    private Policies() {}

    public static Limit limit(double cap, double rate) {
        return Limit.newBuilder().setCap(cap).setRate(rate).setWindowMs(1000).setWindowLimit(0).build();
    }

    public static Policy defaults() {
        return Policy.newBuilder()
                .setTenant(limit(400, 200))
                .setKey(limit(100, 50))
                .setIp(limit(60, 30))
                .setIpLocal(true)
                .setConsistency("bounded")
                .setOutageMode("local")
                .setLockOrdering(true)
                .setVersion(1)
                .build();
    }

    public static Limit level(Policy p, int lvl) {
        return switch (lvl) { case 0 -> p.getTenant(); case 1 -> p.getKey(); default -> p.getIp(); };
    }
}
