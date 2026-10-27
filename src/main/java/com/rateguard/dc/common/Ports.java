package com.rateguard.dc.common;

/** Fixed port layout of a local RateGuard-DC cluster (everything on 127.0.0.1). */
public final class Ports {
    private Ports() {}

    public static final String HOST = "127.0.0.1";
    public static final int RMI_REGISTRY = 1099;   // launcher: ClusterControl remote object
    public static final int LB = 8080;             // load balancer (clients send traffic here)
    public static final int CONSOLE = 9000;        // operations console
    public static final int COORDS = 3;            // coordinator replicas

    public static int coord(int id) { return 9200 + id; }          // gRPC TokenStore + Replication
    public static int edgeHttp(int id) { return 8100 + id; }       // edge gateway HTTP
    public static int edgePeer(int id) { return 9100 + id; }       // edge gRPC Peer service
    public static int upstream(int i) { return 8200 + i; }         // protected API instances

    public static String coordAddr(int id) { return HOST + ":" + coord(id); }
    public static String edgeName(int id) { return "edge-" + id; }
    public static String coordName(int id) { return "coord-" + id; }
}
