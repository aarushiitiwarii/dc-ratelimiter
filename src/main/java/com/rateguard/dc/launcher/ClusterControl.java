package com.rateguard.dc.launcher;

import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * Java RMI remote interface exported by the launcher (lab experiment 1: "gRPC / RMI").
 * It is the cluster's control plane and naming service: the console uses it for
 * failure injection and scaling, the load balancer to discover edge nodes.
 */
public interface ClusterControl extends Remote {
    String NAME = "rateguard-cluster";

    record ProcInfo(String name, String role, int port, boolean alive, long pid, long startedAt) implements Serializable {}

    List<ProcInfo> processes() throws RemoteException;

    /** Names of the edge nodes that should be running (e.g. edge-1..edge-3). */
    List<String> edges() throws RemoteException;

    /** Kills a process (failure injection). */
    boolean kill(String name) throws RemoteException;

    /** Starts a stopped process again. */
    boolean start(String name) throws RemoteException;

    /** Starts or stops edge nodes so that exactly n run. Returns the new count. */
    int scaleEdges(int n) throws RemoteException;

    /** Adds an artificial delay to one edge node (dynamic load for the load-balancing experiment). */
    boolean slow(String edge, long ms) throws RemoteException;
}
