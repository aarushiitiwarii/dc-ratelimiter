package com.rateguard.dc.launcher;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Ports;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.time.Duration;
import java.util.*;

/**
 * Starts and supervises the whole cluster as separate JVM processes (no Docker,
 * no Kubernetes): 3 coordinators, N edges, the upstream API, the load balancer and
 * the console. Exports the ClusterControl RMI object on the registry at :1099.
 */
public final class Launcher implements ClusterControl {

    private static final class Proc {
        final String name, role; final int port; final List<String> args;
        volatile Process process; volatile long startedAt;
        Proc(String name, String role, int port, List<String> args) { this.name = name; this.role = role; this.port = port; this.args = args; }
        boolean alive() { return process != null && process.isAlive(); }
    }

    private final Map<String, Proc> procs = Collections.synchronizedMap(new LinkedHashMap<>());
    private final String jar, java, secret;
    private final File logs = new File("logs");
    private volatile int edgeCount;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();

    Launcher(int edges, String secret) throws Exception {
        this.edgeCount = edges;
        this.secret = secret;
        this.jar = new File(Launcher.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getAbsolutePath();
        this.java = ProcessHandle.current().info().command().orElse(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
        logs.mkdirs();
    }

    private void define(String name, String role, int port, String... args) {
        procs.put(name, new Proc(name, role, port, List.of(args)));
    }

    private synchronized boolean launch(Proc p) {
        if (p.alive()) return false;
        try {
            List<String> cmd = new ArrayList<>(List.of(java, "-Xmx160m", "-XX:+UseSerialGC", "-Xss512k",
                    "-cp", jar, "com.rateguard.dc.Main", p.role));
            cmd.addAll(p.args);
            ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logs, p.name + ".log")));
            p.process = pb.start();
            p.startedAt = System.currentTimeMillis();
            Log.info("started %-9s pid %d", p.name, p.process.pid());
            return true;
        } catch (Exception e) {
            Log.info("could not start %s: %s", p.name, e.getMessage());
            return false;
        }
    }

    private void defineEdge(int i) {
        define(Ports.edgeName(i), "edge", Ports.edgeHttp(i), "--id=" + i, "--edges=" + edgeCount, "--secret=" + secret);
    }

    void startAll(boolean openBrowser) throws Exception {
        define("upstream", "upstream", Ports.upstream(1), "--count=3");
        for (int i = 1; i <= Ports.COORDS; i++) define(Ports.coordName(i), "coord", Ports.coord(i), "--id=" + i);
        for (int i = 1; i <= edgeCount; i++) defineEdge(i);
        define("lb", "lb", Ports.LB, "--strategy=rr");
        define("console", "console", Ports.CONSOLE, "--secret=" + secret);

        Registry reg = LocateRegistry.createRegistry(Ports.RMI_REGISTRY);
        reg.rebind(NAME, UnicastRemoteObject.exportObject(this, 0));
        Log.info("RMI registry on :%d, ClusterControl bound as '%s'", Ports.RMI_REGISTRY, NAME);

        for (Proc p : snapshot()) launch(p);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Proc p : snapshot()) if (p.alive()) p.process.destroyForcibly();
        }));
        if (openBrowser) {
            Thread.sleep(6000);
            try { new ProcessBuilder("cmd", "/c", "start", "", "http://localhost:" + Ports.CONSOLE).start(); } catch (Exception ignored) {}
        }
        Log.info("cluster up: console http://localhost:%d, load balancer http://localhost:%d (Ctrl+C stops everything)", Ports.CONSOLE, Ports.LB);
    }

    private List<Proc> snapshot() { synchronized (procs) { return new ArrayList<>(procs.values()); } }

    // ------------------------------------------------------------ ClusterControl (RMI)
    @Override public List<ProcInfo> processes() {
        List<ProcInfo> out = new ArrayList<>();
        for (Proc p : snapshot()) out.add(new ProcInfo(p.name, p.role, p.port, p.alive(), p.alive() ? p.process.pid() : 0, p.startedAt));
        return out;
    }

    @Override public List<String> edges() {
        List<String> out = new ArrayList<>();
        for (Proc p : snapshot()) if (p.role.equals("edge") && p.alive()) out.add(p.name);
        return out;
    }

    @Override public boolean kill(String name) {
        Proc p = procs.get(name);
        if (p == null || !p.alive()) return false;
        p.process.destroyForcibly();
        Log.info("killed %s (failure injection)", name);
        return true;
    }

    @Override public boolean start(String name) {
        Proc p = procs.get(name);
        return p != null && launch(p);
    }

    @Override public synchronized int scaleEdges(int n) {
        n = Math.max(1, Math.min(8, n));
        edgeCount = n;
        for (int i = 1; i <= n; i++) {
            if (!procs.containsKey(Ports.edgeName(i))) defineEdge(i);
            launch(procs.get(Ports.edgeName(i)));
        }
        for (Proc p : snapshot()) {
            if (!p.role.equals("edge")) continue;
            int id = Integer.parseInt(p.name.substring(5));
            if (id <= n) continue;
            if (p.alive()) { p.process.destroyForcibly(); Log.info("scaled down: stopped %s", p.name); }
            procs.remove(p.name);                         // no longer part of the cluster (not just "down")
        }
        Log.info("edges scaled to %d", n);
        return n;
    }

    @Override public boolean slow(String edge, long ms) throws RemoteException {
        Proc p = procs.get(edge);
        if (p == null) return false;
        try {
            http.send(HttpRequest.newBuilder(URI.create("http://" + Ports.HOST + ":" + p.port + "/admin/slow?ms=" + ms)).GET().build(),
                    HttpResponse.BodyHandlers.discarding());
            return true;
        } catch (Exception e) { throw new RemoteException("slow failed", e); }
    }

    public static void main(int edges, String secret, boolean openBrowser) throws Exception {
        Log.setName("launcher");
        new Launcher(edges, secret).startAll(openBrowser);
        Thread.currentThread().join();
    }
}
