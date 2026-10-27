package com.rateguard.dc.upstream;

import com.rateguard.dc.common.Log;
import com.rateguard.dc.common.Ports;
import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;

/** The protected API: three tiny "hello" instances on :8201-8203 (one process). */
public final class Upstream {
    public static void main(int count) throws Exception {
        Log.setName("upstream");
        for (int i = 1; i <= count; i++) {
            final String name = "upstream-" + i;
            HttpServer s = HttpServer.create(new InetSocketAddress(Ports.HOST, Ports.upstream(i)), 2048);
            s.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            s.createContext("/", ex -> {
                byte[] b = ("hello from " + name + "\n").getBytes(StandardCharsets.UTF_8);
                ex.sendResponseHeaders(200, b.length);
                try (OutputStream os = ex.getResponseBody()) { os.write(b); }
            });
            s.start();
            Log.info("%s on :%d", name, Ports.upstream(i));
        }
        Thread.currentThread().join();
    }
}
