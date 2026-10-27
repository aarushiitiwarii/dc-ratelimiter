package com.rateguard.dc;

import com.rateguard.dc.common.Args;

/**
 * Entry point: java -jar rateguard-dc.jar <role> [--options]
 *   launcher            start the whole cluster (default)
 *   coord --id=N        coordinator replica N (1..3)
 *   edge --id=N         edge gateway N
 *   lb                  load balancer on :8080
 *   upstream            protected API instances on :8201-8203
 *   console             operations console on :9000
 *   mint TENANT KEY     print a signed API key
 */
public final class Main {
    public static void main(String[] argv) throws Exception {
        String role = argv.length > 0 ? argv[0] : "launcher";
        Args a = new Args(argv, 1);
        switch (role) {
            case "launcher" -> com.rateguard.dc.launcher.Launcher.main(a.integer("edges", 3), a.str("secret", "demo123"), !a.bool("no-browser", false));
            case "coord" -> com.rateguard.dc.coord.CoordNode.main(a.integer("id", 1));
            case "edge" -> com.rateguard.dc.edge.EdgeNode.main(a.integer("id", 1), a.integer("edges", 3), a.str("secret", "demo123"), a.bool("trust-xff", true));
            case "lb" -> com.rateguard.dc.lb.LoadBalancer.main(a.str("strategy", "rr"));
            case "upstream" -> com.rateguard.dc.upstream.Upstream.main(a.integer("count", 3));
            case "console" -> com.rateguard.dc.console.ConsoleServer.main(a.str("secret", "demo123"));
            case "mint" -> {
                if (argv.length < 3) { System.err.println("usage: mint TENANT KEY [--secret=...]"); System.exit(2); }
                System.out.println(new com.rateguard.dc.auth.Authenticator(a.str("secret", "demo123"), 4, 1000).mintKey(argv[1], argv[2]));
            }
            default -> {
                System.err.println("unknown role: " + role);
                System.exit(2);
            }
        }
    }
}
