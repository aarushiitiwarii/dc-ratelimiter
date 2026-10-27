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
            case "coord" -> com.rateguard.dc.coord.CoordNode.main(a.integer("id", 1));
            default -> {
                System.err.println("unknown role: " + role);
                System.exit(2);
            }
        }
    }
}
