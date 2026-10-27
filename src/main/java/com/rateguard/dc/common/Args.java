package com.rateguard.dc.common;

import java.util.HashMap;
import java.util.Map;

/** Minimal --key=value argument parser. */
public final class Args {
    private final Map<String, String> m = new HashMap<>();

    public Args(String[] argv, int from) {
        for (int i = from; i < argv.length; i++) {
            String a = argv[i];
            if (!a.startsWith("--")) continue;
            int eq = a.indexOf('=');
            if (eq < 0) m.put(a.substring(2), "true");
            else m.put(a.substring(2, eq), a.substring(eq + 1));
        }
    }

    public String str(String k, String def) { return m.getOrDefault(k, def); }
    public int integer(String k, int def) { return m.containsKey(k) ? Integer.parseInt(m.get(k)) : def; }
    public long lng(String k, long def) { return m.containsKey(k) ? Long.parseLong(m.get(k)) : def; }
    public double dbl(String k, double def) { return m.containsKey(k) ? Double.parseDouble(m.get(k)) : def; }
    public boolean bool(String k, boolean def) { return m.containsKey(k) ? Boolean.parseBoolean(m.get(k)) : def; }
}
