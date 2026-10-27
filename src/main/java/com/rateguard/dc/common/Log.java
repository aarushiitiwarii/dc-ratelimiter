package com.rateguard.dc.common;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/** Tiny timestamped logger (stdout is redirected to logs/<process>.log by the launcher). */
public final class Log {
    private Log() {}

    private static final DateTimeFormatter F = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static volatile String who = "main";

    public static void setName(String name) { who = name; }

    public static void info(String fmt, Object... a) {
        System.out.println(LocalTime.now().format(F) + " [" + who + "] " + String.format(fmt, a));
    }
}
