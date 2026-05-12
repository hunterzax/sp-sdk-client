package com.ptt.client.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Parses ingest / realtime time strings (with or without milliseconds, ISO-Z).
 */
public final class RealtimeTimestampParser {

    private static final DateTimeFormatter WITH_MS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter NO_MS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private RealtimeTimestampParser() {
    }

    public static LocalDateTime parse(String timeStr) {
        if (timeStr == null || timeStr.isBlank()) {
            throw new IllegalArgumentException("time is empty");
        }
        String t = timeStr.trim();
        if (t.contains("T") && t.endsWith("Z")) {
            return LocalDateTime.ofInstant(Instant.parse(t), ZoneId.systemDefault());
        }
        try {
            return LocalDateTime.parse(t, WITH_MS);
        } catch (DateTimeParseException ignored) {
        }
        return LocalDateTime.parse(t, NO_MS);
    }
}
