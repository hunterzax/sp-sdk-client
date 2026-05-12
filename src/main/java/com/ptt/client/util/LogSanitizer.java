package com.ptt.client.util;

import org.owasp.encoder.Encode;

/**
 * Centralised log-sanitisation utility that prevents log-injection attacks
 * (CWE-117) by delegating to the <strong>OWASP Java Encoder</strong> —
 * a Veracode-trusted library for output neutralisation.
 * <p>
 * All untrusted values (user input, Kafka payloads, WebSocket messages, etc.)
 * <strong>must</strong> pass through {@link #sanitize(String)} before being
 * interpolated into a log statement.
 */
public final class LogSanitizer {

    /** Maximum length retained in a single sanitised log token. */
    private static final int MAX_LENGTH = 255;

    private LogSanitizer() {
        // utility class – no instances
    }

    /**
     * Sanitises the supplied value for safe inclusion in log output using
     * {@link org.owasp.encoder.Encode#forJava(String)} to neutralise
     * all CRLF sequences, control characters, and special escape sequences.
     * The result is truncated to {@value #MAX_LENGTH} characters to prevent
     * log flooding.
     * <p>
     * Veracode intrinsically recognises {@code Encode.forJava()} as a
     * trusted cleanser for CWE-117.
     *
     * @param value the untrusted string to sanitise; may be {@code null}
     * @return a safe string suitable for logging; never {@code null}
     */
    public static String sanitize(String value) {
        if (value == null) {
            return "null";
        }

        // 1. Truncate FIRST (prevents CWE-400 resource exhaustion
        //    and avoids splitting escape sequences produced by the encoder)
        String truncated = value;
        if (truncated.length() > MAX_LENGTH) {
            truncated = truncated.substring(0, MAX_LENGTH - 3) + "...";
        }

        // 2. Encode LAST – Veracode sees the return value directly from the
        //    trusted OWASP cleanser, keeping the "Safe" taint tag intact.
        return Encode.forJava(truncated);
    }
}
