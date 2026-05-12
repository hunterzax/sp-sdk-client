package com.ptt.client.service;

import com.ptt.client.model.SensorDataRecord;
import com.ptt.client.util.LogSanitizer;
import com.ptt.client.util.RealtimeTimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.regex.Pattern;

/**
 * Writes Kafka sensor payloads to MSSQL using the same logical columns as PostgreSQL
 * {@code realtime_analog} (time, tagname, cur_value, averages, flag_fresh, hillow_state).
 */
@Service
@ConditionalOnProperty(name = "dbwriter.enabled", havingValue = "true")
public class MssqlWriterService {

    private static final Logger logger = LoggerFactory.getLogger(MssqlWriterService.class);
    private static final Pattern SAFE_TABLE = Pattern.compile("^[a-zA-Z0-9_.\\[\\]]+$");

    private final JdbcTemplate jdbc;
    private final String qualifiedTable;

    public MssqlWriterService(
            @Qualifier("mssqlWriterJdbcTemplate") JdbcTemplate jdbcTemplate,
            @Value("${dbwriter.table:dbo.realtime_analog}") String table) {
        this.jdbc = jdbcTemplate;
        this.qualifiedTable = sanitizeTableName(table);
    }

    private static String sanitizeTableName(String table) {
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("dbwriter.table must be set (e.g. dbo.realtime_analog)");
        }
        String t = table.trim();
        if (!SAFE_TABLE.matcher(t).matches()) {
            throw new IllegalArgumentException("dbwriter.table contains invalid characters: " + table);
        }
        return t;
    }

    /**
     * Inserts one row mirroring the realtime stream (append).
     */
    public void write(SensorDataRecord record) {
        if (record.getTagname() == null || record.getTagname().isBlank()) {
            logger.warn("Skipping MSSQL write: missing tagname");
            return;
        }
        String sql = String.format(
                "INSERT INTO %s ([time], [tagname], [cur_value], [avg_hour_current], [avg_hour_previous], "
                        + "[avg_day_current], [avg_day_previous], [flag_fresh], [hillow_state]) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                qualifiedTable);

        try {
            // CWE-476: getTime() originates from untrusted Kafka payload and may be null
            String timeStr = record.getTime();
            if (timeStr == null || timeStr.isBlank()) {
                logger.warn("Skipping MSSQL write: missing time field for tag {}",
                        LogSanitizer.sanitize(record.getTagname()));
                return;
            }
            // CWE-476: explicitly null-check the parsed result to prevent NullPointerException
            java.time.LocalDateTime parsed = RealtimeTimestampParser.parse(timeStr);
            if (parsed == null) {
                logger.warn("Parsed timestamp was null for tag {}",
                        LogSanitizer.sanitize(record.getTagname()));
                return;
            }
            Timestamp ts = Timestamp.valueOf(parsed);
            jdbc.update(sql, (PreparedStatement ps) -> {
                ps.setTimestamp(1, ts);
                ps.setString(2, record.getTagname());
                ps.setDouble(3, record.getCurValue());
                setNullableDouble(ps, 4, record.getAvgHourCurrent());
                setNullableDouble(ps, 5, record.getAvgHourPrevious());
                setNullableDouble(ps, 6, record.getAvgDayCurrent());
                setNullableDouble(ps, 7, record.getAvgDayPrevious());
                if (record.getFlagFresh() == null) {
                    ps.setNull(8, Types.NVARCHAR);
                } else {
                    ps.setString(8, record.getFlagFresh());
                }
                if (record.getHillowState() == null) {
                    ps.setNull(9, Types.NVARCHAR);
                } else {
                    ps.setString(9, record.getHillowState());
                }
            });
        } catch (Exception e) {
            // CWE-117: sanitize tagname (from untrusted Kafka payload) before logging
            logger.error("MSSQL mirror write failed for tag {}", LogSanitizer.sanitize(record.getTagname()), e);
        }
    }

    private static void setNullableDouble(PreparedStatement ps, int index, Double value) throws java.sql.SQLException {
        if (value == null) {
            ps.setNull(index, Types.FLOAT);
        } else {
            ps.setDouble(index, value);
        }
    }

}
