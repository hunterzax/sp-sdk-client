package com.ptt.client.service;

import com.ptt.client.model.SensorDataRecord;
import com.ptt.client.util.LogSanitizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.ptt.client.util.RealtimeTimestampParser;
import jakarta.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

/**
 * Service for managing H2 database operations using Spring JdbcTemplate.
 */
@Service
public class DatabaseService {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseService.class);

    private final JdbcTemplate jdbcTemplate;
    private final int retentionDays;

    @Autowired
    public DatabaseService(@Qualifier("localJdbcTemplate") JdbcTemplate jdbcTemplate,
            @Value("${db.retention.days:7}") int retentionDays) {
        this.jdbcTemplate = jdbcTemplate;
        this.retentionDays = retentionDays;
    }

    /**
     * Initialize database schema.
     */
    @PostConstruct
    public void createSchema() {
        String createTableSQL = """
                    CREATE TABLE IF NOT EXISTS sensor_data (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        tagname VARCHAR(255) NOT NULL,
                        cur_value DOUBLE NOT NULL,
                        time TIMESTAMP NOT NULL,
                        avg_hour_current DOUBLE,
                        avg_hour_previous DOUBLE,
                        avg_day_current DOUBLE,
                        avg_day_previous DOUBLE,
                        flag_fresh VARCHAR(64),
                        hillow_state VARCHAR(64),
                        received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """;

        String createReceivedAtIndex = "CREATE INDEX IF NOT EXISTS idx_received_at ON sensor_data(received_at)";
        String createTagnameIndex = "CREATE INDEX IF NOT EXISTS idx_tagname ON sensor_data(tagname)";
        String addAvgHourCurrentColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS avg_hour_current DOUBLE";
        String addAvgHourPreviousColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS avg_hour_previous DOUBLE";
        String addAvgDayCurrentColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS avg_day_current DOUBLE";
        String addAvgDayPreviousColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS avg_day_previous DOUBLE";
        String addFlagFreshColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS flag_fresh VARCHAR(64)";
        String addHillowStateColumn = "ALTER TABLE sensor_data ADD COLUMN IF NOT EXISTS hillow_state VARCHAR(64)";

        try {
            jdbcTemplate.execute(createTableSQL);
            jdbcTemplate.execute(addAvgHourCurrentColumn);
            jdbcTemplate.execute(addAvgHourPreviousColumn);
            jdbcTemplate.execute(addAvgDayCurrentColumn);
            jdbcTemplate.execute(addAvgDayPreviousColumn);
            jdbcTemplate.execute(addFlagFreshColumn);
            jdbcTemplate.execute(addHillowStateColumn);
            jdbcTemplate.execute(createReceivedAtIndex);
            jdbcTemplate.execute(createTagnameIndex);
            logger.info("Schema created/verified");
        } catch (Exception e) {
            logger.error("Error creating schema", e);
        }
    }

    /**
     * Insert a sensor data record into the database.
     */
    public void insertRecord(SensorDataRecord record) {
        String insertSQL = """
                INSERT INTO sensor_data
                (tagname, cur_value, time, avg_hour_current, avg_hour_previous, avg_day_current, avg_day_previous, flag_fresh, hillow_state)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        try {
            // CWE-476: guard against null time before parsing to prevent NullPointerException
            Timestamp timestamp = parseTimestamp(record.getTime());
            jdbcTemplate.update(
                    insertSQL,
                    record.getTagname(),
                    record.getCurValue(),
                    timestamp,
                    record.getAvgHourCurrent(),
                    record.getAvgHourPrevious(),
                    record.getAvgDayCurrent(),
                    record.getAvgDayPrevious(),
                    record.getFlagFresh(),
                    record.getHillowState());
            // CWE-117: sanitize tagname (untrusted Kafka payload) before logging
            logger.debug("Stored record to database: {}", LogSanitizer.sanitize(record.getTagname()));
        } catch (Exception e) {
            logger.error("Error inserting record", e);
        }
    }

    /**
     * Parse timestamp string to SQL Timestamp.
     * <p>
     * CWE-476: {@code timeStr} may be {@code null} when a field is missing from
     * the incoming payload. The null is handled explicitly here so that
     * {@code Timestamp.valueOf()} is never called with a {@code null} argument.
     *
     * @param timeStr the raw time string from the sensor record; may be null
     * @return a valid {@link Timestamp}; falls back to current time on any parse failure
     */
    private Timestamp parseTimestamp(String timeStr) {
        // CWE-476: explicit null/blank guard — never propagate null into Timestamp.valueOf()
        if (timeStr == null || timeStr.isBlank()) {
            logger.warn("Missing timestamp field, using current time");
            return new Timestamp(System.currentTimeMillis());
        }
        try {
            // CWE-476: explicitly null-check the parsed result to prevent NullPointerException
            java.time.LocalDateTime parsed = RealtimeTimestampParser.parse(timeStr);
            if (parsed == null) {
                logger.warn("Parsed timestamp was null, using current time");
                return new Timestamp(System.currentTimeMillis());
            }
            return Timestamp.valueOf(parsed);
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp '{}', using current time", LogSanitizer.sanitize(timeStr));
            return new Timestamp(System.currentTimeMillis());
        }
    }

    /**
     * Delete records older than the configured retention period.
     */
    public void cleanupOldRecords() {
        String deleteSQL = "DELETE FROM sensor_data WHERE received_at < DATEADD('DAY', ?, CURRENT_TIMESTAMP)";
        int deleted = jdbcTemplate.update(deleteSQL, -retentionDays);
        if (deleted > 0) {
            logger.info("Cleaned up {} records older than {} days", deleted, retentionDays);
        }
    }

    /**
     * Get the total count of records in the database.
     */
    public long getRecordCount() {
        String countSQL = "SELECT COUNT(*) FROM sensor_data";
        Long count = jdbcTemplate.queryForObject(countSQL, Long.class);
        return count != null ? count : 0;
    }

    /**
     * Get unique tags in the database.
     */
    public List<String> getAllTags() {
        return jdbcTemplate.queryForList("SELECT DISTINCT tagname FROM sensor_data ORDER BY tagname", String.class);
    }

    /**
     * Get sensor history for the last N days.
     */
    public List<Map<String, Object>> getSensorHistory(String tag, int days) {
        String sql = """
                    SELECT time, cur_value
                    FROM sensor_data
                    WHERE tagname = ?
                    AND received_at > DATEADD('DAY', ?, CURRENT_TIMESTAMP)
                    ORDER BY time ASC
                """;
        return jdbcTemplate.queryForList(sql, tag, -days);
    }

    /**
     * Get heatmap data (aggregated by minute) for a specific tag.
     * Returns count of records per minute to visualize density/availability.
     */
    public List<Map<String, Object>> getSensorHeatmap(String tag) {
        // H2 specific SQL to truncate timestamp to minute
        String sql = """
                    SELECT FORMATDATETIME(time, 'yyyy-MM-dd HH:mm') as minute_bucket, COUNT(*) as count, AVG(cur_value) as avg_value
                    FROM sensor_data
                    WHERE tagname = ?
                    GROUP BY minute_bucket
                    ORDER BY minute_bucket DESC
                    LIMIT 10080
                """; // Last 7 days in minutes (approx)
        return jdbcTemplate.queryForList(sql, tag);
    }
}
