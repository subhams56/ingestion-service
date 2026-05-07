package com.telecom.ingestion_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaDataIngestionService {

    private final JdbcTemplate jdbcTemplate;

    // In-memory buffer to hold messages before batch insertion
    private final List<Map<String, Object>> messageBuffer = new ArrayList<>();

    // The maximum number of records to hold before forcing a database insert
    private static final int BATCH_SIZE_THRESHOLD = 50;

    /**
     * Consumes messages from Kafka continuously, but only adds them to an in-memory list.
     * Triggers a batch insert ONLY if the list size reaches 50.
     */
    @KafkaListener(topics = "telecom-network-metrics", groupId = "telecom-ingestion-group")
    public synchronized void consumeTelemetry(Map<String, Object> payload) {

        messageBuffer.add(payload);
        log.debug("Buffered message. Current buffer size: {}", messageBuffer.size());

        if (messageBuffer.size() >= BATCH_SIZE_THRESHOLD) {
            log.info("Batch threshold reached ({} records). Triggering batch insert.", BATCH_SIZE_THRESHOLD);
            flushBufferToDatabase();
        }
    }

    /**
     * A scheduled task that runs every 15 minutes.
     * If the buffer has records (but hasn't reached 50 yet), this forces the insert
     * so data doesn't sit in memory forever during low-traffic periods.
     */
    @Scheduled(fixedRate = 900000) // 15 minutes in milliseconds
    public synchronized void scheduledBufferFlush() {
        if (!messageBuffer.isEmpty()) {
            log.info("Scheduled 15-minute flush triggered. Flushing {} pending records.", messageBuffer.size());
            flushBufferToDatabase();
        }
    }

    /**
     * The core logic for taking the in-memory list and running a high-performance
     * Batch Insert into PostgreSQL.
     */
    private void flushBufferToDatabase() {
        if (messageBuffer.isEmpty()) {
            return;
        }

        String sql = """
            INSERT INTO refined_network_metrics (
                timestamp, hour_of_day, is_peak_hour, region, state, city, 
                network_band, environment_type, avg_latency_ms, download_speed_mbps, 
                upload_speed_mbps, packet_loss_pct, active_users, network_utilization_pct, 
                congestion_level, dropped_calls, weather_condition, quality_score, 
                device_model, carrier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

        try {
            // We copy the buffer to a local list so we can clear the main buffer instantly
            // allowing the Kafka consumer to keep receiving messages without blocking.
            List<Map<String, Object>> recordsToInsert = new ArrayList<>(messageBuffer);
            messageBuffer.clear();

            jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    Map<String, Object> record = recordsToInsert.get(i);

                    // Parse the ISO-8601 string timestamp from Python into a SQL Timestamp
                    String timestampStr = (String) record.get("timestamp");
                    ps.setTimestamp(1, Timestamp.from(Instant.parse(timestampStr + "Z")));

                    ps.setInt(2, (Integer) record.get("hour_of_day"));
                    ps.setInt(3, (Integer) record.get("is_peak_hour"));
                    ps.setString(4, (String) record.get("region"));
                    ps.setString(5, (String) record.get("state"));
                    ps.setString(6, (String) record.get("city"));
                    ps.setString(7, (String) record.get("network_band"));
                    ps.setString(8, (String) record.get("environment_type"));
                    ps.setInt(9, (Integer) record.get("avg_latency_ms"));
                    ps.setInt(10, (Integer) record.get("download_speed_mbps"));
                    ps.setInt(11, (Integer) record.get("upload_speed_mbps"));
                    ps.setDouble(12, ((Number) record.get("packet_loss_pct")).doubleValue());
                    ps.setInt(13, (Integer) record.get("active_users"));
                    ps.setInt(14, (Integer) record.get("network_utilization_pct"));
                    ps.setString(15, (String) record.get("congestion_level"));
                    ps.setInt(16, (Integer) record.get("dropped_calls"));
                    ps.setString(17, (String) record.get("weather_condition"));
                    ps.setDouble(18, ((Number) record.get("quality_score")).doubleValue());
                    ps.setString(19, (String) record.get("device_model"));
                    ps.setString(20, (String) record.get("carrier"));
                }

                @Override
                public int getBatchSize() {
                    return recordsToInsert.size();
                }
            });

            log.info("Successfully batch inserted {} records into PostgreSQL.", recordsToInsert.size());

        } catch (Exception e) {
            log.error("Failed to execute batch insert into PostgreSQL", e);
        }
    }

/**
 * A scheduled task that runs every 2 minutes.
 * Logs the current record count in the refined_network_metrics table.
 */
@Scheduled(fixedRate = 120000) // 2 minutes in milliseconds
public void logTableRecordCount() {
    try {
        String countSql = "SELECT COUNT(*) FROM refined_network_metrics";
        Integer count = jdbcTemplate.queryForObject(countSql, Integer.class);
        log.info("Current record count in refined_network_metrics table: {}", count);
    } catch (Exception e) {
        log.error("Failed to retrieve record count from refined_network_metrics table", e);
    }
}

}

