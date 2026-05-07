package com.telecom.ingestion_service.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaDataIngestionService {

    private final JdbcTemplate jdbcTemplate;

    @KafkaListener(topics = "telecom-network-metrics", groupId = "telecom-ingestion-group")
    public void consumeTelemetry(Map<String, Object> payload) {
        log.info("Received telemetry from Kafka -> City: {} | Band: {} | Drops: {}",
                payload.get("city"), payload.get("network_band"), payload.get("dropped_calls"));

        String sql = """
            INSERT INTO refined_network_metrics (
                timestamp, hour_of_day, is_peak_hour, region, state, city, 
                network_band, environment_type, avg_latency_ms, download_speed_mbps, 
                upload_speed_mbps, packet_loss_pct, active_users, network_utilization_pct, 
                congestion_level, dropped_calls, weather_condition, quality_score, 
                device_model, carrier
            ) VALUES (
                CAST(? AS timestamp), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """;

        try {
            jdbcTemplate.update(sql,
                    payload.get("timestamp"),
                    payload.get("hour_of_day"),
                    payload.get("is_peak_hour"),
                    payload.get("region"),
                    payload.get("state"),
                    payload.get("city"),
                    payload.get("network_band"),
                    payload.get("environment_type"),
                    payload.get("avg_latency_ms"),
                    payload.get("download_speed_mbps"),
                    payload.get("upload_speed_mbps"),
                    payload.get("packet_loss_pct"),
                    payload.get("active_users"),
                    payload.get("network_utilization_pct"),
                    payload.get("congestion_level"),
                    payload.get("dropped_calls"),
                    payload.get("weather_condition"),
                    payload.get("quality_score"),
                    payload.get("device_model"),
                    payload.get("carrier")
            );
        } catch (Exception e) {
            log.error("Failed to insert Kafka payload into PostgreSQL", e);
        }
    }
}
