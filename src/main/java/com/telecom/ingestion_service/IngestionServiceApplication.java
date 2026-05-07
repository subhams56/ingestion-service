package com.telecom.ingestion_service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;
import java.sql.Connection;

@Slf4j
@SpringBootApplication
@EnableKafka
@EnableScheduling
public class IngestionServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionServiceApplication.class, args);
	}

	@Bean
	public CommandLineRunner startupStatusCheck(DataSource dataSource,
												Environment env,
												KafkaListenerEndpointRegistry kafkaRegistry) {
		return args -> {
			log.info("=======================================================");
			log.info("          INGESTION SERVICE STARTUP STATUS             ");
			log.info("=======================================================");

			// 1. & 2. Kafka Connection, Topic, and Group Status
			String bootstrapServers = env.getProperty("spring.kafka.bootstrap-servers", "localhost:29092");
			String groupId = env.getProperty("spring.kafka.consumer.group-id", "telecom-ingestion-group");
			String topic = "telecom-network-metrics"; // Hardcoded in your @KafkaListener

			// If the registry has active containers, Kafka successfully connected and subscribed
			boolean isKafkaListening = !kafkaRegistry.getListenerContainers().isEmpty();

			log.info("[KAFKA] Connection Status : {}", isKafkaListening ? "CONNECTED & LISTENING" : "FAILED / NOT LISTENING");
			log.info("[KAFKA] Bootstrap Servers : {}", bootstrapServers);
			log.info("[KAFKA] Subscribed Topic  : {}", topic);
			log.info("[KAFKA] Consumer Group    : {}", groupId);

			// 3. & 4. Database Connection, Host, and Name
			try (Connection connection = dataSource.getConnection()) {
				String dbUrl = connection.getMetaData().getURL();
				String dbUser = connection.getMetaData().getUserName();

				log.info("[DATABASE] Connection Status : SUCCESS");
				log.info("[DATABASE] Connected User    : {}", dbUser);
				log.info("[DATABASE] Host & DB Name    : {}", dbUrl);
			} catch (Exception e) {
				log.error("[DATABASE] Connection Status : FAILED - Could not connect to PostgreSQL", e);
			}

			log.info("=======================================================");
		};
	}
}