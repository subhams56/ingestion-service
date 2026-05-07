package com.telecom.ingestion_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class IngestionServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionServiceApplication.class, args);
	}

}
