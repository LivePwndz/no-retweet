package com.bdt.demobigdataprocessing;

import com.bdt.demobigdataprocessing.service.SparkService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class DemoBigDataProcessingApplication implements CommandLineRunner {

	@Autowired
	private SparkService client;

	public static void main(String[] args) {
		SpringApplication.run(DemoBigDataProcessingApplication.class, args);
	}




	@Override
	public void run(String... args) {
		client.noRetreets();
	}

}
