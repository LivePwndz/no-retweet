package com.bdt.demobigdataprocessing;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class DemoBigDataProcessingApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(DemoBigDataProcessingApplication.class, args);
	}

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;



	@Override
	public void run(String... args) throws Exception {
		kafkaTemplate.send("quickstart-events", "This is another commandline message.");
		SparkService client = new SparkService();
		client.process();
	}

}
