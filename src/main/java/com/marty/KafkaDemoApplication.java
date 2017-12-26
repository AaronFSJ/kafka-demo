package com.marty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaDemoApplication {

	public static Logger logger = LoggerFactory.getLogger(KafkaDemoApplication.class);





	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

}
