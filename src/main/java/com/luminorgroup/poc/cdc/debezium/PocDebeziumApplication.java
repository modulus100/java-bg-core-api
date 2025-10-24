package com.luminorgroup.poc.cdc.debezium;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class PocDebeziumApplication {

	public static void main(String[] args) {
		SpringApplication.run(PocDebeziumApplication.class, args);
	}

}
