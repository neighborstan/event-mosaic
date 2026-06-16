package com.neighbor.eventmosaic;

import org.springframework.boot.SpringApplication;

public class TestEventMosaicApplication {

	static void main(String[] args) {
		SpringApplication.from(EventMosaicApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
