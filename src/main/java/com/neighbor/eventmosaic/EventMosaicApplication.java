package com.neighbor.eventmosaic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Точка запуска единого Spring Boot приложения Event Mosaic.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class EventMosaicApplication {

	/**
	 * Запускает application context с переданными параметрами командной строки.
	 *
	 * @param args параметры запуска Spring Boot
	 */
	public static void main(String[] args) {
		SpringApplication.run(EventMosaicApplication.class, args);
	}

}
