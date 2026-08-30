package com.neighbor.eventmosaic;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * Точка запуска единого Spring Boot приложения Event Mosaic.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class EventMosaicApplication {

	/**
	 * Запускает обычное web-приложение либо одну серверную maintenance-команду
	 * без HTTP-сервера, если ее mode явно передан в аргументах.
	 *
	 * @param args параметры запуска Spring Boot
	 */
	public static void main(String[] args) {
		boolean partitionRebuildCommand = PartitionRebuildCommandLine.isRequested(args);
		boolean generationCleanupCommand = GenerationCleanupCommandLine.isRequested(args);
		boolean maintenanceCommand = partitionRebuildCommand || generationCleanupCommand;
		SpringApplication application = createApplication(maintenanceCommand);
		ConfigurableApplicationContext context = application.run(args);
		if (maintenanceCommand) {
			context.close();
		}
	}

	static SpringApplication createApplication(boolean maintenanceCommand) {
		SpringApplication application = new SpringApplication(EventMosaicApplication.class);
		if (maintenanceCommand) {
			application.setWebApplicationType(WebApplicationType.NONE);
		}
		return application;
	}

}
