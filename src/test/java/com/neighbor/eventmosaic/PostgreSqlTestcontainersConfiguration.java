package com.neighbor.eventmosaic;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/** Общая Testcontainers-конфигурация PostgreSQL integration tests. */
@TestConfiguration(proxyBeanMethods = false)
public class PostgreSqlTestcontainersConfiguration {

	/** Возвращает PostgreSQL container для Spring service connection. */
	@Bean
	@ServiceConnection
	PostgreSQLContainer postgresContainer() {
		return new PostgreSQLContainer(DockerImageName.parse("postgres:18"));
	}
}
