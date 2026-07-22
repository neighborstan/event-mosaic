package com.neighbor.eventmosaic;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.testcontainers.service.connection.Ssl;
import org.springframework.context.annotation.Bean;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

/** Общая Testcontainers-конфигурация Elasticsearch integration tests. */
@TestConfiguration(proxyBeanMethods = false)
public class ElasticsearchTestcontainersConfiguration {

	/** Возвращает Elasticsearch container для Spring service connection. */
	@Bean
	@ServiceConnection
	@Ssl
	ElasticsearchContainer elasticsearchContainer() {
		return new ElasticsearchContainer(DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:9.4.2"));
	}
}
