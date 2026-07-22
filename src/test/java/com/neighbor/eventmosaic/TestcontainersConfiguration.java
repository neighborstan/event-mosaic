package com.neighbor.eventmosaic;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Import;

/** Объединяет все service containers только для полного application context test. */
@TestConfiguration(proxyBeanMethods = false)
@Import({PostgreSqlTestcontainersConfiguration.class, ElasticsearchTestcontainersConfiguration.class})
public class TestcontainersConfiguration {
}
