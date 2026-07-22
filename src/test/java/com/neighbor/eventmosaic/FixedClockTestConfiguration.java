package com.neighbor.eventmosaic;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Общая конфигурация детерминированного UTC-времени для интеграционных тестов. */
@TestConfiguration(proxyBeanMethods = false)
public class FixedClockTestConfiguration {

	public static final Instant NOW = Instant.parse("2026-07-20T13:00:00Z");

	@Bean
	@Primary
	Clock fixedClock() {
		return Clock.fixed(NOW, ZoneOffset.UTC);
	}
}
