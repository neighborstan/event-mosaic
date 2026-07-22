package com.neighbor.eventmosaic.shared.time;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Единая конфигурация прикладного времени. Production-код получает UTC clock
 * через constructor injection, а тесты могут подменить bean на {@code Clock.fixed(...)}.
 */
@Configuration(proxyBeanMethods = false)
public class TimeConfiguration {

	/**
	 * Создает системный clock с UTC zone.
	 *
	 * @return общий production clock приложения
	 */
	@Bean
	public Clock applicationClock() {
		return Clock.systemUTC();
	}
}
