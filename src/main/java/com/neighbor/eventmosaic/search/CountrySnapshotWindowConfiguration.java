package com.neighbor.eventmosaic.search;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.shared.time.RollingWindowPolicy;
import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Создает общее правило расчета суточного окна карты. Одно и то же правило используют
 * поиск и загрузка данных, поэтому они не расходятся во времени.
 */
@Configuration(proxyBeanMethods = false)
class CountrySnapshotWindowConfiguration {

	@Bean
	RollingWindowPolicy rollingWindowPolicy(CountryMapSnapshotProperties properties) {
		return new RollingWindowPolicy(
				GdeltSourceContract.UPDATE_INTERVAL,
				Duration.ofHours(24),
				properties.ingestionGrace());
	}
}
