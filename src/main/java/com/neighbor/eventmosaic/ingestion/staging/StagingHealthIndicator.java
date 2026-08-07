package com.neighbor.eventmosaic.ingestion.staging;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Включает в readiness только безопасный доступ к staging storage. Низкий
 * остаток свободного места остается отдельным operational state и сам по себе
 * не меняет readiness.
 */
@Component
public class StagingHealthIndicator implements HealthIndicator {

	private final StagingStorageProbe probe;

	/**
	 * Создает health indicator поверх общей проверки staging.
	 *
	 * @param probe безопасная проверка staging storage
	 */
	public StagingHealthIndicator(StagingStorageProbe probe) {
		this.probe = probe;
	}

	/**
	 * Возвращает bounded health без path, exception и технических сообщений.
	 *
	 * @return {@code UP} для доступного staging, иначе {@code DOWN}
	 */
	@Override
	public Health health() {
		StagingStorageProbeResult result = probe.probe();
		Health.Builder builder = result.isWritable() ? Health.up() : Health.down();
		return builder
				.withDetail("status", result.status().name())
				.withDetail("errorCode", result.errorCode().name())
				.build();
	}
}
