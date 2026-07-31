package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Настройки Backend Data MVP")
class BackendDataPropertiesTest {

	@Test
	@DisplayName("Принимает утвержденные безопасные defaults")
	void acceptsApprovedSafeDefaults() {
		assertThatCode(() -> properties(false, Duration.ofMinutes(15)))
				.doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Отклоняет включение автоматического удаления")
	void rejectsAutomaticDeletion() {
		assertThatThrownBy(() -> properties(true, Duration.ofMinutes(15)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("automaticDeletionEnabled");
	}

	@Test
	@DisplayName("Отклоняет maximum retry delay меньше initial delay")
	void rejectsMaximumRetryDelayBelowInitialDelay() {
		assertThatThrownBy(() -> properties(false, Duration.ofSeconds(30)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("maximumDelay");
	}

	@Test
	@DisplayName("Отклоняет partition interval, отличный от принятого P7D")
	void rejectsPartitionIntervalOtherThanP7d() {
		assertThatThrownBy(() -> new BackendDataProperties(
				Duration.ofDays(1),
				retry(Duration.ofMinutes(15)),
				Duration.ofMinutes(12),
				500,
				diskPressure(),
				cleanup(false)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("P7D");
	}

	private static BackendDataProperties properties(
			boolean automaticDeletionEnabled,
			Duration maximumRetryDelay
	) {
		return new BackendDataProperties(
				Duration.ofDays(7),
				retry(maximumRetryDelay),
				Duration.ofMinutes(12),
				500,
				diskPressure(),
				cleanup(automaticDeletionEnabled)
		);
	}

	private static BackendDataProperties.Retry retry(Duration maximumRetryDelay) {
		return new BackendDataProperties.Retry(
				Duration.ofMinutes(1),
				2.0,
				maximumRetryDelay,
				0.1,
				3);
	}

	private static BackendDataProperties.DiskPressure diskPressure() {
		return new BackendDataProperties.DiskPressure(
				1024L * 1024 * 1024,
				2L * 1024 * 1024 * 1024);
	}

	private static BackendDataProperties.Cleanup cleanup(boolean automaticDeletionEnabled) {
		return new BackendDataProperties.Cleanup(
				Duration.ofHours(24),
				Duration.ofDays(7),
				automaticDeletionEnabled);
	}
}
