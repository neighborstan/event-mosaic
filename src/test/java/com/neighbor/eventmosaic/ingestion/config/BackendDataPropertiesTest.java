package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThat;
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
	@DisplayName("Сохраняет окна проверки и сроки подтвержденной команды очистки")
	void keepsCleanupDefaultShape() {
		BackendDataProperties.Cleanup cleanup = cleanup(false);

		assertThat(cleanup.orphanBuildingAge()).isEqualTo(Duration.ofHours(24));
		assertThat(cleanup.supersededAge()).isEqualTo(Duration.ofDays(7));
		assertThat(cleanup.planTtl()).isEqualTo(Duration.ofMinutes(15));
		assertThat(cleanup.ownershipLease()).isEqualTo(Duration.ofMinutes(15));
		assertThat(cleanup.automaticDeletionEnabled()).isFalse();
	}

	@Test
	@DisplayName("Отклоняет неположительный срок плана очистки")
	void rejectsNonPositiveCleanupPlanTtl() {
		assertThatThrownBy(() -> cleanup(Duration.ZERO, Duration.ofMinutes(15), false))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("planTtl");
	}

	@Test
	@DisplayName("Отклоняет неположительный срок владения очисткой")
	void rejectsNonPositiveCleanupOwnershipLease() {
		assertThatThrownBy(() -> cleanup(
				Duration.ofMinutes(15),
				Duration.ofSeconds(-1),
				false))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("ownershipLease");
	}

	@Test
	@DisplayName("Срок владения очисткой должен превышать общий deadline операции")
	void rejectsCleanupLeaseNotExceedingOperationDeadline() {
		assertThatThrownBy(() -> new BackendDataProperties(
				Duration.ofDays(7),
				retry(Duration.ofMinutes(15)),
				Duration.ofMinutes(12),
				500,
				diskPressure(),
				rebuild(),
				cleanup(Duration.ofMinutes(15), Duration.ofMinutes(12), false)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("cleanup ownershipLease");
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
				rebuild(),
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
				rebuild(),
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

	private static BackendDataProperties.Rebuild rebuild() {
		return new BackendDataProperties.Rebuild(
				Duration.ofMinutes(15),
				Duration.ofMinutes(15));
	}

	private static BackendDataProperties.Cleanup cleanup(boolean automaticDeletionEnabled) {
		return cleanup(
				Duration.ofMinutes(15),
				Duration.ofMinutes(15),
				automaticDeletionEnabled);
	}

	private static BackendDataProperties.Cleanup cleanup(
			Duration planTtl,
			Duration ownershipLease,
			boolean automaticDeletionEnabled
	) {
		return new BackendDataProperties.Cleanup(
				Duration.ofHours(24),
				Duration.ofDays(7),
				planTtl,
				ownershipLease,
				automaticDeletionEnabled);
	}
}
