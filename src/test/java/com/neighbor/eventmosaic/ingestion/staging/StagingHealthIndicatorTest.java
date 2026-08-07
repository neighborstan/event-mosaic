package com.neighbor.eventmosaic.ingestion.staging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

@DisplayName("Readiness staging storage")
class StagingHealthIndicatorTest {

	private final StagingStorageProbe probe = mock(StagingStorageProbe.class);
	private final StagingHealthIndicator indicator = new StagingHealthIndicator(probe);

	@Test
	@DisplayName("Доступный staging остается UP при любом подтвержденном остатке места")
	void reportsUpWithoutApplyingDiskPressureThreshold() {
		when(probe.probe()).thenReturn(StagingStorageProbeResult.writable(0));

		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsExactlyInAnyOrderEntriesOf(Map.of(
				"status", "WRITABLE",
				"errorCode", "NONE"));
	}

	@Test
	@DisplayName("Небезопасный staging возвращает DOWN только с bounded details")
	void reportsUnsafeStorageAsDownWithBoundedDetails() {
		when(probe.probe()).thenReturn(StagingStorageProbeResult.unsafe(
				StagingStorageProbeErrorCode.PATH_REJECTED));

		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		assertThat(health.getDetails()).containsExactlyInAnyOrderEntriesOf(Map.of(
				"status", "UNSAFE",
				"errorCode", "PATH_REJECTED"));
	}

	@Test
	@DisplayName("Недоступный staging возвращает DOWN без path и exception details")
	void reportsUnavailableStorageAsDownWithoutTechnicalDetails() {
		when(probe.probe()).thenReturn(StagingStorageProbeResult.unavailable(
				StagingStorageProbeErrorCode.WRITE_CHECK_FAILED));

		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.DOWN);
		assertThat(health.getDetails()).containsExactlyInAnyOrderEntriesOf(Map.of(
				"status", "UNAVAILABLE",
				"errorCode", "WRITE_CHECK_FAILED"));
	}
}
