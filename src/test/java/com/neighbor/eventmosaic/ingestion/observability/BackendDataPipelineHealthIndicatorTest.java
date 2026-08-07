package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

@DisplayName("Отдельное operational health pipeline")
class BackendDataPipelineHealthIndicatorTest {

	private BackendDataOperationalState operationalState;
	private BackendDataOperationalMetrics operationalMetrics;
	private BackendDataStorageMonitor storageMonitor;
	private BackendDataPipelineHealthIndicator indicator;

	@BeforeEach
	void setUp() {
		operationalState = mock(BackendDataOperationalState.class);
		operationalMetrics = mock(BackendDataOperationalMetrics.class);
		storageMonitor = mock(BackendDataStorageMonitor.class);
		indicator = new BackendDataPipelineHealthIndicator(
				operationalState,
				operationalMetrics,
				storageMonitor);
		when(storageMonitor.observe()).thenReturn(storage(
				StoragePressureState.AVAILABLE,
				StoragePressureState.AVAILABLE));
	}

	@Test
	@DisplayName("Чистый pipeline с доступным storage возвращает UP")
	void returnsUpForHealthyPipeline() {
		BackendDataOperationalSnapshot snapshot = snapshot(
				true,
				0,
				0,
				AliasConsistencyState.CONSISTENT);
		when(operationalState.observe()).thenReturn(snapshot);

		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.UP);
		assertThat(health.getDetails()).containsEntry("state", "UP");
		assertThat(health.getDetails().keySet()).containsExactlyInAnyOrderElementsOf(Set.of(
				"state",
				"lagSeconds",
				"openGaps",
				"retryDue",
				"retryDeferred",
				"retryExhausted",
				"permanentFailures",
				"receiptMismatches",
				"receiptSurpluses",
				"buildingGenerations",
				"activeGenerations",
				"supersededGenerations",
				"repairRequiredPartitions",
				"openMaintenanceOperations",
				"aliasConsistency",
				"stagingPressure",
				"elasticsearchPressure"));
		verify(operationalMetrics).observe(snapshot);
	}

	@Test
	@DisplayName("Отложенный retry и receipt surplus дают DEGRADED без DOWN")
	void returnsDegradedForPipelineFailures() {
		when(operationalState.observe()).thenReturn(snapshot(
				true,
				1,
				1,
				AliasConsistencyState.CONSISTENT));

		Health health = indicator.health();

		assertThat(health.getStatus().getCode()).isEqualTo("DEGRADED");
		assertThat(health.getStatus()).isNotEqualTo(Status.DOWN);
		assertThat(health.getDetails())
				.containsEntry("retryDeferred", 1L)
				.containsEntry("receiptSurpluses", 1L);
	}

	@Test
	@DisplayName("Положительный lag дает DEGRADED как operational state")
	void returnsDegradedForLag() {
		BackendDataOperationalSnapshot snapshot = snapshot(
				true,
				0,
				0,
				AliasConsistencyState.CONSISTENT);
		snapshot = new BackendDataOperationalSnapshot(
				snapshot.databaseAvailable(),
				900,
				snapshot.openGaps(),
				snapshot.sourcePollRetries(),
				snapshot.acquisitionRetries(),
				snapshot.processingRetries(),
				snapshot.permanentFailures(),
				snapshot.receipts(),
				snapshot.generations(),
				snapshot.repairRequiredPartitions(),
				snapshot.openMaintenanceOperations(),
				snapshot.aliasConsistency());
		when(operationalState.observe()).thenReturn(snapshot);

		Health health = indicator.health();

		assertThat(health.getStatus().getCode()).isEqualTo("DEGRADED");
		assertThat(health.getDetails()).containsEntry("lagSeconds", 900L);
	}

	@Test
	@DisplayName("Неподтвержденная диагностика дает UNKNOWN, а не restart signal DOWN")
	void returnsUnknownWhenDiagnosticStateIsUnavailable() {
		when(operationalState.observe()).thenReturn(BackendDataOperationalSnapshot.unavailable());

		Health health = indicator.health();

		assertThat(health.getStatus()).isEqualTo(Status.UNKNOWN);
		assertThat(health.getStatus()).isNotEqualTo(Status.DOWN);
	}

	@Test
	@DisplayName("Disk pressure дает DEGRADED и остается отдельным от readiness")
	void returnsDegradedForDiskPressure() {
		when(operationalState.observe()).thenReturn(snapshot(
				true,
				0,
				0,
				AliasConsistencyState.CONSISTENT));
		when(storageMonitor.observe()).thenReturn(storage(
				StoragePressureState.PRESSURE,
				StoragePressureState.AVAILABLE));

		Health health = indicator.health();

		assertThat(health.getStatus().getCode()).isEqualTo("DEGRADED");
		assertThat(health.getDetails()).containsEntry("stagingPressure", "PRESSURE");
	}

	private static BackendDataOperationalSnapshot snapshot(
			boolean databaseAvailable,
			long deferredRetry,
			long receiptSurplus,
			AliasConsistencyState aliasState
	) {
		return new BackendDataOperationalSnapshot(
				databaseAvailable,
				0,
				0,
				new BackendDataOperationalSnapshot.RetryCounts(0, deferredRetry, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				0,
				new BackendDataOperationalSnapshot.ReceiptCounts(0, receiptSurplus),
				new BackendDataOperationalSnapshot.GenerationCounts(0, 0, 0),
				0,
				0,
				aliasState);
	}

	private static BackendDataStorageSnapshot storage(
			StoragePressureState stagingState,
			StoragePressureState elasticsearchState
	) {
		return new BackendDataStorageSnapshot(
				observation(StorageResource.STAGING, stagingState, 100),
				observation(StorageResource.ELASTICSEARCH, elasticsearchState, 200));
	}

	private static StoragePressureObservation observation(
			StorageResource resource,
			StoragePressureState state,
			long minimum
	) {
		return new StoragePressureObservation(
				resource,
				state,
				state == StoragePressureState.UNAVAILABLE ? -1 : minimum,
				minimum);
	}
}
