package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.nio.file.Path;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
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
	private IngestionCycleActivity activity;

	@BeforeEach
	void setUp() {
		operationalState = mock(BackendDataOperationalState.class);
		operationalMetrics = mock(BackendDataOperationalMetrics.class);
		storageMonitor = mock(BackendDataStorageMonitor.class);
		activity = mock(IngestionCycleActivity.class);
		when(activity.observe()).thenReturn(new IngestionCycleActivity.Observation(false, false, false, 0, 0));
		indicator = new BackendDataPipelineHealthIndicator(
				operationalState,
				operationalMetrics,
				storageMonitor,
				activity,
				GdeltTestFixtures.properties(Path.of(".local", "health-test"), 1024));
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
				"automaticEnabled", "cycleRunning", "schedulerStale", "terminalActivityAgeSeconds",
				"nextCycleDelaySeconds", "sourceOutage", "successfulPollAgeSeconds", "sourceLagSeconds",
				"sourceRetryDelaySeconds", "sourceCooldown", "catalogPending", "eventBootstrapRemaining",
				"mentionBootstrapRemaining",
				"retryDue",
				"retryDeferred",
				"retryExhausted",
				"receiptAuditRetryDue", "receiptAuditRetryDeferred", "receiptAuditRetryExhausted",
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
				snapshot.aliasConsistency(),
				snapshot.live());
		when(operationalState.observe()).thenReturn(snapshot);

		Health health = indicator.health();

		assertThat(health.getStatus().getCode()).isEqualTo("DEGRADED");
		assertThat(health.getDetails()).containsEntry("lagSeconds", 900L);
	}

	@Test
	@DisplayName("Долгий отказ GDELT и пауза повторов не означают остановку планировщика")
	void sourceOutageAndCooldownRemainSeparateFromSchedulerActivity() {
		BackendDataOperationalSnapshot base = snapshot(true, 0, 0, AliasConsistencyState.CONSISTENT);
		when(operationalState.observe()).thenReturn(new BackendDataOperationalSnapshot(
				base.databaseAvailable(), base.lagSeconds(), base.openGaps(),
				base.sourcePollRetries(), base.acquisitionRetries(), base.processingRetries(),
				base.permanentFailures(), base.receipts(), base.generations(),
				base.repairRequiredPartitions(), base.openMaintenanceOperations(), base.aliasConsistency(),
				new BackendDataOperationalSnapshot.LiveCounts(2400, 2400, 1800, 600, true, false, 0, 1,
						new BackendDataOperationalSnapshot.RetryCounts(0, 1, 0))));
		when(activity.observe()).thenReturn(new IngestionCycleActivity.Observation(true, false, false, 20, 40));

		Health health = indicator.health();

		assertThat(health.getStatus().getCode()).isEqualTo("DEGRADED");
		assertThat(health.getDetails()).containsEntry("sourceOutage", true)
				.containsEntry("schedulerStale", false)
				.containsEntry("sourceCooldown", true)
				.containsEntry("eventBootstrapRemaining", 0L)
				.containsEntry("mentionBootstrapRemaining", 1L)
				.containsEntry("receiptAuditRetryDeferred", 1L);
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
				aliasState,
				BackendDataOperationalSnapshot.LiveCounts.empty());
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
