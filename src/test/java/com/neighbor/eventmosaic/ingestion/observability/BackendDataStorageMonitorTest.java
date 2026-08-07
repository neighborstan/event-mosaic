package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.staging.StagingStorageProbe;
import com.neighbor.eventmosaic.ingestion.staging.StagingStorageProbeErrorCode;
import com.neighbor.eventmosaic.ingestion.staging.StagingStorageProbeResult;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Защита новых операций от storage pressure")
class BackendDataStorageMonitorTest {

	private static final long STAGING_MINIMUM = 100;
	private static final long ELASTICSEARCH_MINIMUM = 200;

	private StagingStorageProbe stagingProbe;
	private IndexMaintenanceGateway indexGateway;
	private SimpleMeterRegistry meterRegistry;
	private BackendDataStorageMonitor monitor;

	@BeforeEach
	void setUp() {
		stagingProbe = mock(StagingStorageProbe.class);
		indexGateway = mock(IndexMaintenanceGateway.class);
		meterRegistry = new SimpleMeterRegistry();
		monitor = new BackendDataStorageMonitor(
				stagingProbe,
				indexGateway,
				properties(),
				new BackendDataStorageMetrics(meterRegistry));
		monitor.bindMetrics();
	}

	@Test
	@DisplayName("Metrics scrape сам обновляет bounded storage observation")
	void metricsScrapeRefreshesStorageWithoutHealthPoll() {
		when(stagingProbe.probe()).thenReturn(
				StagingStorageProbeResult.writable(STAGING_MINIMUM + 25));

		assertThat(meterRegistry.get("event_mosaic.storage.available")
				.tag("resource", "staging").gauge().value())
				.isEqualTo(STAGING_MINIMUM + 25);
		verify(stagingProbe).probe();
	}

	@Test
	@DisplayName("Ровно настроенный резерв разрешает новую растущую операцию")
	void exactReserveAllowsGrowth() {
		when(stagingProbe.probe()).thenReturn(
				StagingStorageProbeResult.writable(STAGING_MINIMUM));
		when(indexGateway.minimumAvailableDiskBytes()).thenReturn(ELASTICSEARCH_MINIMUM);

		monitor.requireCapacity(StorageResource.STAGING);
		monitor.requireCapacity(StorageResource.ELASTICSEARCH);

		assertThat(meterRegistry.get("event_mosaic.storage.pressure")
				.tag("resource", "staging").gauge().value()).isZero();
		assertThat(meterRegistry.get("event_mosaic.storage.pressure")
				.tag("resource", "elasticsearch").gauge().value()).isZero();
		verify(indexGateway, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Недостаток места в staging блокирует операцию до Elasticsearch и не удаляет данные")
	void stagingPressureBlocksBeforeElasticsearchWithoutDelete() {
		when(stagingProbe.probe()).thenReturn(
				StagingStorageProbeResult.writable(STAGING_MINIMUM - 1));

		assertThatThrownBy(() -> monitor.requireCapacity(StorageResource.STAGING))
				.isInstanceOfSatisfying(StoragePressureException.class, exception -> {
					assertThat(exception.resource()).isEqualTo(StorageResource.STAGING);
					assertThat(exception.pressureState()).isEqualTo(StoragePressureState.PRESSURE);
				});

		assertThat(meterRegistry.get("event_mosaic.storage.pressure.blocks")
				.tag("resource", "staging").counter().count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.storage.pressure")
				.tag("resource", "staging").gauge().value()).isEqualTo(1);
		verifyNoInteractions(indexGateway);
	}

	@Test
	@DisplayName("Неподтвержденное место Elasticsearch блокирует рост как bounded unavailable state")
	void unavailableElasticsearchBlocksGrowth() {
		when(indexGateway.minimumAvailableDiskBytes()).thenThrow(
				new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE));

		assertThatThrownBy(() -> monitor.requireCapacity(StorageResource.ELASTICSEARCH))
				.isInstanceOfSatisfying(StoragePressureException.class, exception -> {
					assertThat(exception.resource()).isEqualTo(StorageResource.ELASTICSEARCH);
					assertThat(exception.pressureState())
							.isEqualTo(StoragePressureState.UNAVAILABLE);
				});

		assertThat(meterRegistry.get("event_mosaic.storage.available")
				.tag("resource", "elasticsearch").gauge().value()).isNaN();
		verify(indexGateway, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Неожиданный defect Elasticsearch gateway не маскируется как pressure")
	void unexpectedElasticsearchFailureEscapesGuard() {
		when(indexGateway.minimumAvailableDiskBytes()).thenThrow(
				new IllegalStateException("programming defect"));

		assertThatThrownBy(() -> monitor.requireCapacity(StorageResource.ELASTICSEARCH))
				.isExactlyInstanceOf(IllegalStateException.class);
	}

	@Test
	@DisplayName("Diagnostic observation сворачивает внешний сбой в bounded unavailable state")
	void diagnosticObservationFailsSafe() {
		when(indexGateway.minimumAvailableDiskBytes()).thenThrow(
				new IllegalStateException("external diagnostic"));

		assertThat(monitor.observe(StorageResource.ELASTICSEARCH).state())
				.isEqualTo(StoragePressureState.UNAVAILABLE);
	}

	@Test
	@DisplayName("Недоступный staging probe не раскрывает техническую причину через guard")
	void unavailableStagingFailsClosed() {
		when(stagingProbe.probe()).thenReturn(StagingStorageProbeResult.unavailable(
				StagingStorageProbeErrorCode.ROOT_ACCESS_FAILED));

		assertThatThrownBy(() -> monitor.requireCapacity(StorageResource.STAGING))
				.isInstanceOfSatisfying(StoragePressureException.class, exception ->
						assertThat(exception.pressureState())
								.isEqualTo(StoragePressureState.UNAVAILABLE));
	}

	private static BackendDataProperties properties() {
		BackendDataProperties defaults = GdeltTestFixtures.backendDataProperties();
		return new BackendDataProperties(
				defaults.partitionInterval(),
				defaults.retry(),
				defaults.operationDeadline(),
				defaults.receiptPageSize(),
				new BackendDataProperties.DiskPressure(
						STAGING_MINIMUM,
						ELASTICSEARCH_MINIMUM),
				defaults.rebuild(),
				defaults.cleanup());
	}
}
