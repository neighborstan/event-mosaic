package com.neighbor.eventmosaic.ingestion.observability;

import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.staging.StagingStorageProbe;
import com.neighbor.eventmosaic.ingestion.staging.StagingStorageProbeResult;
import jakarta.annotation.PostConstruct;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Использует единые pressure semantics для operational health, gauges и guards
 * перед новыми storage-growing operations.
 */
@Component
public class BackendDataStorageMonitor {

	private static final Logger LOGGER = LoggerFactory.getLogger(
			BackendDataStorageMonitor.class);

	private final StagingStorageProbe stagingProbe;
	private final IndexMaintenanceGateway indexGateway;
	private final BackendDataProperties properties;
	private final BackendDataStorageMetrics metrics;

	/** Создает monitor поверх staging probe и official Elasticsearch gateway. */
	public BackendDataStorageMonitor(
			StagingStorageProbe stagingProbe,
			IndexMaintenanceGateway indexGateway,
			BackendDataProperties properties,
			BackendDataStorageMetrics metrics
	) {
		this.stagingProbe = Objects.requireNonNull(stagingProbe, "stagingProbe must not be null");
		this.indexGateway = Objects.requireNonNull(indexGateway, "indexGateway must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	@PostConstruct
	void bindMetrics() {
		metrics.bind(this::observeForMetrics);
	}

	/** Проверяет оба storage ресурса и обновляет bounded gauges. */
	public BackendDataStorageSnapshot observe() {
		return new BackendDataStorageSnapshot(
				observe(StorageResource.STAGING),
				observe(StorageResource.ELASTICSEARCH));
	}

	/** Проверяет один ресурс и обновляет его bounded gauges. */
	public StoragePressureObservation observe(StorageResource resource) {
		Objects.requireNonNull(resource, "resource must not be null");
		StoragePressureObservation observation = observeDiagnostic(resource);
		metrics.observe(observation);
		return observation;
	}

	/**
	 * Fail-closed запрещает начало новой растущей operation при pressure или
	 * неподтвержденной доступности, не изменяя durable state.
	 */
	public void requireCapacity(StorageResource resource) {
		Objects.requireNonNull(resource, "resource must not be null");
		StoragePressureObservation observation = switch (resource) {
			case STAGING -> observeStaging();
			case ELASTICSEARCH -> observeElasticsearchForGuard();
		};
		metrics.observe(observation);
		if (observation.blocksGrowth()) {
			metrics.blocked(resource);
			LOGGER.atWarn()
					.addKeyValue("event", "backend_data.storage_growth_blocked")
					.addKeyValue("resource", resource)
					.addKeyValue("state", observation.state())
					.log("Backend data storage guard blocked a new growing operation");
			throw new StoragePressureException(resource, observation.state());
		}
	}

	private StoragePressureObservation observeForMetrics(StorageResource resource) {
		return observeDiagnostic(resource);
	}

	private StoragePressureObservation observeDiagnostic(StorageResource resource) {
		try {
			return switch (resource) {
				case STAGING -> observeStaging();
				case ELASTICSEARCH -> observeElasticsearch();
			};
		}
		catch (RuntimeException _) {
			return unavailable(resource, minimum(resource));
		}
	}

	private StoragePressureObservation observeStaging() {
		long minimum = properties.diskPressure().stagingMinFreeBytes();
		StagingStorageProbeResult result = stagingProbe.probe();
		if (!result.isWritable()) {
			return unavailable(StorageResource.STAGING, minimum);
		}
		return classify(StorageResource.STAGING, result.usableBytes(), minimum);
	}

	private StoragePressureObservation observeElasticsearch() {
		long minimum = properties.diskPressure().elasticsearchMinFreeBytes();
		long available = indexGateway.minimumAvailableDiskBytes();
		if (available < 0) {
			return unavailable(StorageResource.ELASTICSEARCH, minimum);
		}
		return classify(StorageResource.ELASTICSEARCH, available, minimum);
	}

	private StoragePressureObservation observeElasticsearchForGuard() {
		try {
			return observeElasticsearch();
		}
		catch (IndexingAccessException | IndexingProtocolException _) {
			return unavailable(
					StorageResource.ELASTICSEARCH,
					properties.diskPressure().elasticsearchMinFreeBytes());
		}
	}

	private long minimum(StorageResource resource) {
		return switch (resource) {
			case STAGING -> properties.diskPressure().stagingMinFreeBytes();
			case ELASTICSEARCH -> properties.diskPressure().elasticsearchMinFreeBytes();
		};
	}

	private static StoragePressureObservation classify(
			StorageResource resource,
			long available,
			long minimum
	) {
		StoragePressureState state = available < minimum
				? StoragePressureState.PRESSURE
				: StoragePressureState.AVAILABLE;
		return new StoragePressureObservation(resource, state, available, minimum);
	}

	private static StoragePressureObservation unavailable(
			StorageResource resource,
			long minimum
	) {
		return new StoragePressureObservation(
				resource,
				StoragePressureState.UNAVAILABLE,
				-1,
				minimum);
	}
}
