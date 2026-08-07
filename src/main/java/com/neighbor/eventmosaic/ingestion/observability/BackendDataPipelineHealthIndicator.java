package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.boot.health.contributor.Status;
import org.springframework.stereotype.Component;

/**
 * Показывает bounded operational state pipeline отдельно от process liveness и
 * обязательной infrastructure readiness.
 */
@Component
public class BackendDataPipelineHealthIndicator implements HealthIndicator {

	private static final Status DEGRADED = new Status("DEGRADED");

	private final BackendDataOperationalState operationalState;
	private final BackendDataOperationalMetrics operationalMetrics;
	private final BackendDataStorageMonitor storageMonitor;

	/** Собирает indicator из общей lifecycle и storage проекций. */
	public BackendDataPipelineHealthIndicator(
			BackendDataOperationalState operationalState,
			BackendDataOperationalMetrics operationalMetrics,
			BackendDataStorageMonitor storageMonitor
	) {
		this.operationalState = Objects.requireNonNull(
				operationalState,
				"operationalState must not be null");
		this.operationalMetrics = Objects.requireNonNull(
				operationalMetrics,
				"operationalMetrics must not be null");
		this.storageMonitor = Objects.requireNonNull(
				storageMonitor,
				"storageMonitor must not be null");
	}

	/**
	 * Возвращает UP, DEGRADED или UNKNOWN без high-cardinality details. Pipeline
	 * state никогда не превращает remote outage или pressure в DOWN readiness.
	 */
	@Override
	public Health health() {
		BackendDataOperationalSnapshot pipeline = operationalState.observe();
		operationalMetrics.observe(pipeline);
		BackendDataStorageSnapshot storage = storageMonitor.observe();
		Status status = status(pipeline, storage);
		return Health.status(status)
				.withDetail("state", status.getCode())
				.withDetail("lagSeconds", pipeline.lagSeconds())
				.withDetail("openGaps", pipeline.openGaps())
				.withDetail("retryDue", pipeline.retryDue())
				.withDetail("retryDeferred", pipeline.retryDeferred())
				.withDetail("retryExhausted", pipeline.retryExhausted())
				.withDetail("permanentFailures", pipeline.permanentFailures())
				.withDetail("receiptMismatches", pipeline.receipts().mismatch())
				.withDetail("receiptSurpluses", pipeline.receipts().surplus())
				.withDetail("buildingGenerations", pipeline.generations().building())
				.withDetail("activeGenerations", pipeline.generations().active())
				.withDetail("supersededGenerations", pipeline.generations().superseded())
				.withDetail("repairRequiredPartitions", pipeline.repairRequiredPartitions())
				.withDetail("openMaintenanceOperations", pipeline.openMaintenanceOperations())
				.withDetail("aliasConsistency", pipeline.aliasConsistency().name())
				.withDetail("stagingPressure", storage.staging().state().name())
				.withDetail(
						"elasticsearchPressure",
						storage.elasticsearch().state().name())
				.build();
	}

	private static Status status(
			BackendDataOperationalSnapshot pipeline,
			BackendDataStorageSnapshot storage
	) {
		if (!pipeline.databaseAvailable()
				|| pipeline.aliasConsistency() == AliasConsistencyState.UNAVAILABLE
				|| storage.staging().state() == StoragePressureState.UNAVAILABLE
				|| storage.elasticsearch().state() == StoragePressureState.UNAVAILABLE) {
			return Status.UNKNOWN;
		}
		if (isDegraded(pipeline, storage)) {
			return DEGRADED;
		}
		return Status.UP;
	}

	private static boolean isDegraded(
			BackendDataOperationalSnapshot pipeline,
			BackendDataStorageSnapshot storage
	) {
		return pipeline.lagSeconds() > 0
				|| pipeline.openGaps() > 0
				|| pipeline.retryDue() > 0
				|| pipeline.retryDeferred() > 0
				|| pipeline.retryExhausted() > 0
				|| pipeline.permanentFailures() > 0
				|| pipeline.receipts().mismatch() > 0
				|| pipeline.receipts().surplus() > 0
				|| pipeline.repairRequiredPartitions() > 0
				|| pipeline.aliasConsistency() == AliasConsistencyState.INCIDENT
				|| storage.staging().state() == StoragePressureState.PRESSURE
				|| storage.elasticsearch().state() == StoragePressureState.PRESSURE;
	}
}
