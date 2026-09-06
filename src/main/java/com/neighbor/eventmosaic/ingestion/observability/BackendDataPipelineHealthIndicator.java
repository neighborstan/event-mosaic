package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
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
	private final IngestionCycleActivity cycleActivity;
	private final long sourceOutageThresholdSeconds;

	/** Собирает indicator из общей lifecycle и storage проекций. */
	public BackendDataPipelineHealthIndicator(
			BackendDataOperationalState operationalState,
			BackendDataOperationalMetrics operationalMetrics,
			BackendDataStorageMonitor storageMonitor,
			IngestionCycleActivity cycleActivity,
			GdeltIngestionProperties properties
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
		this.cycleActivity = Objects.requireNonNull(cycleActivity, "cycleActivity must not be null");
		this.sourceOutageThresholdSeconds = properties.automatic().sourceOutageThreshold().toSeconds();
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
		IngestionCycleActivity.Observation activity = cycleActivity.observe();
		boolean sourceOutage = pipeline.live().sourceOutageAgeSeconds() >= sourceOutageThresholdSeconds;
		Status status = status(pipeline, storage, activity, sourceOutage);
		return Health.status(status)
				.withDetail("state", status.getCode())
				.withDetail("lagSeconds", pipeline.lagSeconds())
				.withDetail("openGaps", pipeline.openGaps())
				.withDetail("automaticEnabled", activity.automaticEnabled())
				.withDetail("cycleRunning", activity.running())
				.withDetail("schedulerStale", activity.stale())
				.withDetail("terminalActivityAgeSeconds", activity.terminalAgeSeconds())
				.withDetail("nextCycleDelaySeconds", activity.nextDelaySeconds())
				.withDetail("sourceOutage", sourceOutage)
				.withDetail("successfulPollAgeSeconds", pipeline.live().successfulPollAgeSeconds())
				.withDetail("sourceLagSeconds", pipeline.live().sourceLagSeconds())
				.withDetail("sourceRetryDelaySeconds", pipeline.live().sourceRetryDelaySeconds())
				.withDetail("sourceCooldown", pipeline.live().sourceCooldown())
				.withDetail("catalogPending", pipeline.live().catalogPending())
				.withDetail("eventBootstrapRemaining", pipeline.live().eventBootstrapRemaining())
				.withDetail("mentionBootstrapRemaining", pipeline.live().mentionBootstrapRemaining())
				.withDetail("retryDue", pipeline.retryDue())
				.withDetail("retryDeferred", pipeline.retryDeferred())
				.withDetail("retryExhausted", pipeline.retryExhausted())
				.withDetail("receiptAuditRetryDue", pipeline.live().receiptAuditRetries().due())
				.withDetail("receiptAuditRetryDeferred", pipeline.live().receiptAuditRetries().deferred())
				.withDetail("receiptAuditRetryExhausted", pipeline.live().receiptAuditRetries().exhausted())
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
			BackendDataStorageSnapshot storage,
			IngestionCycleActivity.Observation activity,
			boolean sourceOutage
	) {
		if (!pipeline.databaseAvailable()
				|| pipeline.aliasConsistency() == AliasConsistencyState.UNAVAILABLE
				|| storage.staging().state() == StoragePressureState.UNAVAILABLE
				|| storage.elasticsearch().state() == StoragePressureState.UNAVAILABLE) {
			return Status.UNKNOWN;
		}
		if (isDegraded(pipeline, storage) || activity.stale() || sourceOutage) {
			return DEGRADED;
		}
		return Status.UP;
	}

	private static boolean isDegraded(
			BackendDataOperationalSnapshot pipeline,
			BackendDataStorageSnapshot storage
	) {
		return pipeline.lagSeconds() > 0
				|| pipeline.live().sourceLagSeconds() > 0
				|| pipeline.live().catalogPending()
				|| pipeline.live().eventBootstrapRemaining() > 0
				|| pipeline.live().mentionBootstrapRemaining() > 0
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
