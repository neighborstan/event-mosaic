package com.neighbor.eventmosaic.ingestion.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToLongFunction;
import org.springframework.stereotype.Component;

/**
 * Регистрирует только фиксированный набор gauges backend data lifecycle и один
 * counter переходов в alias-consistency incident.
 */
@Component
public class BackendDataOperationalMetrics {

	private static final String OWNER_TAG = "owner";
	private static final String STATE_TAG = "state";

	private final BackendDataOperationalState operationalState;
	private final Counter aliasIncidents;
	private final AtomicReference<AliasConsistencyState> previousAliasState =
			new AtomicReference<>(AliasConsistencyState.UNAVAILABLE);

	/** Регистрирует bounded gauges поверх общего application registry. */
	public BackendDataOperationalMetrics(
			MeterRegistry meterRegistry,
			BackendDataOperationalState operationalState,
			IngestionCycleActivity cycleActivity
	) {
		this.operationalState = Objects.requireNonNull(
				operationalState,
				"operationalState must not be null");
		Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
		Objects.requireNonNull(cycleActivity, "cycleActivity must not be null");
		Gauge.builder("event_mosaic.ingestion.automatic.enabled", cycleActivity,
				activity -> activity.observe().automaticEnabled() ? 1 : 0).register(meterRegistry);
		Gauge.builder("event_mosaic.ingestion.cycle.running", cycleActivity,
				activity -> activity.observe().running() ? 1 : 0).register(meterRegistry);
		Gauge.builder("event_mosaic.ingestion.scheduler.stale", cycleActivity,
				activity -> activity.observe().stale() ? 1 : 0).register(meterRegistry);
		Gauge.builder("event_mosaic.ingestion.scheduler.terminal.age", cycleActivity,
				activity -> activity.observe().terminalAgeSeconds()).baseUnit("seconds").register(meterRegistry);
		Gauge.builder("event_mosaic.ingestion.cycle.next.delay", cycleActivity,
				activity -> activity.observe().nextDelaySeconds()).baseUnit("seconds").register(meterRegistry);
		aliasIncidents = meterRegistry.counter(
				"event_mosaic.indexing.alias.consistency.incidents");

		gauge(meterRegistry, "event_mosaic.pipeline.lag", "seconds", null, null,
				BackendDataOperationalSnapshot::lagSeconds);
		gauge(meterRegistry, "event_mosaic.pipeline.gaps", null, null, null,
				BackendDataOperationalSnapshot::openGaps);
		gauge(meterRegistry, "event_mosaic.ingestion.source.poll.age", "seconds", null, null,
				snapshot -> snapshot.live().successfulPollAgeSeconds());
		gauge(meterRegistry, "event_mosaic.ingestion.source.outage.age", "seconds", null, null,
				snapshot -> snapshot.live().sourceOutageAgeSeconds());
		gauge(meterRegistry, "event_mosaic.ingestion.source.lag", "seconds", null, null,
				snapshot -> snapshot.live().sourceLagSeconds());
		gauge(meterRegistry, "event_mosaic.ingestion.source.retry.delay", "seconds", null, null,
				snapshot -> snapshot.live().sourceRetryDelaySeconds());
		gauge(meterRegistry, "event_mosaic.ingestion.source.cooldown", null, null, null,
				snapshot -> snapshot.live().sourceCooldown() ? 1 : 0);
		gauge(meterRegistry, "event_mosaic.ingestion.bootstrap.catalog.pending", null, null, null,
				snapshot -> snapshot.live().catalogPending() ? 1 : 0);
		gauge(meterRegistry, "event_mosaic.ingestion.bootstrap.remaining", null, "type", "event",
				snapshot -> snapshot.live().eventBootstrapRemaining());
		gauge(meterRegistry, "event_mosaic.ingestion.bootstrap.remaining", null, "type", "mention",
				snapshot -> snapshot.live().mentionBootstrapRemaining());
		gauge(meterRegistry, "event_mosaic.pipeline.failures", null, "state", "permanent",
				BackendDataOperationalSnapshot::permanentFailures);
		gauge(meterRegistry, "event_mosaic.indexing.receipt.state", null,
				"outcome", "mismatch", snapshot -> snapshot.receipts().mismatch());
		gauge(meterRegistry, "event_mosaic.indexing.receipt.state", null,
				"outcome", "surplus", snapshot -> snapshot.receipts().surplus());
		gauge(meterRegistry, "event_mosaic.indexing.generations", null,
				STATE_TAG, "building", snapshot -> snapshot.generations().building());
		gauge(meterRegistry, "event_mosaic.indexing.generations", null,
				STATE_TAG, "active", snapshot -> snapshot.generations().active());
		gauge(meterRegistry, "event_mosaic.indexing.generations", null,
				STATE_TAG, "superseded", snapshot -> snapshot.generations().superseded());
		gauge(meterRegistry, "event_mosaic.indexing.repairs", null,
				STATE_TAG, "required", BackendDataOperationalSnapshot::repairRequiredPartitions);
		gauge(meterRegistry, "event_mosaic.maintenance.operations", null,
				STATE_TAG, "open", BackendDataOperationalSnapshot::openMaintenanceOperations);

		registerRetryGauges(meterRegistry);
		Gauge.builder(
				"event_mosaic.indexing.alias.consistency",
				this,
				BackendDataOperationalMetrics::readAliasConsistency)
				.register(meterRegistry);
	}

	/** Учитывает новый snapshot при явном pipeline health observation. */
	public void observe(BackendDataOperationalSnapshot snapshot) {
		Objects.requireNonNull(snapshot, "snapshot must not be null");
		observeAliasTransition(snapshot.aliasConsistency());
	}

	private void registerRetryGauges(MeterRegistry meterRegistry) {
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "receipt_audit", STATE_TAG, "due", snapshot -> snapshot.live().receiptAuditRetries().due());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "receipt_audit", STATE_TAG, "deferred", snapshot -> snapshot.live().receiptAuditRetries().deferred());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "receipt_audit", STATE_TAG, "exhausted", snapshot -> snapshot.live().receiptAuditRetries().exhausted());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "source_poll", STATE_TAG, "due",
				snapshot -> snapshot.sourcePollRetries().due());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "source_poll", STATE_TAG, "deferred",
				snapshot -> snapshot.sourcePollRetries().deferred());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "source_poll", STATE_TAG, "exhausted",
				snapshot -> snapshot.sourcePollRetries().exhausted());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "acquisition", STATE_TAG, "due",
				snapshot -> snapshot.acquisitionRetries().due());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "acquisition", STATE_TAG, "deferred",
				snapshot -> snapshot.acquisitionRetries().deferred());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "acquisition", STATE_TAG, "exhausted",
				snapshot -> snapshot.acquisitionRetries().exhausted());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "processing", STATE_TAG, "due",
				snapshot -> snapshot.processingRetries().due());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "processing", STATE_TAG, "deferred",
				snapshot -> snapshot.processingRetries().deferred());
		gauge(meterRegistry, "event_mosaic.pipeline.retries", null,
				OWNER_TAG, "processing", STATE_TAG, "exhausted",
				snapshot -> snapshot.processingRetries().exhausted());
	}

	private void gauge(
			MeterRegistry meterRegistry,
			String name,
			String baseUnit,
			String firstTagKey,
			String firstTagValue,
			ToLongFunction<BackendDataOperationalSnapshot> value
	) {
		Gauge.Builder<BackendDataOperationalState> builder = Gauge.builder(
				name,
				operationalState,
				state -> metricValue(state.current(), value));
		if (baseUnit != null) {
			builder.baseUnit(baseUnit);
		}
		if (firstTagKey != null) {
			builder.tag(firstTagKey, firstTagValue);
		}
		builder.register(meterRegistry);
	}

	private void gauge(
			MeterRegistry meterRegistry,
			String name,
			String baseUnit,
			String firstTagKey,
			String firstTagValue,
			String secondTagKey,
			String secondTagValue,
			ToLongFunction<BackendDataOperationalSnapshot> value
	) {
		Gauge.Builder<BackendDataOperationalState> builder = Gauge.builder(
				name,
				operationalState,
				state -> metricValue(state.current(), value));
		if (baseUnit != null) {
			builder.baseUnit(baseUnit);
		}
		builder.tag(firstTagKey, firstTagValue);
		builder.tag(secondTagKey, secondTagValue);
		builder.register(meterRegistry);
	}

	private static double metricValue(
			BackendDataOperationalSnapshot snapshot,
			ToLongFunction<BackendDataOperationalSnapshot> value
	) {
		return snapshot.databaseAvailable()
				? value.applyAsLong(snapshot)
				: Double.NaN;
	}

	private double readAliasConsistency() {
		BackendDataOperationalSnapshot snapshot = operationalState.current();
		AliasConsistencyState state = snapshot.aliasConsistency();
		observeAliasTransition(state);
		return switch (state) {
			case CONSISTENT, MAINTENANCE -> 1.0;
			case INCIDENT -> 0.0;
			case UNAVAILABLE -> Double.NaN;
		};
	}

	private void observeAliasTransition(AliasConsistencyState current) {
		AliasConsistencyState previous = previousAliasState.getAndSet(current);
		if (current == AliasConsistencyState.INCIDENT
				&& previous != AliasConsistencyState.INCIDENT) {
			aliasIncidents.increment();
		}
	}

}
