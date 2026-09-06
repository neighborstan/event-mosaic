package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Bounded gauges backend data lifecycle")
class BackendDataOperationalMetricsTest {

	@Test
	@DisplayName("Gauges используют только фиксированные owner, state и outcome tags")
	void gaugesUseOnlyFixedTags() {
		BackendDataOperationalState state = mock(BackendDataOperationalState.class);
		when(state.current()).thenReturn(snapshot(AliasConsistencyState.CONSISTENT));
		SimpleMeterRegistry registry = new SimpleMeterRegistry();

		new BackendDataOperationalMetrics(registry, state, activity());

		var retry = registry.get("event_mosaic.pipeline.retries")
				.tags("owner", "acquisition", "state", "deferred")
				.gauge();
		assertThat(retry.value()).isZero();
		assertThat(retry.getId().getTags()).containsExactlyInAnyOrderElementsOf(
				Tags.of("owner", "acquisition", "state", "deferred").stream().toList());
		assertThat(registry.get("event_mosaic.indexing.generations")
				.tag("state", "active").gauge().value()).isZero();
		assertThat(registry.get("event_mosaic.indexing.alias.consistency")
				.gauge().value()).isEqualTo(1);
		assertThat(registry.get("event_mosaic.ingestion.source.poll.age").gauge().value()).isEqualTo(1200);
		assertThat(registry.get("event_mosaic.ingestion.bootstrap.remaining")
				.tag("type", "event").gauge().value()).isEqualTo(2);
		assertThat(registry.get("event_mosaic.ingestion.bootstrap.remaining")
				.tag("type", "mention").gauge().value()).isEqualTo(5);
		assertThat(registry.get("event_mosaic.pipeline.retries")
				.tags("owner", "receipt_audit", "state", "deferred").gauge().value()).isEqualTo(1);
		assertThat(registry.getMeters()).allSatisfy(meter ->
				assertThat(meter.getId().getTags()).allSatisfy(tag -> {
					assertThat(tag.getKey()).isIn("owner", "state", "outcome", "type");
					assertThat(tag.getValue()).isIn("source_poll", "acquisition", "processing", "receipt_audit", "due", "deferred",
							"exhausted", "permanent", "mismatch", "surplus", "building", "active", "superseded",
							"required", "open", "event", "mention");
				}));
	}

	@Test
	@DisplayName("Alias incident counter растет только при новом переходе в incident")
	void aliasIncidentCounterRecordsTransitionsOnly() {
		BackendDataOperationalState state = mock(BackendDataOperationalState.class);
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		BackendDataOperationalMetrics metrics = new BackendDataOperationalMetrics(
				registry,
				state,
				activity());
		BackendDataOperationalSnapshot incident = snapshot(AliasConsistencyState.INCIDENT);

		metrics.observe(incident);
		metrics.observe(incident);
		metrics.observe(snapshot(AliasConsistencyState.CONSISTENT));
		metrics.observe(incident);

		assertThat(registry.get("event_mosaic.indexing.alias.consistency.incidents")
				.counter().count()).isEqualTo(2);
	}

	private static BackendDataOperationalSnapshot snapshot(AliasConsistencyState aliasState) {
		return new BackendDataOperationalSnapshot(
				true,
				0,
				0,
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0),
				0,
				new BackendDataOperationalSnapshot.ReceiptCounts(0, 0),
				new BackendDataOperationalSnapshot.GenerationCounts(0, 0, 0),
				0,
				0,
				aliasState,
				new BackendDataOperationalSnapshot.LiveCounts(1200, 1200, 0, 60, false, true, 2, 5,
						new BackendDataOperationalSnapshot.RetryCounts(0, 1, 0)));
	}

	private static IngestionCycleActivity activity() {
		IngestionCycleActivity activity = mock(IngestionCycleActivity.class);
		when(activity.observe()).thenReturn(new IngestionCycleActivity.Observation(false, false, false, 0, 0));
		return activity;
	}
}
