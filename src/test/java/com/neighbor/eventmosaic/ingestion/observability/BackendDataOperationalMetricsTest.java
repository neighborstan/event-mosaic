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

		new BackendDataOperationalMetrics(registry, state);

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
	}

	@Test
	@DisplayName("Alias incident counter растет только при новом переходе в incident")
	void aliasIncidentCounterRecordsTransitionsOnly() {
		BackendDataOperationalState state = mock(BackendDataOperationalState.class);
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		BackendDataOperationalMetrics metrics = new BackendDataOperationalMetrics(
				registry,
				state);
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
				aliasState);
	}
}
