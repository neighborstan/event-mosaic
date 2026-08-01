package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Метрики Elasticsearch bulk indexing")
class IndexingMetricsTest {

	private static final String REQUESTS_METER = "event_mosaic.indexing.bulk.requests";
	private static final String DOCUMENTS_METER = "event_mosaic.indexing.bulk.documents";
	private static final String EVENT_IDENTITY_GUARD_METER =
			"event_mosaic.indexing.event_identity_guard";

	private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
	private final IndexingMetrics metrics = new IndexingMetrics(meterRegistry);

	@Test
	@DisplayName("Публикует counts только с enum-backed kind и outcome")
	void publishesOnlyBoundedTags() {
		metrics.completed(new BulkIndexResult(
				GdeltIndexKind.MENTION,
				5,
				3,
				2,
				17L,
				BulkIndexOutcome.RETRYABLE_PARTIAL_FAILURE));
		metrics.failed(GdeltIndexKind.EVENT, 4, false);

		Collection<Counter> requestCounters = meterRegistry.find(REQUESTS_METER).counters();
		assertThat(requestCounters)
				.extracting(
						counter -> counter.getId().getTag("kind"),
						counter -> counter.getId().getTag("outcome"),
						Counter::count)
				.containsExactlyInAnyOrder(
						tuple("mention", "retryable_partial_failure", 1.0),
						tuple("event", "non_retryable_failure", 1.0));
		assertOnlyTagKeys(requestCounters);

		Collection<Counter> documentCounters = meterRegistry.find(DOCUMENTS_METER).counters();
		assertThat(documentCounters)
				.extracting(
						counter -> counter.getId().getTag("kind"),
						counter -> counter.getId().getTag("outcome"),
						Counter::count)
				.containsExactlyInAnyOrder(
						tuple("mention", "succeeded", 3.0),
						tuple("mention", "retryable_failure", 2.0),
						tuple("event", "unknown", 4.0));
		assertOnlyTagKeys(documentCounters);
	}

	@Test
	@DisplayName("Измеряет Event identity guard только с bounded outcome")
	void measuresEventIdentityGuardWithBoundedOutcome() {
		metrics.eventIdentityGuard(12_000, EventIdentityGuardMetricOutcome.REPLAY);

		Timer timer = meterRegistry.find(EVENT_IDENTITY_GUARD_METER)
				.tag("outcome", "replay")
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(timer.getId().getTags())
				.extracting(Tag::getKey)
				.containsExactly("outcome");
	}

	private static void assertOnlyTagKeys(Collection<Counter> counters) {
		assertThat(counters)
				.isNotEmpty()
				.allSatisfy(counter -> assertThat(counter.getId().getTags())
						.extracting(Tag::getKey)
						.containsExactlyInAnyOrder("kind", "outcome"));
	}

}
