package com.neighbor.eventmosaic.gdelt.csv;

import static com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind.TRANSLATION_EVENTS;
import static com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind.TRANSLATION_MENTIONS;
import static com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH;
import static com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode.INVALID_VALUE;
import static com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Collection;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Метрики потокового чтения GDELT CSV")
class GdeltCsvMetricsTest {

	private static final String RECORDS_METER = "event_mosaic.gdelt.csv.records";
	private static final String REJECTIONS_METER = "event_mosaic.gdelt.csv.rejections";
	private static final String FILES_METER = "event_mosaic.gdelt.csv.files";

	private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
	private final GdeltCsvMetrics metrics = new GdeltCsvMetrics(meterRegistry);

	@Test
	@DisplayName("Публикует точные partial counts только с ограниченными тегами")
	void publishesExactPartialCountsWithBoundedTags() {
		metrics.publish(
				TRANSLATION_MENTIONS,
				7,
				Map.of(
						FIELD_COUNT_MISMATCH, 2L,
						INVALID_VALUE, 3L,
						REQUIRED_VALUE_MISSING, 0L),
				GdeltCsvFileOutcome.SCHEMA_FAILED);

		assertThat(meterRegistry.get(RECORDS_METER)
				.tags("kind", "translation_mentions", "outcome", "valid")
				.counter()
				.count()).isEqualTo(7);
		assertThat(meterRegistry.get(RECORDS_METER)
				.tags("kind", "translation_mentions", "outcome", "invalid")
				.counter()
				.count()).isEqualTo(5);
		assertThat(meterRegistry.get(REJECTIONS_METER)
				.tags("kind", "translation_mentions", "reason", "field_count_mismatch")
				.counter()
				.count()).isEqualTo(2);
		assertThat(meterRegistry.get(REJECTIONS_METER)
				.tags("kind", "translation_mentions", "reason", "invalid_value")
				.counter()
				.count()).isEqualTo(3);
		assertThat(meterRegistry.find(REJECTIONS_METER)
				.tags("kind", "translation_mentions", "reason", "required_value_missing")
				.counter()).isNull();
		assertThat(meterRegistry.get(FILES_METER)
				.tags("kind", "translation_mentions", "outcome", "schema_failed")
				.counter()
				.count()).isEqualTo(1);

		Collection<Counter> recordCounters = meterRegistry.find(RECORDS_METER).counters();
		assertThat(recordCounters)
				.hasSize(2)
				.extracting(
						counter -> counter.getId().getTag("kind"),
						counter -> counter.getId().getTag("outcome"))
				.containsExactlyInAnyOrder(
						tuple("translation_mentions", "valid"),
						tuple("translation_mentions", "invalid"));
		assertTagKeys(recordCounters, "kind", "outcome");

		Collection<Counter> rejectionCounters = meterRegistry.find(REJECTIONS_METER).counters();
		assertThat(rejectionCounters)
				.hasSize(2)
				.extracting(
						counter -> counter.getId().getTag("kind"),
						counter -> counter.getId().getTag("reason"))
				.containsExactlyInAnyOrder(
						tuple("translation_mentions", "field_count_mismatch"),
						tuple("translation_mentions", "invalid_value"));
		assertTagKeys(rejectionCounters, "kind", "reason");

		Collection<Counter> fileCounters = meterRegistry.find(FILES_METER).counters();
		assertThat(fileCounters).hasSize(1);
		assertTagKeys(fileCounters, "kind", "outcome");
	}

	@Test
	@DisplayName("Всегда публикует каждый типизированный итог файла при нулевых records")
	void alwaysPublishesFileOutcomeWhenRecordCountsAreZero() {
		for (GdeltCsvFileOutcome outcome : GdeltCsvFileOutcome.values()) {
			metrics.publish(TRANSLATION_EVENTS, 0, Map.of(), outcome);
		}

		assertThat(meterRegistry.find(RECORDS_METER).counters()).isEmpty();
		assertThat(meterRegistry.find(REJECTIONS_METER).counters()).isEmpty();

		Collection<Counter> fileCounters = meterRegistry.find(FILES_METER).counters();
		assertThat(fileCounters)
				.hasSize(GdeltCsvFileOutcome.values().length)
				.allSatisfy(counter -> assertThat(counter.count()).isEqualTo(1))
				.extracting(
						counter -> counter.getId().getTag("kind"),
						counter -> counter.getId().getTag("outcome"))
				.containsExactlyInAnyOrder(
						tuple("translation_events", "completed"),
						tuple("translation_events", "schema_failed"),
						tuple("translation_events", "io_failed"),
						tuple("translation_events", "consumer_failed"),
						tuple("translation_events", "interrupted"));
		assertTagKeys(fileCounters, "kind", "outcome");
	}

	private static void assertTagKeys(Collection<Counter> counters, String... expectedKeys) {
		assertThat(counters)
				.isNotEmpty()
				.allSatisfy(counter -> assertThat(counter.getId().getTags())
						.extracting(Tag::getKey)
						.containsExactlyInAnyOrder(expectedKeys));
	}
}
