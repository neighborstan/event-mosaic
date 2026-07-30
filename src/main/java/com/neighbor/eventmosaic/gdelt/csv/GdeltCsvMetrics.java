package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Публикует агрегированные CSV counters только с типизированными
 * low-cardinality tags.
 */
@Component
final class GdeltCsvMetrics {

	private static final String RECORDS_METER = "event_mosaic.gdelt.csv.records";
	private static final String REJECTIONS_METER = "event_mosaic.gdelt.csv.rejections";
	private static final String FILES_METER = "event_mosaic.gdelt.csv.files";
	private static final String KIND_TAG = "kind";
	private static final String OUTCOME_TAG = "outcome";
	private static final String REASON_TAG = "reason";

	private final MeterRegistry meterRegistry;

	GdeltCsvMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
	}

	/**
	 * Публикует частичные или итоговые counts одного файла и всегда фиксирует
	 * ровно один file outcome.
	 *
	 * @param kind тип CSV archive
	 * @param validRecords число корректно разобранных records
	 * @param rejectionCounts counts локальных отклонений по bounded-причинам
	 * @param fileOutcome итог чтения файла
	 */
	void publish(
			GdeltArchiveKind kind,
			long validRecords,
			Map<GdeltCsvRecordErrorCode, Long> rejectionCounts,
			GdeltCsvFileOutcome fileOutcome
	) {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(fileOutcome, "fileOutcome must not be null");
		Map<GdeltCsvRecordErrorCode, Long> counts = Map.copyOf(
				Objects.requireNonNull(rejectionCounts, "rejectionCounts must not be null"));
		if (validRecords < 0) {
			throw new IllegalArgumentException("validRecords must not be negative");
		}

		long invalidRecords = 0;
		for (Map.Entry<GdeltCsvRecordErrorCode, Long> entry : counts.entrySet()) {
			long count = entry.getValue();
			if (count < 0) {
				throw new IllegalArgumentException("rejection count must not be negative");
			}
			invalidRecords = Math.addExact(invalidRecords, count);
		}

		String kindTag = tag(kind);
		if (validRecords > 0) {
			meterRegistry.counter(
					RECORDS_METER,
					KIND_TAG, kindTag,
					OUTCOME_TAG, tag(GdeltCsvRecordOutcome.VALID)
			).increment(validRecords);
		}
		if (invalidRecords > 0) {
			meterRegistry.counter(
					RECORDS_METER,
					KIND_TAG, kindTag,
					OUTCOME_TAG, tag(GdeltCsvRecordOutcome.INVALID)
			).increment(invalidRecords);
		}
		for (Map.Entry<GdeltCsvRecordErrorCode, Long> entry : counts.entrySet()) {
			long count = entry.getValue();
			if (count > 0) {
				meterRegistry.counter(
						REJECTIONS_METER,
						KIND_TAG, kindTag,
						REASON_TAG, tag(entry.getKey())
				).increment(count);
			}
		}
		meterRegistry.counter(
				FILES_METER,
				KIND_TAG, kindTag,
				OUTCOME_TAG, tag(fileOutcome)
		).increment();
	}

	private static String tag(Enum<?> value) {
		return value.name().toLowerCase(Locale.ROOT);
	}
}
