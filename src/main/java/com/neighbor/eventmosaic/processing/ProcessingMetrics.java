package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Публикует processing counters только с bounded enum-backed tags.
 */
@Component
final class ProcessingMetrics {

	private static final String KIND_TAG = "kind";
	private static final String OUTCOME_TAG = "outcome";

	private final MeterRegistry meterRegistry;

	ProcessingMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(
				meterRegistry,
				"meterRegistry must not be null");
	}

	void mapped(GdeltArchiveKind kind) {
		incrementRecord(kind, "mapped");
	}

	void rejected(GdeltArchiveKind kind, ProcessingMappingRejection rejection) {
		Objects.requireNonNull(rejection, "rejection must not be null");
		incrementRecord(kind, "rejected");
		meterRegistry.counter(
				"event_mosaic.processing.rejections",
				KIND_TAG, tag(kind),
				"reason", tag(rejection)
		).increment();
	}

	void invalidGeoCandidates(GdeltArchiveKind kind, int count) {
		if (count > 0) {
			meterRegistry.counter(
					"event_mosaic.processing.geo_candidates",
					KIND_TAG, tag(kind),
					OUTCOME_TAG, "invalid"
			).increment(count);
		}
	}

	void archiveOutcome(GdeltArchiveKind kind, ArchiveProcessingOutcome outcome) {
		meterRegistry.counter(
				"event_mosaic.processing.archives",
				KIND_TAG, tag(kind),
				OUTCOME_TAG, tag(outcome)
		).increment();
	}

	void failure(GdeltArchiveKind kind, ArchiveProcessingErrorCode code) {
		meterRegistry.counter(
				"event_mosaic.processing.failures",
				KIND_TAG, tag(kind),
				"code", tag(code)
		).increment();
	}

	private void incrementRecord(GdeltArchiveKind kind, String outcome) {
		meterRegistry.counter(
				"event_mosaic.processing.records",
				KIND_TAG, tag(kind),
				OUTCOME_TAG, outcome
		).increment();
	}

	private static String tag(Enum<?> value) {
		return Objects.requireNonNull(value, "metric tag value must not be null")
				.name()
				.toLowerCase(Locale.ROOT);
	}
}
