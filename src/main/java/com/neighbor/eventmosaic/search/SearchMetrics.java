package com.neighbor.eventmosaic.search;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Измеряет поисковые операции с небольшим фиксированным набором результатов.
 */
@Component
final class SearchMetrics {

	private static final String DETAILS_DURATION_METER =
			"event_mosaic.search.details.duration";
	private static final String OUTCOME_TAG = "outcome";

	private final MeterRegistry meterRegistry;

	SearchMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(
				meterRegistry,
				"meterRegistry must not be null");
	}

	/** Начинает измерение одного запроса деталей события. */
	Timer.Sample startDetailsTimer() {
		return Timer.start(meterRegistry);
	}

	/** Завершает запрос с безопасным результатом без идентификатора события. */
	void detailsDuration(Timer.Sample sample, SearchDetailsMetricOutcome outcome) {
		Objects.requireNonNull(sample, "sample must not be null").stop(
				meterRegistry.timer(
						DETAILS_DURATION_METER,
						OUTCOME_TAG, tag(outcome)));
	}

	private static String tag(SearchDetailsMetricOutcome outcome) {
		return Objects.requireNonNull(outcome, "outcome must not be null")
				.name()
				.toLowerCase(Locale.ROOT);
	}
}
