package com.neighbor.eventmosaic.indexing;

import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Публикует indexing counters только с enum-backed low-cardinality tags.
 */
@Component
final class IndexingMetrics {

	private static final String REQUESTS_METER = "event_mosaic.indexing.bulk.requests";
	private static final String DOCUMENTS_METER = "event_mosaic.indexing.bulk.documents";
	private static final String KIND_TAG = "kind";
	private static final String OUTCOME_TAG = "outcome";

	private final MeterRegistry meterRegistry;

	IndexingMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
	}

	/**
	 * Публикует подтвержденный bulk response.
	 *
	 * @param result полностью проанализированный результат
	 */
	void completed(BulkIndexResult result) {
		Objects.requireNonNull(result, "result must not be null");
		String kind = tag(result.kind());
		meterRegistry.counter(
				REQUESTS_METER,
				KIND_TAG, kind,
				OUTCOME_TAG, tag(requestOutcome(result.outcome()))
		).increment();

		incrementDocuments(kind, IndexingDocumentMetricOutcome.SUCCEEDED, result.succeeded());
		if (result.failed() > 0) {
			IndexingDocumentMetricOutcome failedOutcome = result.retryable()
					? IndexingDocumentMetricOutcome.RETRYABLE_FAILURE
					: IndexingDocumentMetricOutcome.NON_RETRYABLE_FAILURE;
			incrementDocuments(kind, failedOutcome, result.failed());
		}
	}

	/**
	 * Публикует request-level failure без подтвержденных item outcomes.
	 *
	 * @param kind вид документов
	 * @param submitted число отправленных элементов с неизвестным результатом
	 * @param retryable допустимость повтора owning operation
	 */
	void failed(GdeltIndexKind kind, long submitted, boolean retryable) {
		Objects.requireNonNull(kind, "kind must not be null");
		if (submitted <= 0) {
			throw new IllegalArgumentException("submitted must be positive");
		}
		String kindTag = tag(kind);
		IndexingRequestMetricOutcome outcome = retryable
				? IndexingRequestMetricOutcome.RETRYABLE_FAILURE
				: IndexingRequestMetricOutcome.NON_RETRYABLE_FAILURE;
		meterRegistry.counter(
				REQUESTS_METER,
				KIND_TAG, kindTag,
				OUTCOME_TAG, tag(outcome)
		).increment();
		incrementDocuments(kindTag, IndexingDocumentMetricOutcome.UNKNOWN, submitted);
	}

	private void incrementDocuments(
			String kind,
			IndexingDocumentMetricOutcome outcome,
			long count
	) {
		if (count > 0) {
			meterRegistry.counter(
					DOCUMENTS_METER,
					KIND_TAG, kind,
					OUTCOME_TAG, tag(outcome)
			).increment(count);
		}
	}

	private static IndexingRequestMetricOutcome requestOutcome(BulkIndexOutcome outcome) {
		return switch (outcome) {
			case SUCCEEDED -> IndexingRequestMetricOutcome.SUCCEEDED;
			case RETRYABLE_PARTIAL_FAILURE ->
					IndexingRequestMetricOutcome.RETRYABLE_PARTIAL_FAILURE;
			case NON_RETRYABLE_PARTIAL_FAILURE ->
					IndexingRequestMetricOutcome.NON_RETRYABLE_PARTIAL_FAILURE;
		};
	}

	private static String tag(Enum<?> value) {
		return value.name().toLowerCase(Locale.ROOT);
	}

}
