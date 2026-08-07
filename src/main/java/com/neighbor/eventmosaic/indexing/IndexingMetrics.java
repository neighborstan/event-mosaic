package com.neighbor.eventmosaic.indexing;

import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;

/**
 * Собирает счетчики и длительность операций Elasticsearch с небольшим
 * фиксированным набором значений меток метрик.
 */
@Component
final class IndexingMetrics {

	private static final String REQUESTS_METER = "event_mosaic.indexing.bulk.requests";
	private static final String DOCUMENTS_METER = "event_mosaic.indexing.bulk.documents";
	private static final String EVENT_IDENTITY_GUARD_METER =
			"event_mosaic.indexing.event_identity_guard";
	private static final String BULK_DURATION_METER =
			"event_mosaic.indexing.bulk.duration";
	private static final String RECEIPT_DURATION_METER =
			"event_mosaic.indexing.receipt.duration";
	private static final String RECEIPT_INCIDENTS_METER =
			"event_mosaic.indexing.receipt.incidents";
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
		IndexingRequestMetricOutcome outcome = failureOutcome(retryable);
		meterRegistry.counter(
				REQUESTS_METER,
				KIND_TAG, kindTag,
				OUTCOME_TAG, tag(outcome)
		).increment();
		incrementDocuments(kindTag, IndexingDocumentMetricOutcome.UNKNOWN, submitted);
	}

	/** Записывает стоимость и bounded outcome Event identity guard. */
	void eventIdentityGuard(long elapsedNanos, EventIdentityGuardMetricOutcome outcome) {
		if (elapsedNanos < 0) {
			throw new IllegalArgumentException("elapsedNanos must not be negative");
		}
		Objects.requireNonNull(outcome, "outcome must not be null");
		meterRegistry.timer(
				EVENT_IDENTITY_GUARD_METER,
				OUTCOME_TAG, tag(outcome)
		).record(elapsedNanos, TimeUnit.NANOSECONDS);
	}

	/** Начинает измерение одной пакетной записи. */
	Timer.Sample startBulkTimer() {
		return Timer.start(meterRegistry);
	}

	/** Завершает измерение пакетной записи с итогом, определенным writer. */
	void bulkDuration(
			Timer.Sample sample,
			GdeltIndexKind kind,
			IndexingRequestMetricOutcome outcome
	) {
		stop(
				sample,
				BULK_DURATION_METER,
				kind,
				Objects.requireNonNull(outcome, "outcome must not be null"));
	}

	/** Начинает измерение одной приемочной проверки архива. */
	Timer.Sample startReceiptTimer() {
		return Timer.start(meterRegistry);
	}

	/**
	 * Завершает приемочную проверку и отмечает обнаруженное расхождение.
	 *
	 * @param sample начатое измерение текущей проверки
	 * @param kind вид проверяемых документов
	 * @param outcome безопасный итог проверки или отказа
	 */
	void receiptDuration(
			Timer.Sample sample,
			GdeltIndexKind kind,
			IndexingReceiptMetricOutcome outcome
	) {
		stop(
				sample,
				RECEIPT_DURATION_METER,
				kind,
				Objects.requireNonNull(outcome, "outcome must not be null"));
		switch (outcome) {
			case SHORTAGE, IDENTITY_MISMATCH -> receiptIncident(kind, "mismatch");
			case SURPLUS -> receiptIncident(kind, "surplus");
			case MATCHED, RETRYABLE_FAILURE, NON_RETRYABLE_FAILURE -> {
				// Эти результаты не являются подтвержденным расхождением receipt.
			}
		}
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

	static IndexingRequestMetricOutcome requestOutcome(BulkIndexOutcome outcome) {
		return switch (outcome) {
			case SUCCEEDED -> IndexingRequestMetricOutcome.SUCCEEDED;
			case RETRYABLE_PARTIAL_FAILURE ->
					IndexingRequestMetricOutcome.RETRYABLE_PARTIAL_FAILURE;
			case NON_RETRYABLE_PARTIAL_FAILURE ->
					IndexingRequestMetricOutcome.NON_RETRYABLE_PARTIAL_FAILURE;
		};
	}

	static IndexingRequestMetricOutcome failureOutcome(boolean retryable) {
		return retryable
				? IndexingRequestMetricOutcome.RETRYABLE_FAILURE
				: IndexingRequestMetricOutcome.NON_RETRYABLE_FAILURE;
	}

	static IndexingReceiptMetricOutcome receiptOutcome(ArchiveReceiptStatus status) {
		return switch (Objects.requireNonNull(status, "status must not be null")) {
			case MATCHED -> IndexingReceiptMetricOutcome.MATCHED;
			case SHORTAGE -> IndexingReceiptMetricOutcome.SHORTAGE;
			case SURPLUS -> IndexingReceiptMetricOutcome.SURPLUS;
			case IDENTITY_MISMATCH -> IndexingReceiptMetricOutcome.IDENTITY_MISMATCH;
		};
	}

	private void stop(
			Timer.Sample sample,
			String meter,
			GdeltIndexKind kind,
			Enum<?> outcome
	) {
		Objects.requireNonNull(sample, "sample must not be null").stop(
				meterRegistry.timer(
						meter,
						KIND_TAG, tag(kind),
						OUTCOME_TAG, tag(outcome)));
	}

	private void receiptIncident(GdeltIndexKind kind, String outcome) {
		meterRegistry.counter(
				RECEIPT_INCIDENTS_METER,
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
