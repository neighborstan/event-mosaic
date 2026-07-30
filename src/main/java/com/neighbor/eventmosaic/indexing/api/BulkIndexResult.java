package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Полный анализ подтвержденного bulk-ответа.
 *
 * @param kind вид документов
 * @param submitted количество отправленных элементов
 * @param succeeded количество подтвержденных успешных элементов
 * @param failed количество подтвержденных отказов
 * @param firstFailedLineNumber первая физическая строка с отказом или {@code null}
 * @param outcome агрегированный исход порции
 */
public record BulkIndexResult(
		GdeltIndexKind kind,
		long submitted,
		long succeeded,
		long failed,
		Long firstFailedLineNumber,
		BulkIndexOutcome outcome
) {

	/**
	 * Проверяет согласованность счетчиков и исхода.
	 */
	public BulkIndexResult {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(outcome, "outcome must not be null");
		if (submitted <= 0 || succeeded < 0 || failed < 0 || succeeded + failed != submitted) {
			throw new IllegalArgumentException("bulk counters must be non-negative and complete");
		}
		if (outcome == BulkIndexOutcome.SUCCEEDED) {
			if (failed != 0 || firstFailedLineNumber != null) {
				throw new IllegalArgumentException("successful outcome must not contain failures");
			}
		}
		else if (failed == 0 || firstFailedLineNumber == null || firstFailedLineNumber <= 0) {
			throw new IllegalArgumentException("partial outcome must contain a failed physical line");
		}
	}

	/**
	 * Возвращает признак полного успеха.
	 *
	 * @return {@code true}, если все элементы подтверждены
	 */
	public boolean successful() {
		return outcome == BulkIndexOutcome.SUCCEEDED;
	}

	/**
	 * Возвращает признак допустимого повтора всей порции.
	 *
	 * @return {@code true}, если все отказы временные
	 */
	public boolean retryable() {
		return outcome == BulkIndexOutcome.RETRYABLE_PARTIAL_FAILURE;
	}

}
