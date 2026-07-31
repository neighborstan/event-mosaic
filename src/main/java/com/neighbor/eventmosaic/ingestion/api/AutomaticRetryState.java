package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;

/**
 * Durable counters и due boundary текущей automatic retry sequence.
 *
 * @param automaticRetriesUsed уже израсходованные automatic retries
 * @param consecutiveRetryableFailures последовательные retryable failures
 * @param automaticRetryLimit сохраненный предел automatic retries
 * @param retryNotBefore earliest UTC-время следующего claim или {@code null}
 */
public record AutomaticRetryState(
		int automaticRetriesUsed,
		int consecutiveRetryableFailures,
		int automaticRetryLimit,
		Instant retryNotBefore
) {

	/** Проверяет неотрицательные counters и соблюдение сохраненного budget. */
	public AutomaticRetryState {
		if (automaticRetriesUsed < 0) {
			throw new IllegalArgumentException("automaticRetriesUsed must not be negative");
		}
		if (consecutiveRetryableFailures < 0) {
			throw new IllegalArgumentException("consecutiveRetryableFailures must not be negative");
		}
		if (automaticRetryLimit < 0 || automaticRetryLimit > 100) {
			throw new IllegalArgumentException("automaticRetryLimit must be between 0 and 100");
		}
		if (automaticRetriesUsed > automaticRetryLimit) {
			throw new IllegalArgumentException("automaticRetriesUsed must not exceed automaticRetryLimit");
		}
	}

	/**
	 * Создает пустую retry sequence с сохраненным limit.
	 *
	 * @param automaticRetryLimit предел automatic retries
	 * @return начальное durable retry state
	 */
	public static AutomaticRetryState initial(int automaticRetryLimit) {
		return new AutomaticRetryState(0, 0, automaticRetryLimit, null);
	}

	/**
	 * Возвращает признак исчерпанного automatic budget.
	 *
	 * @return {@code true}, когда новый automatic claim запрещен budget
	 */
	public boolean exhausted() {
		return automaticRetriesUsed >= automaticRetryLimit;
	}
}
