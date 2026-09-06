package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;

/**
 * Хранит счетчики текущей серии повторов, время следующей попытки и последнюю причину исчерпания серии. Эти сведения сохраняются между перезапусками приложения.
 *
 * @param automaticRetriesUsed уже израсходованные automatic retries
 * @param consecutiveRetryableFailures последовательные retryable failures
 * @param automaticRetryLimit сохраненный предел automatic retries
 * @param retryNotBefore earliest UTC-время следующего claim или {@code null}
 * @param retrySequence номер текущей серии попыток, начиная с единицы
 * @param lastExhaustedAt время последней исчерпанной серии или {@code null}
 * @param lastExhaustedErrorCode безопасный код причины последнего исчерпания или {@code null}
 */
public record AutomaticRetryState(
		int automaticRetriesUsed,
		int consecutiveRetryableFailures,
		int automaticRetryLimit,
		Instant retryNotBefore,
		long retrySequence,
		Instant lastExhaustedAt,
		String lastExhaustedErrorCode
) {

	/** Проверяет неотрицательные counters и соблюдение сохраненного budget. */
	public AutomaticRetryState {
		if (retrySequence < 1) {
			throw new IllegalArgumentException("retrySequence must be positive");
		}
		if ((lastExhaustedAt == null) != (lastExhaustedErrorCode == null)) {
			throw new IllegalArgumentException("exhausted evidence must be present together");
		}
		if (lastExhaustedErrorCode != null
				&& !lastExhaustedErrorCode.matches("[A-Z][A-Z0-9_]{0,63}")) {
			throw new IllegalArgumentException("lastExhaustedErrorCode must be a safe code");
		}
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

	/** Создает первую серию, для которой еще нет сведений о прежнем исчерпании попыток. */
	public AutomaticRetryState(
			int automaticRetriesUsed,
			int consecutiveRetryableFailures,
			int automaticRetryLimit,
			Instant retryNotBefore
	) {
		this(automaticRetriesUsed, consecutiveRetryableFailures, automaticRetryLimit,
				retryNotBefore, 1, null, null);
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
	 * Показывает, что все повторы текущей серии уже использованы. Новая серия разрешается хранилищем отдельно, после полной паузы и проверки актуальности работы.
	 *
	 * @return {@code true}, когда в текущей серии больше не осталось повторов
	 */
	public boolean exhausted() {
		return automaticRetriesUsed >= automaticRetryLimit;
	}
}
