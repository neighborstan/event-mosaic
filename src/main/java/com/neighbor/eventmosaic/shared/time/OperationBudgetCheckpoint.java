package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.util.Objects;

/**
 * Возвращает итог одной проверки общей deadline и внешнего ownership.
 * Нулевой остаток запрещает начинать следующий побочный эффект.
 *
 * @param status причина разрешения или запрета следующего шага
 * @param remaining безопасный остаток времени для дочерней операции
 */
public record OperationBudgetCheckpoint(
		OperationBudgetStatus status,
		Duration remaining
) {

	/** Проверяет согласованность статуса и безопасного остатка. */
	public OperationBudgetCheckpoint {
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(remaining, "remaining must not be null");
		if (remaining.isNegative()) {
			throw new IllegalArgumentException("remaining must not be negative");
		}
		if (status == OperationBudgetStatus.AVAILABLE && remaining.isZero()) {
			throw new IllegalArgumentException("available checkpoint must have remaining time");
		}
		if (status != OperationBudgetStatus.AVAILABLE && !remaining.isZero()) {
			throw new IllegalArgumentException("unavailable checkpoint must have zero remaining time");
		}
	}

	/** Возвращает {@code true}, если следующий ограниченный шаг еще разрешен. */
	public boolean available() {
		return status == OperationBudgetStatus.AVAILABLE;
	}
}
