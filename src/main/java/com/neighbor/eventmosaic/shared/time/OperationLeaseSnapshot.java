package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.util.Objects;

/**
 * Передает результат проверки внешнего ownership и остаток его lease.
 *
 * @param current принадлежит ли ownership вызывающей операции
 * @param remainingLease остаток lease для текущего владельца или ноль
 */
public record OperationLeaseSnapshot(
		boolean current,
		Duration remainingLease
) {

	/** Проверяет цельную форму current и lost снимка. */
	public OperationLeaseSnapshot {
		Objects.requireNonNull(remainingLease, "remainingLease must not be null");
		if (remainingLease.isNegative()) {
			throw new IllegalArgumentException("remainingLease must not be negative");
		}
		if (current && remainingLease.isZero()) {
			throw new IllegalArgumentException("current lease must have remaining time");
		}
		if (!current && !remainingLease.isZero()) {
			throw new IllegalArgumentException("lost lease must have zero remaining time");
		}
	}

	/** Создает подтвержденный снимок с положительным остатком lease. */
	public static OperationLeaseSnapshot current(Duration remainingLease) {
		return new OperationLeaseSnapshot(true, remainingLease);
	}

	/** Создает снимок потерянного либо истекшего ownership. */
	public static OperationLeaseSnapshot lost() {
		return new OperationLeaseSnapshot(false, Duration.ZERO);
	}
}
