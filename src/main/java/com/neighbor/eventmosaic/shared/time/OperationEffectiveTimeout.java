package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.util.Objects;

/**
 * Передает положительный timeout внешнего вызова и ограничение, которое его
 * определило.
 *
 * @param timeout effective timeout внешнего вызова
 * @param origin источник минимального ограничения
 */
public record OperationEffectiveTimeout(
		Duration timeout,
		OperationTimeoutOrigin origin
) {

	/** Проверяет цельную форму timeout перед созданием внешнего request. */
	public OperationEffectiveTimeout {
		Objects.requireNonNull(timeout, "timeout must not be null");
		Objects.requireNonNull(origin, "origin must not be null");
		if (timeout.isZero() || timeout.isNegative()) {
			throw new IllegalArgumentException("timeout must be positive");
		}
	}
}
