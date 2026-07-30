package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Предоставляет безопасную module-specific проекцию ожидаемого indexing failure.
 */
public interface IndexingFailureContract {

	/**
	 * Возвращает стабильный код текущего отказа.
	 *
	 * @return безопасный machine-readable code
	 */
	ApplicationErrorCode errorCode();

	/**
	 * Определяет допустимость повтора owning operation.
	 *
	 * @return {@code true} только для временного отказа
	 */
	boolean retryable();

	/**
	 * Определяет, должен ли отказ остановить весь processing cycle.
	 *
	 * @return {@code true} только для cooperative interruption
	 */
	default boolean interruptsProcessing() {
		return false;
	}
}
