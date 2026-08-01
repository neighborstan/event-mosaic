package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;

/** Останавливает cleanup, пока оператор не получит новый plan или не устранит конфликт. */
@SuppressWarnings("java:S110")
public final class GenerationCleanupRejectedException extends NonRetryableException {

	/** Создает окончательный отказ с безопасным cleanup code. */
	public GenerationCleanupRejectedException(GenerationCleanupErrorCode errorCode) {
		super(errorCode);
	}

	/** Создает окончательный отказ с локальной технической причиной. */
	public GenerationCleanupRejectedException(
			GenerationCleanupErrorCode errorCode,
			Throwable cause
	) {
		super(errorCode, cause);
	}
}
