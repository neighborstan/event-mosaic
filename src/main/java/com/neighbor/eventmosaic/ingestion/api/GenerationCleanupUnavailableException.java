package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;

/** Передает временную недоступность cleanup dependency или maintenance ownership. */
@SuppressWarnings("java:S110")
public final class GenerationCleanupUnavailableException extends RetryableException {

	/** Создает временный отказ с безопасным cleanup code. */
	public GenerationCleanupUnavailableException(GenerationCleanupErrorCode errorCode) {
		super(errorCode);
	}

	/** Создает временный отказ с локальной технической причиной. */
	public GenerationCleanupUnavailableException(
			GenerationCleanupErrorCode errorCode,
			Throwable cause
	) {
		super(errorCode, cause);
	}
}
