package com.neighbor.eventmosaic.processing.api;

import java.util.Objects;

/**
 * Безопасная проекция ожидаемого processing failure.
 *
 * @param code стабильный код
 * @param retryable допускается ли полный повтор архива
 * @param firstFailedLineNumber первая подтвержденная failed line или {@code null}
 */
public record ArchiveProcessingFailure(
		ArchiveProcessingErrorCode code,
		boolean retryable,
		Long firstFailedLineNumber
) {

	/** Проверяет обязательный код и optional положительный line number. */
	public ArchiveProcessingFailure {
		Objects.requireNonNull(code, "code must not be null");
		if (firstFailedLineNumber != null && firstFailedLineNumber <= 0) {
			throw new IllegalArgumentException("firstFailedLineNumber must be positive");
		}
	}
}
