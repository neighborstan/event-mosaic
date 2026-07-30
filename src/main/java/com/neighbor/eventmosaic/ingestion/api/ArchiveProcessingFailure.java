package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Безопасная durable проекция processing failure.
 *
 * @param errorCode стабильный machine-readable code
 * @param retryable разрешен ли automatic retry owning operation
 */
public record ArchiveProcessingFailure(String errorCode, boolean retryable) {

	private static final Pattern ERROR_CODE_PATTERN =
			Pattern.compile("^[A-Z][A-Z0-9_]{0,63}$");

	/**
	 * Проверяет bounded machine-readable code.
	 */
	public ArchiveProcessingFailure {
		Objects.requireNonNull(errorCode, "errorCode must not be null");
		if (!ERROR_CODE_PATTERN.matcher(errorCode).matches()) {
			throw new IllegalArgumentException(
					"errorCode must be a bounded uppercase identifier");
		}
	}
}
