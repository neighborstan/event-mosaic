package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;

/**
 * Безопасная проекция ingestion failure, сохраняемая в ledger.
 *
 * @param code стабильный error code
 * @param retryable разрешен ли повтор owning operation
 */
public record IngestionFailure(
		IngestionErrorCode code,
		boolean retryable
) {

	/**
	 * Проверяет обязательные атрибуты failure.
	 */
	public IngestionFailure {
		Objects.requireNonNull(code, "code must not be null");
	}

	/**
	 * Создает неповторяемую проекцию неожиданного внутреннего отказа.
	 *
	 * @return безопасная failure projection
	 */
	public static IngestionFailure internalError() {
		return new IngestionFailure(IngestionErrorCode.INTERNAL_ERROR, false);
	}
}
