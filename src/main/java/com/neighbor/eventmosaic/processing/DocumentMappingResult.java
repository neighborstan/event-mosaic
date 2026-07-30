package com.neighbor.eventmosaic.processing;

import java.util.Objects;

/**
 * Результат локального mapping одной source record.
 *
 * @param document готовый документ или {@code null}
 * @param rejection bounded rejection или {@code null}
 * @param invalidGeoCandidates число пропущенных некорректных geo candidates
 * @param <T> тип индексируемого документа
 */
record DocumentMappingResult<T>(
		T document,
		ProcessingMappingRejection rejection,
		int invalidGeoCandidates
) {

	/** Проверяет взаимоисключающие accepted/rejected outcomes. */
	DocumentMappingResult {
		if ((document == null) == (rejection == null)) {
			throw new IllegalArgumentException(
					"mapping result must contain exactly one document or rejection");
		}
		if (invalidGeoCandidates < 0) {
			throw new IllegalArgumentException("invalidGeoCandidates must not be negative");
		}
	}

	static <T> DocumentMappingResult<T> accepted(T document, int invalidGeoCandidates) {
		return new DocumentMappingResult<>(
				Objects.requireNonNull(document, "document must not be null"),
				null,
				invalidGeoCandidates);
	}

	static <T> DocumentMappingResult<T> rejected(ProcessingMappingRejection rejection) {
		return new DocumentMappingResult<>(
				null,
				Objects.requireNonNull(rejection, "rejection must not be null"),
				0);
	}

	boolean accepted() {
		return document != null;
	}
}
