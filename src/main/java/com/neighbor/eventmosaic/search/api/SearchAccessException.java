package com.neighbor.eventmosaic.search.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.util.Objects;

/**
 * Представляет временную недоступность Elasticsearch на search boundary.
 */
@SuppressWarnings("java:S110")
public final class SearchAccessException extends RetryableException {

	/**
	 * Создает безопасную поисковую ошибку с исходной технической причиной.
	 *
	 * @param cause исходная причина отказа клиента
	 */
	public SearchAccessException(Throwable cause) {
		super(
				SearchErrorCode.SEARCH_UNAVAILABLE,
				Objects.requireNonNull(cause, "cause must not be null"));
	}
}
