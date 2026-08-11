package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;

/** Сообщает о временной недоступности read-only проверки полноты ingestion. */
@SuppressWarnings("java:S110")
public final class IngestionCoverageUnavailableException extends RetryableException {

	/**
	 * Создает безопасную typed ошибку без публикации JDBC-причины наружу.
	 *
	 * @param cause локальная причина отказа доступа к данным
	 */
	public IngestionCoverageUnavailableException(Throwable cause) {
		super(IngestionErrorCode.COVERAGE_QUERY_UNAVAILABLE, cause);
	}
}
