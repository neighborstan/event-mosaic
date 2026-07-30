package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;

/**
 * Представляет временный отказ доступа к Elasticsearch.
 */
@SuppressWarnings("java:S110")
public final class IndexingAccessException extends RetryableException
		implements IndexingFailureContract {

	/**
	 * Создает безопасный отказ доступа без исходной причины.
	 *
	 * @param errorCode стабильный code временного отказа
	 */
	public IndexingAccessException(IndexingErrorCode errorCode) {
		super(errorCode);
	}

	/**
	 * Создает безопасный отказ доступа с исходной технической причиной.
	 *
	 * @param errorCode стабильный code временного отказа
	 * @param cause исходная причина для локального stack trace
	 */
	public IndexingAccessException(IndexingErrorCode errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	@Override
	public boolean retryable() {
		return true;
	}
}
