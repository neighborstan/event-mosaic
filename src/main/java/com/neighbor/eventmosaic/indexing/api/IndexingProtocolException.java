package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;

/**
 * Представляет окончательное отклонение или нарушение indexing protocol.
 */
@SuppressWarnings("java:S110")
public final class IndexingProtocolException extends NonRetryableException
		implements IndexingFailureContract {

	/**
	 * Создает безопасный protocol failure без исходной причины.
	 *
	 * @param errorCode стабильный code окончательного отказа
	 */
	public IndexingProtocolException(IndexingErrorCode errorCode) {
		super(errorCode);
	}

	/**
	 * Создает безопасный protocol failure с исходной технической причиной.
	 *
	 * @param errorCode стабильный code окончательного отказа
	 * @param cause исходная причина для локального stack trace
	 */
	public IndexingProtocolException(IndexingErrorCode errorCode, Throwable cause) {
		super(errorCode, cause);
	}

	@Override
	public boolean retryable() {
		return false;
	}
}
