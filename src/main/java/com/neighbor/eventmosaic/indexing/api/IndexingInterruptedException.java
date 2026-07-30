package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;

/**
 * Управляющий отказ cooperative interruption на indexing boundary.
 */
@SuppressWarnings("java:S110")
public final class IndexingInterruptedException extends RetryableException
		implements IndexingFailureContract {

	/**
	 * Создает interruption для явного checkpoint без исходной причины.
	 */
	public IndexingInterruptedException() {
		super(IndexingErrorCode.INDEXING_INTERRUPTED);
	}

	/**
	 * Создает interruption с исходной I/O причиной без очистки interrupt flag.
	 *
	 * @param cause исходный транспортный отказ
	 */
	public IndexingInterruptedException(Throwable cause) {
		super(IndexingErrorCode.INDEXING_INTERRUPTED, cause);
	}

	@Override
	public boolean retryable() {
		return true;
	}

	@Override
	public boolean interruptsProcessing() {
		return true;
	}
}
