package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;

/**
 * Останавливает Event write до перезаписи документа с другой provenance.
 *
 * <p>Исключение раскрывает только физическую строку текущего trusted input и
 * не публикует document ID, archive key или processing fingerprint.</p>
 */
@SuppressWarnings("java:S110")
public final class EventIdentityConflictException extends NonRetryableException
		implements IndexingFailureContract {

	private final long sourceLineNumber;

	/**
	 * Создает non-retryable identity conflict текущей физической строки.
	 *
	 * @param sourceLineNumber положительный номер строки входного CSV
	 */
	public EventIdentityConflictException(long sourceLineNumber) {
		super(IndexingErrorCode.EVENT_IDENTITY_CONFLICT);
		if (sourceLineNumber <= 0) {
			throw new IllegalArgumentException("sourceLineNumber must be positive");
		}
		this.sourceLineNumber = sourceLineNumber;
	}

	/**
	 * Возвращает безопасный bounded context конфликтующей строки.
	 *
	 * @return положительный номер физической строки
	 */
	public long sourceLineNumber() {
		return sourceLineNumber;
	}

	@Override
	public boolean retryable() {
		return false;
	}
}
