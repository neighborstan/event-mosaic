package com.neighbor.eventmosaic.indexing.api;

/**
 * Ограниченная причина, по которой resolved exact target больше нельзя использовать.
 */
public enum IndexTargetUnavailableReason {

	/** Exact physical index отсутствует. */
	MISSING(IndexingErrorCode.INDEX_TARGET_MISSING),

	/** Exact physical имя принадлежит другому Elasticsearch UUID. */
	REPLACED(IndexingErrorCode.INDEX_TARGET_REPLACED),

	/** Exact physical index закрыт для записи maintenance block. */
	WRITE_BLOCKED(IndexingErrorCode.INDEX_TARGET_WRITE_BLOCKED);

	private final IndexingErrorCode errorCode;

	IndexTargetUnavailableReason(IndexingErrorCode errorCode) {
		this.errorCode = errorCode;
	}

	/**
	 * Возвращает безопасный код причины.
	 *
	 * @return bounded indexing error code
	 */
	public IndexingErrorCode errorCode() {
		return errorCode;
	}
}
