package com.neighbor.eventmosaic.ingestion.api;

/** Ограниченные operational outcomes ручного partition rebuild. */
public enum PartitionRebuildErrorCode {
	UNKNOWN_PARTITION(false),
	REBUILD_NOT_REQUIRED(false),
	STALE_REBUILD_PLAN(false),
	AUTHORITATIVE_SET_INCOMPLETE(false),
	REBUILD_DISK_PRESSURE(true),
	REBUILD_MAINTENANCE_BUSY(true),
	REBUILD_OWNERSHIP_LOST(true),
	REBUILD_EXACT_TARGET_CONFLICT(false),
	REBUILD_ALIAS_CONFLICT(false),
	REBUILD_SOURCE_INVALID(false),
	REBUILD_RECEIPT_MISMATCH(false),
	REBUILD_PROCESSING_FAILED(true),
	REBUILD_OPERATION_UNAVAILABLE(true);

	private final boolean retryable;

	PartitionRebuildErrorCode(boolean retryable) {
		this.retryable = retryable;
	}

	/** Возвращает, можно ли безопасно повторить ту же operation после проверки. */
	public boolean retryable() {
		return retryable;
	}
}
