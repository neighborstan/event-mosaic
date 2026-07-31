package com.neighbor.eventmosaic.indexing.api;

/** Состояние физического поколения индексов одной logical partition. */
public enum IndexGenerationStatus {
	BUILDING,
	ACTIVE,
	SUPERSEDED,
	FAILED,
	CLEANUP_PENDING,
	DELETE_REQUESTED,
	CLEANED
}
