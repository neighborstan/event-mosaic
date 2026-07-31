package com.neighbor.eventmosaic.ingestion.api;

/**
 * Текущее durable состояние получения latest source manifest.
 */
public enum SourcePollStatus {
	IDLE,
	POLLING,
	FAILED
}
