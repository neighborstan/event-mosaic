package com.neighbor.eventmosaic.ingestion.api;

/**
 * Ограниченный итог одного ingestion cycle для долговечной диагностики без
 * внешних идентификаторов и текстов ошибок.
 */
public enum IngestionCycleOutcome {
	COMPLETED,
	UNCHANGED,
	RETRY_DEFERRED,
	SKIPPED_ACTIVE_CYCLE,
	OWNERSHIP_LOST,
	STORAGE_PRESSURE,
	DEADLINE,
	INTERRUPTED,
	EXPECTED_FAILURE,
	INTERNAL_FAILURE
}
