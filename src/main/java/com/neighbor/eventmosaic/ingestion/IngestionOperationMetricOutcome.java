package com.neighbor.eventmosaic.ingestion;

/** Ограниченные terminal outcomes acquisition и полного one-shot cycle. */
enum IngestionOperationMetricOutcome {

	COMPLETED,
	UNCHANGED,
	RETRY_DEFERRED,
	OWNERSHIP_LOST,
	OPERATION_DEADLINE_EXCEEDED,
	STORAGE_PRESSURE,
	EXPECTED_FAILURE,
	INTERRUPTED,
	INTERNAL_FAILURE
}
