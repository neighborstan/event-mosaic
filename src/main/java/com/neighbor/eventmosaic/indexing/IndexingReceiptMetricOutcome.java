package com.neighbor.eventmosaic.indexing;

/**
 * Ограниченные итоги приемочной проверки архива для меток метрик.
 */
enum IndexingReceiptMetricOutcome {
	MATCHED,
	SHORTAGE,
	SURPLUS,
	IDENTITY_MISMATCH,
	RETRYABLE_FAILURE,
	NON_RETRYABLE_FAILURE
}
