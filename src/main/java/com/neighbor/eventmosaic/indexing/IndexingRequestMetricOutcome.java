package com.neighbor.eventmosaic.indexing;

/**
 * Ограниченные outcomes bulk request для Micrometer tags.
 */
enum IndexingRequestMetricOutcome {
	SUCCEEDED,
	RETRYABLE_PARTIAL_FAILURE,
	NON_RETRYABLE_PARTIAL_FAILURE,
	RETRYABLE_FAILURE,
	NON_RETRYABLE_FAILURE
}
