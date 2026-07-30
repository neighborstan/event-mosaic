package com.neighbor.eventmosaic.indexing;

/**
 * Ограниченные outcomes bulk documents для Micrometer tags.
 */
enum IndexingDocumentMetricOutcome {
	SUCCEEDED,
	RETRYABLE_FAILURE,
	NON_RETRYABLE_FAILURE,
	UNKNOWN
}
