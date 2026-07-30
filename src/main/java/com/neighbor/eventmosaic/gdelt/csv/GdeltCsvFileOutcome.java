package com.neighbor.eventmosaic.gdelt.csv;

/**
 * Ограниченный набор итогов одного вызова потокового CSV reader.
 */
enum GdeltCsvFileOutcome {
	COMPLETED,
	SCHEMA_FAILED,
	IO_FAILED,
	CONSUMER_FAILED,
	INTERRUPTED
}
