package com.neighbor.eventmosaic.ingestion.api;

/**
 * Состояние жизненного цикла одного archive в durable ledger.
 */
public enum IngestionArchiveStatus {
	DISCOVERED,
	PROCESSING,
	STAGED,
	FAILED
}
