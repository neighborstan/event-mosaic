package com.neighbor.eventmosaic.ingestion.api;

/**
 * Состояние независимой downstream-обработки одного staged archive.
 */
public enum ArchiveProcessingStatus {
	PENDING,
	PROCESSING,
	FAILED,
	INDEXED
}
