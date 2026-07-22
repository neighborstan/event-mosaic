package com.neighbor.eventmosaic.ingestion;

/**
 * Ограниченный набор стабильных результатов обработки одного archive.
 */
enum ArchiveOutcome {
	STAGED,
	FAILED,
	SKIPPED,
	OWNERSHIP_LOST
}
