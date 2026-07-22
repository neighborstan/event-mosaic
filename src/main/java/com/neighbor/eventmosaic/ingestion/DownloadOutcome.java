package com.neighbor.eventmosaic.ingestion;

/**
 * Ограниченный набор стабильных результатов download/reuse archive.
 */
enum DownloadOutcome {
	DOWNLOADED,
	REUSED,
	FAILED
}
