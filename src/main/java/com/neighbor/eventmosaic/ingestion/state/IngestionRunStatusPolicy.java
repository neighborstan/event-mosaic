package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;

/**
 * Чистая policy производного run status по состоянию обязательных archives.
 */
final class IngestionRunStatusPolicy {

	private static final int REQUIRED_ARCHIVE_COUNT = 2;

	private IngestionRunStatusPolicy() {
	}

	static IngestionRunStatus derive(RunCounts counts) {
		if (counts.staged() == REQUIRED_ARCHIVE_COUNT) {
			return IngestionRunStatus.STAGED;
		}
		if (counts.failed() > 0 && counts.staged() > 0) {
			return IngestionRunStatus.PARTIAL;
		}
		if (counts.processing() > 0 || counts.staged() > 0) {
			return IngestionRunStatus.IN_PROGRESS;
		}
		if (counts.failed() > 0) {
			return IngestionRunStatus.FAILED;
		}
		return counts.attempted() == 0
				? IngestionRunStatus.DISCOVERED
				: IngestionRunStatus.IN_PROGRESS;
	}
}
