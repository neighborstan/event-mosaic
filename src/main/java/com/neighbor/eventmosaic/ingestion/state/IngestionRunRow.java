package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.RecordedIngestionFailure;
import java.time.Instant;

/**
 * Представляет составную строку ingestion run для facade mapping.
 */
record IngestionRunRow(
		long id,
		Instant sourceUpdateTime,
		IngestionRunStatus status,
		Instant firstSeenAt,
		Instant completedAt,
		RecordedIngestionFailure lastFailure
) {
}
