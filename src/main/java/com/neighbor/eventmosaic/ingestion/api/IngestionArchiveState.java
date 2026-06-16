package com.neighbor.eventmosaic.ingestion.api;

import java.net.URI;
import java.time.Instant;

public record IngestionArchiveState(
		String idempotencyKey,
		String archiveName,
		URI archiveUri,
		String expectedMd5,
		String actualMd5,
		ArchiveType archiveType,
		IngestionArchiveStatus status,
		Long fileSizeBytes,
		long rawRecordCount,
		long parsedRecordCount,
		long processedRecordCount,
		long indexedRecordCount,
		long failedRecordCount,
		Instant firstSeenAt,
		Instant lastAttemptAt,
		Instant completedAt,
		int attemptCount,
		String lastError
) {
}
