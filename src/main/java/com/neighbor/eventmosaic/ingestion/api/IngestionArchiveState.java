package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;

/**
 * Состояние одного Event или Mention archive в durable ingestion ledger.
 *
 * @param runId идентификатор общего update run
 * @param archive проверенная source metadata
 * @param status текущий статус архива
 * @param attempt состояние claim/lease
 * @param stagedArchive опубликованные artifacts или {@code null}
 * @param failure последняя записанная ошибка или {@code null}
 * @param firstSeenAt время первой регистрации
 * @param completedAt время успешного staging или {@code null}
 */
public record IngestionArchiveState(
		long runId,
		DiscoveredArchive archive,
		IngestionArchiveStatus status,
		ArchiveAttemptState attempt,
		StagedArchive stagedArchive,
		RecordedIngestionFailure failure,
		Instant firstSeenAt,
		Instant completedAt
) {
}
