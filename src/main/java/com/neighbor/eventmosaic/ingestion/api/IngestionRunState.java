package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.List;

/**
 * Производное состояние ingestion run для одного GDELT update.
 *
 * @param id идентификатор run
 * @param sourceUpdateTime UTC timestamp update
 * @param status производный статус двух обязательных archives
 * @param firstSeenAt время discovery
 * @param completedAt время полного staging или {@code null}
 * @param lastFailure последняя записанная archive failure или {@code null}
 * @param archives состояния Event и Mention
 */
public record IngestionRunState(
		long id,
		Instant sourceUpdateTime,
		IngestionRunStatus status,
		Instant firstSeenAt,
		Instant completedAt,
		RecordedIngestionFailure lastFailure,
		List<IngestionArchiveState> archives
) {

	/**
	 * Создает immutable snapshot состояний обязательных архивов run.
	 */
	public IngestionRunState {
		archives = List.copyOf(archives);
	}
}
