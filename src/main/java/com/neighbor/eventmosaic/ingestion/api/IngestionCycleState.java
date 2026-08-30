package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Хранит текущее source-scoped состояние общего ingestion cycle и последний
 * ограниченный итог. Это current state, а не история каждого запуска.
 *
 * @param sourceName стабильное имя source
 * @param status текущее состояние ownership
 * @param fencingEpoch последний выданный монотонный номер ownership
 * @param ownership текущий owner либо {@code null} для свободного cycle
 * @param lastStartedAt время последнего успешного claim либо {@code null}
 * @param lastProgressAt время последнего подтвержденного progress либо {@code null}
 * @param lastTerminalAt время последнего terminal outcome либо {@code null}
 * @param lastOutcome последний ограниченный итог либо {@code null}
 * @param stateVersion версия current state для диагностики изменений
 * @param createdAt время создания source row
 * @param updatedAt время последнего изменения
 */
public record IngestionCycleState(
		String sourceName,
		IngestionCycleStatus status,
		long fencingEpoch,
		IngestionCycleOwnership ownership,
		Instant lastStartedAt,
		Instant lastProgressAt,
		Instant lastTerminalAt,
		IngestionCycleOutcome lastOutcome,
		long stateVersion,
		Instant createdAt,
		Instant updatedAt
) {

	/** Проверяет цельную active/idle форму и связанные timestamps. */
	public IngestionCycleState {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(createdAt, "createdAt must not be null");
		Objects.requireNonNull(updatedAt, "updatedAt must not be null");
		if (sourceName.isBlank() || sourceName.length() > 64) {
			throw new IllegalArgumentException(
					"sourceName must contain between 1 and 64 characters");
		}
		if (fencingEpoch < 0 || stateVersion < 0) {
			throw new IllegalArgumentException("versions must not be negative");
		}
		if ((lastTerminalAt == null) != (lastOutcome == null)) {
			throw new IllegalArgumentException(
					"lastTerminalAt and lastOutcome must be present together");
		}
		if (lastProgressAt != null
				&& (lastStartedAt == null || lastProgressAt.isBefore(lastStartedAt))) {
			throw new IllegalArgumentException(
					"lastProgressAt must not be before lastStartedAt");
		}
		if (status == IngestionCycleStatus.IDLE && ownership != null) {
			throw new IllegalArgumentException("IDLE cycle must not contain ownership");
		}
		if (status == IngestionCycleStatus.ACTIVE) {
			Objects.requireNonNull(ownership, "ACTIVE cycle must contain ownership");
			Objects.requireNonNull(lastStartedAt, "ACTIVE cycle must contain lastStartedAt");
			if (!sourceName.equals(ownership.sourceName())
					|| fencingEpoch != ownership.fencingEpoch()) {
				throw new IllegalArgumentException(
						"ACTIVE ownership must match source and fencing epoch");
			}
			if (!ownership.leaseExpiresAt().isAfter(lastStartedAt)) {
				throw new IllegalArgumentException(
						"ACTIVE lease must expire after lastStartedAt");
			}
		}
	}
}
