package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Подтверждает временное право одного trigger выполнять общий ingestion cycle.
 * Token отличает конкретного owner, а монотонная epoch не дает старому owner
 * изменить состояние после takeover.
 *
 * @param sourceName стабильное имя source
 * @param token случайный token текущего owner
 * @param fencingEpoch монотонный номер выданного ownership
 * @param leaseExpiresAt время окончания права по часам PostgreSQL
 */
public record IngestionCycleOwnership(
		String sourceName,
		UUID token,
		long fencingEpoch,
		Instant leaseExpiresAt
) {

	/** Проверяет полноту identity и положительную fencing epoch. */
	public IngestionCycleOwnership {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		Objects.requireNonNull(token, "token must not be null");
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
		if (sourceName.isBlank() || sourceName.length() > 64) {
			throw new IllegalArgumentException(
					"sourceName must contain between 1 and 64 characters");
		}
		if (fencingEpoch <= 0) {
			throw new IllegalArgumentException("fencingEpoch must be positive");
		}
	}
}
