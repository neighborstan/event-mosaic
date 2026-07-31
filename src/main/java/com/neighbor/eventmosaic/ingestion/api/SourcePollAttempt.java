package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Временное право получить latest manifest одного source.
 *
 * @param sourceName стабильное имя source
 * @param token уникальный ownership token
 * @param leaseExpiresAt окончание lease
 * @param attemptCount общий порядковый номер attempt
 * @param recovered признак recovery истекшего POLLING lease
 */
public record SourcePollAttempt(
		String sourceName,
		UUID token,
		Instant leaseExpiresAt,
		int attemptCount,
		boolean recovered
) {

	/** Проверяет обязательную identity и положительный attempt number. */
	public SourcePollAttempt {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		Objects.requireNonNull(token, "token must not be null");
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
		if (sourceName.isBlank()) {
			throw new IllegalArgumentException("sourceName must not be blank");
		}
		if (attemptCount <= 0) {
			throw new IllegalArgumentException("attemptCount must be positive");
		}
	}
}
