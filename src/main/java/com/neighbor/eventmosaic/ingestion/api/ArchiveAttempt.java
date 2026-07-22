package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Выданное ledger право временно обрабатывать один архив.
 *
 * @param archive неизменяемая metadata архива
 * @param token уникальный token владельца conditional transitions
 * @param leaseExpiresAt момент окончания права владения
 * @param attemptCount порядковый номер попытки, начиная с единицы
 * @param recovered признак восстановления после истекшего lease
 */
public record ArchiveAttempt(
		DiscoveredArchive archive,
		UUID token,
		Instant leaseExpiresAt,
		int attemptCount,
		boolean recovered
) {

	/**
	 * Проверяет обязательные данные claim и положительный номер попытки.
	 */
	public ArchiveAttempt {
		Objects.requireNonNull(archive, "archive must not be null");
		Objects.requireNonNull(token, "token must not be null");
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
		if (attemptCount <= 0) {
			throw new IllegalArgumentException("attemptCount must be positive");
		}
	}
}
