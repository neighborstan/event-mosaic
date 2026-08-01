package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Временное право обрабатывать один staged archive.
 *
 * @param archiveIdempotencyKey стабильный source archive key
 * @param fingerprint неизменяемая processing identity
 * @param targetBinding captured exact ACTIVE target
 * @param token уникальный token владельца conditional transitions
 * @param leaseExpiresAt момент окончания ownership
 * @param attemptCount порядковый номер попытки, начиная с единицы
 * @param recovered признак восстановления после истекшего lease
 */
public record ArchiveProcessingAttempt(
		String archiveIdempotencyKey,
		ArchiveProcessingFingerprint fingerprint,
		ArchiveProcessingTargetBinding targetBinding,
		UUID token,
		Instant leaseExpiresAt,
		int attemptCount,
		boolean recovered
) {

	/**
	 * Проверяет обязательную identity и положительный attempt count.
	 */
	public ArchiveProcessingAttempt {
		Objects.requireNonNull(archiveIdempotencyKey, "archiveIdempotencyKey must not be null");
		if (archiveIdempotencyKey.isBlank()) {
			throw new IllegalArgumentException("archiveIdempotencyKey must not be blank");
		}
		Objects.requireNonNull(fingerprint, "fingerprint must not be null");
		Objects.requireNonNull(targetBinding, "targetBinding must not be null");
		Objects.requireNonNull(token, "token must not be null");
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
		if (attemptCount <= 0) {
			throw new IllegalArgumentException("attemptCount must be positive");
		}
	}
}
