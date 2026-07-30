package com.neighbor.eventmosaic.ingestion.api;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Транзакционная граница durable processing state поверх acquisition `STAGED`.
 */
public interface ArchiveProcessingLedger {

	/**
	 * Идемпотентно регистрирует processing identity только для staged archive.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param fingerprint неизменяемая processing identity
	 * @return зарегистрированное или ранее существовавшее state
	 */
	ArchiveProcessingState register(
			String archiveIdempotencyKey,
			ArchiveProcessingFingerprint fingerprint
	);

	/**
	 * Пытается получить lease на processing archive.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param leaseDuration срок ownership
	 * @return новая attempt либо empty для сейчас не claimable state
	 */
	Optional<ArchiveProcessingAttempt> claim(
			String archiveIdempotencyKey,
			Duration leaseDuration
	);

	/**
	 * Сохраняет монотонный progress и продлевает lease текущего owner.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param attemptToken token владельца
	 * @param progress абсолютные counters текущей attempt
	 * @param leaseDuration новый срок ownership от текущего времени
	 * @return результат conditional transition
	 */
	AttemptTransitionResult checkpoint(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress,
			Duration leaseDuration
	);

	/**
	 * Завершает processing после подтвержденного Elasticsearch receipt.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param attemptToken token владельца
	 * @param progress итоговые counters
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markIndexed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress
	);

	/**
	 * Сохраняет processing failure и partial counters текущего owner.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param attemptToken token владельца
	 * @param failure безопасная failure projection
	 * @param progress partial counters
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markFailed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress
	);

	/**
	 * Условно переводит INDEXED state в FAILED после несовпавшего receipt.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param expectedProcessingFingerprint fingerprint проверенного state
	 * @param expectedAttemptCount число attempts проверенного state
	 * @param failure retryable shortage либо non-retryable surplus
	 * @return APPLIED либо OWNERSHIP_LOST при изменившемся state
	 */
	AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			ArchiveProcessingFailure failure
	);

	/**
	 * Находит processing state archive.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @return state либо empty до регистрации
	 */
	Optional<ArchiveProcessingState> findByArchiveIdempotencyKey(String archiveIdempotencyKey);
}
