package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import java.time.Duration;
import java.util.Optional;

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
	 * @param targetBinding exact ACTIVE target, разрешенный перед claim
	 * @param leaseDuration срок ownership
	 * @return typed claim outcome с attempt только для CLAIMED
	 */
	ArchiveProcessingClaimResult claim(
			String archiveIdempotencyKey,
			ArchiveProcessingTargetBinding targetBinding,
			Duration leaseDuration
	);

	/**
	 * Сохраняет монотонный progress и продлевает lease текущего owner.
	 *
	 * @param attempt captured token и exact target владельца
	 * @param progress абсолютные counters текущей attempt
	 * @param leaseDuration новый срок ownership от текущего времени
	 * @return результат conditional transition
	 */
	AttemptTransitionResult checkpoint(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			Duration leaseDuration
	);

	/**
	 * Завершает processing после подтвержденного Elasticsearch receipt.
	 *
	 * @param attempt captured token и exact target владельца
	 * @param progress итоговые counters
	 * @param verification полное совпавшее count+digest evidence
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markIndexed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification
	);

	/**
	 * Сохраняет processing failure и partial counters текущего owner.
	 *
	 * @param attempt captured token и exact target владельца
	 * @param failure безопасная failure projection
	 * @param progress partial counters
	 * @param verification receipt mismatch evidence либо {@code null}
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markFailed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification
	);

	/**
	 * Условно обновляет diagnostic receipt успешно проверенного INDEXED state.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param expectedProcessingFingerprint fingerprint проверенного state
	 * @param expectedAttemptCount число attempts проверенного state
	 * @param expectedStateVersion durable version проверенного state
	 * @param expectedStoredTargetBinding binding сохраненного INDEXED state для CAS
	 * @param verifiedCurrentTargetBinding проверенный current ACTIVE target
	 * @param verification совпавшее count+digest evidence current target
	 * @return APPLIED либо OWNERSHIP_LOST при изменившемся state или current target
	 */
	AttemptTransitionResult recordReceiptMatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification
	);

	/**
	 * Условно переводит INDEXED state в FAILED после несовпавшего receipt.
	 *
	 * @param archiveIdempotencyKey стабильный source archive key
	 * @param expectedProcessingFingerprint fingerprint проверенного state
	 * @param expectedAttemptCount число attempts проверенного state
	 * @param expectedStateVersion durable version проверенного state
	 * @param expectedStoredTargetBinding binding сохраненного INDEXED state для CAS
	 * @param verifiedCurrentTargetBinding проверенный current ACTIVE target
	 * @param verification несовпавшее count+digest evidence current target
	 * @param failure retryable shortage либо non-retryable surplus
	 * @return APPLIED либо OWNERSHIP_LOST при изменившемся state
	 */
	AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification,
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
