package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Полное durable processing state одного Event или Mention archive.
 *
 * @param archiveIdempotencyKey стабильный source archive key
 * @param fingerprint неизменяемая processing identity
 * @param status lifecycle status downstream processing
 * @param attempt сохраненное состояние attempts
 * @param targetBinding captured exact target для PROCESSING или INDEXED state
 * @param progress абсолютные counters последней или текущей attempt
 * @param receipt count-only exact target evidence либо {@code null}
 * @param failure последняя failure либо {@code null}
 * @param firstSeenAt время регистрации processing row
 * @param completedAt время подтвержденного indexing либо {@code null}
 */
public record ArchiveProcessingState(
		String archiveIdempotencyKey,
		ArchiveProcessingFingerprint fingerprint,
		ArchiveProcessingStatus status,
		ArchiveProcessingAttemptState attempt,
		ArchiveProcessingTargetBinding targetBinding,
		ArchiveProcessingProgress progress,
		ArchiveProcessingReceipt receipt,
		RecordedArchiveProcessingFailure failure,
		Instant firstSeenAt,
		Instant completedAt
) {

	/**
	 * Проверяет обязательные части durable state.
	 */
	public ArchiveProcessingState {
		Objects.requireNonNull(archiveIdempotencyKey, "archiveIdempotencyKey must not be null");
		Objects.requireNonNull(fingerprint, "fingerprint must not be null");
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(attempt, "attempt must not be null");
		Objects.requireNonNull(progress, "progress must not be null");
		Objects.requireNonNull(firstSeenAt, "firstSeenAt must not be null");
		if (archiveIdempotencyKey.isBlank()) {
			throw new IllegalArgumentException("archiveIdempotencyKey must not be blank");
		}
		validateLifecycle(status, attempt, targetBinding, progress, receipt, failure, completedAt);
	}

	private static void validateLifecycle(
			ArchiveProcessingStatus status,
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingTargetBinding targetBinding,
			ArchiveProcessingProgress progress,
			ArchiveProcessingReceipt receipt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		switch (status) {
			case PENDING -> requirePending(
					attempt, targetBinding, progress, receipt, failure, completedAt);
			case PROCESSING -> requireProcessing(
					attempt, targetBinding, receipt, failure, completedAt);
			case FAILED -> requireFailed(attempt, targetBinding, failure, completedAt);
			case INDEXED -> requireIndexed(
					attempt, targetBinding, progress, receipt, failure, completedAt);
		}
	}

	private static void requirePending(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingTargetBinding targetBinding,
			ArchiveProcessingProgress progress,
			ArchiveProcessingReceipt receipt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() != 0
				|| targetBinding != null
				|| !progress.equals(ArchiveProcessingProgress.empty())
				|| receipt != null
				|| failure != null
				|| completedAt != null) {
			throw new IllegalArgumentException("PENDING state must not contain attempt outcome");
		}
	}

	private static void requireProcessing(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingTargetBinding targetBinding,
			ArchiveProcessingReceipt receipt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() == null
				|| targetBinding == null
				|| receipt != null
				|| failure != null
				|| completedAt != null) {
			throw new IllegalArgumentException(
					"PROCESSING state must contain only active attempt ownership");
		}
	}

	private static void requireFailed(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingTargetBinding targetBinding,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() != null
				|| targetBinding != null
				|| failure == null
				|| completedAt != null) {
			throw new IllegalArgumentException("FAILED state must contain only terminal failure");
		}
	}

	private static void requireIndexed(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingTargetBinding targetBinding,
			ArchiveProcessingProgress progress,
			ArchiveProcessingReceipt receipt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() != null
				|| targetBinding == null
				|| receipt == null
				|| failure != null
				|| completedAt == null) {
			throw new IllegalArgumentException("INDEXED state must contain only completion outcome");
		}
		progress.requireIndexedCompletion();
		if (receipt.expectedDocumentCount() != progress.succeededOperations()
				|| receipt.actualDocumentCount() != progress.receiptDocuments()
				|| receipt.verifiedGenerationId() != targetBinding.generationId()
				|| !receipt.verifiedIndexUuid().equals(targetBinding.indexUuid())) {
			throw new IllegalArgumentException("INDEXED receipt must match captured target");
		}
	}
}
