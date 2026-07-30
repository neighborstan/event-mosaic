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
 * @param progress абсолютные counters последней или текущей attempt
 * @param failure последняя failure либо {@code null}
 * @param firstSeenAt время регистрации processing row
 * @param completedAt время подтвержденного indexing либо {@code null}
 */
public record ArchiveProcessingState(
		String archiveIdempotencyKey,
		ArchiveProcessingFingerprint fingerprint,
		ArchiveProcessingStatus status,
		ArchiveProcessingAttemptState attempt,
		ArchiveProcessingProgress progress,
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
		validateLifecycle(status, attempt, progress, failure, completedAt);
	}

	private static void validateLifecycle(
			ArchiveProcessingStatus status,
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingProgress progress,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		switch (status) {
			case PENDING -> requirePending(attempt, progress, failure, completedAt);
			case PROCESSING -> requireProcessing(attempt, failure, completedAt);
			case FAILED -> requireFailed(attempt, failure, completedAt);
			case INDEXED -> requireIndexed(attempt, progress, failure, completedAt);
		}
	}

	private static void requirePending(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingProgress progress,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() != 0
				|| !progress.equals(ArchiveProcessingProgress.empty())
				|| failure != null
				|| completedAt != null) {
			throw new IllegalArgumentException("PENDING state must not contain attempt outcome");
		}
	}

	private static void requireProcessing(
			ArchiveProcessingAttemptState attempt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() == null
				|| failure != null
				|| completedAt != null) {
			throw new IllegalArgumentException(
					"PROCESSING state must contain only active attempt ownership");
		}
	}

	private static void requireFailed(
			ArchiveProcessingAttemptState attempt,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() != null
				|| failure == null
				|| completedAt != null) {
			throw new IllegalArgumentException("FAILED state must contain only terminal failure");
		}
	}

	private static void requireIndexed(
			ArchiveProcessingAttemptState attempt,
			ArchiveProcessingProgress progress,
			RecordedArchiveProcessingFailure failure,
			Instant completedAt
	) {
		if (attempt.count() == 0
				|| attempt.token() != null
				|| failure != null
				|| completedAt == null) {
			throw new IllegalArgumentException("INDEXED state must contain only completion outcome");
		}
		progress.requireIndexedCompletion();
	}
}
