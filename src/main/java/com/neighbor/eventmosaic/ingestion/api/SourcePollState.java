package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Полное current state latest source poll без append-only attempt history.
 *
 * @param sourceName стабильное имя source
 * @param status lifecycle latest poll
 * @param attempt ownership и retry state
 * @param failure последняя failure либо {@code null}
 * @param firstSeenAt время создания current state
 * @param lastSucceededAt время последнего successful poll либо {@code null}
 */
public record SourcePollState(
		String sourceName,
		SourcePollStatus status,
		SourcePollAttemptState attempt,
		RecordedIngestionFailure failure,
		Instant firstSeenAt,
		Instant lastSucceededAt
) {

	/** Проверяет lifecycle invariants current poll state. */
	public SourcePollState {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		Objects.requireNonNull(status, "status must not be null");
		Objects.requireNonNull(attempt, "attempt must not be null");
		Objects.requireNonNull(firstSeenAt, "firstSeenAt must not be null");
		if (sourceName.isBlank()) {
			throw new IllegalArgumentException("sourceName must not be blank");
		}
		switch (status) {
			case IDLE -> requireIdle(attempt, failure);
			case POLLING -> requirePolling(attempt, failure);
			case FAILED -> requireFailed(attempt, failure);
		}
	}

	private static void requireIdle(
			SourcePollAttemptState attempt,
			RecordedIngestionFailure failure
	) {
		if (attempt.token() != null || attempt.retry().retryNotBefore() != null || failure != null) {
			throw new IllegalArgumentException("IDLE poll must not contain ownership or failure");
		}
	}

	private static void requirePolling(
			SourcePollAttemptState attempt,
			RecordedIngestionFailure failure
	) {
		if (attempt.token() == null || failure != null) {
			throw new IllegalArgumentException("POLLING state must contain only active ownership");
		}
	}

	private static void requireFailed(
			SourcePollAttemptState attempt,
			RecordedIngestionFailure failure
	) {
		if (attempt.token() != null || failure == null) {
			throw new IllegalArgumentException("FAILED poll must contain terminal failure");
		}
		boolean duePresent = attempt.retry().retryNotBefore() != null;
		if (failure.failure().retryable() != duePresent) {
			throw new IllegalArgumentException("retryNotBefore must be present only for retryable poll failure");
		}
	}
}
