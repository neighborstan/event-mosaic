package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable ownership и retry state latest source poll.
 *
 * @param count общее число начатых poll attempts
 * @param token token текущего owner или {@code null}
 * @param lastAttemptAt время последнего claim или {@code null}
 * @param leaseExpiresAt окончание текущего lease или {@code null}
 * @param retry automatic retry counters и due boundary
 */
public record SourcePollAttemptState(
		int count,
		UUID token,
		Instant lastAttemptAt,
		Instant leaseExpiresAt,
		AutomaticRetryState retry
) {

	/** Проверяет ownership, attempt history и retry state. */
	public SourcePollAttemptState {
		if (count < 0) {
			throw new IllegalArgumentException("count must not be negative");
		}
		Objects.requireNonNull(retry, "retry must not be null");
		if (retry.automaticRetriesUsed() > Math.max(count - 1, 0)) {
			throw new IllegalArgumentException(
					"automaticRetriesUsed must not exceed completed automatic claims");
		}
		if (retry.consecutiveRetryableFailures() > count) {
			throw new IllegalArgumentException(
					"consecutiveRetryableFailures must not exceed attempt count");
		}
		if ((token == null) != (leaseExpiresAt == null)) {
			throw new IllegalArgumentException("token and leaseExpiresAt must be present together");
		}
		if (count == 0 && (lastAttemptAt != null || token != null)) {
			throw new IllegalArgumentException("unattempted state must not contain attempt metadata");
		}
		if (count > 0 && lastAttemptAt == null) {
			throw new IllegalArgumentException("attempted state must contain lastAttemptAt");
		}
		if (token != null && !leaseExpiresAt.isAfter(lastAttemptAt)) {
			throw new IllegalArgumentException("leaseExpiresAt must be after lastAttemptAt");
		}
		if (token != null && retry.retryNotBefore() != null) {
			throw new IllegalArgumentException("active poll must not contain retryNotBefore");
		}
	}
}
