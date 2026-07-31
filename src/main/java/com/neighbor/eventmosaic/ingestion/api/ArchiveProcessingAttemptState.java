package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.UUID;

/**
 * Сохраненное состояние processing attempts и текущего lease.
 *
 * @param count число начатых попыток
 * @param token token текущего owner или {@code null}
 * @param lastAttemptAt время последнего claim или {@code null}
 * @param leaseExpiresAt окончание текущего lease или {@code null}
 * @param retry automatic retry counters и due boundary
 */
public record ArchiveProcessingAttemptState(
		int count,
		UUID token,
		Instant lastAttemptAt,
		Instant leaseExpiresAt,
		AutomaticRetryState retry
) {

	/**
	 * Проверяет счетчик, историю claim и парность token/lease.
	 */
	public ArchiveProcessingAttemptState {
		if (count < 0) {
			throw new IllegalArgumentException("count must not be negative");
		}
		if (retry == null) {
			throw new NullPointerException("retry must not be null");
		}
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
			throw new IllegalArgumentException("active attempt must not contain retryNotBefore");
		}
	}
}
