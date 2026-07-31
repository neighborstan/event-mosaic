package com.neighbor.eventmosaic.indexing.api;

import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Immutable P7D logical partition boundary.
 *
 * @param partitionKey стабильный key, используемый в physical names
 * @param startAt включительная UTC boundary
 * @param endAt исключительная UTC boundary
 * @param interval подтвержденный ISO period
 */
public record IndexPartitionDefinition(
		String partitionKey,
		Instant startAt,
		Instant endAt,
		Period interval
) {
	private static final Pattern PARTITION_KEY = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,31}$");
	private static final Period TARGET_INTERVAL = Period.ofDays(7);

	/** Проверяет exact P7D UTC range и bounded key. */
	public IndexPartitionDefinition {
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		Objects.requireNonNull(startAt, "startAt must not be null");
		Objects.requireNonNull(endAt, "endAt must not be null");
		Objects.requireNonNull(interval, "interval must not be null");
		if (!PARTITION_KEY.matcher(partitionKey).matches()) {
			throw new IllegalArgumentException("partitionKey must be a bounded lowercase key");
		}
		if (!TARGET_INTERVAL.equals(interval)) {
			throw new IllegalArgumentException("partition interval must be P7D");
		}
		if (!startAt.plus(Duration.ofDays(7)).equals(endAt)) {
			throw new IllegalArgumentException("partition boundaries must span exactly P7D");
		}
	}
}
