package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Durable processing failure вместе со временем terminal transition.
 *
 * @param failure безопасная failure projection
 * @param occurredAt UTC-время фиксации
 */
public record RecordedArchiveProcessingFailure(
		ArchiveProcessingFailure failure,
		Instant occurredAt
) {

	/**
	 * Проверяет полноту failure state.
	 */
	public RecordedArchiveProcessingFailure {
		Objects.requireNonNull(failure, "failure must not be null");
		Objects.requireNonNull(occurredAt, "occurredAt must not be null");
	}
}
