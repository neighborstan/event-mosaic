package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Каталогизированная ingestion failure вместе со временем durable transition.
 *
 * @param failure безопасное описание отказа
 * @param occurredAt UTC-время фиксации отказа в ledger
 */
public record RecordedIngestionFailure(IngestionFailure failure, Instant occurredAt) {

	/**
	 * Проверяет полноту записанной failure.
	 */
	public RecordedIngestionFailure {
		Objects.requireNonNull(failure, "failure must not be null");
		Objects.requireNonNull(occurredAt, "occurredAt must not be null");
	}
}
