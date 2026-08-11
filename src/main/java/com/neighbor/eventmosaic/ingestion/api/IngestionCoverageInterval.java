package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Полуоткрытый интервал отсутствующих Event-данных: начало входит в диапазон,
 * конец не входит.
 *
 * @param from включенная нижняя граница
 * @param to исключенная верхняя граница
 */
public record IngestionCoverageInterval(Instant from, Instant to) {

	/** Проверяет, что интервал задан двумя разными границами в правильном порядке. */
	public IngestionCoverageInterval {
		Objects.requireNonNull(from, "from must not be null");
		Objects.requireNonNull(to, "to must not be null");
		if (!from.isBefore(to)) {
			throw new IllegalArgumentException("from must be before to");
		}
	}
}
