package com.neighbor.eventmosaic.ingestion.api;

import java.util.List;
import java.util.Objects;

/**
 * Результат проверки полноты Event-слотов и объединенные интервалы пропусков.
 *
 * @param status итог проверки полноты
 * @param missingIntervals отсортированные объединенные пропуски или {@code null} для UNKNOWN
 */
public record IngestionCoverageEvidence(
		IngestionCoverageStatus status,
		List<IngestionCoverageInterval> missingIntervals
) {

	/** Проверяет согласованность статуса и формы списка пропусков. */
	public IngestionCoverageEvidence {
		Objects.requireNonNull(status, "status must not be null");
		if (status == IngestionCoverageStatus.UNKNOWN) {
			if (missingIntervals != null) {
				throw new IllegalArgumentException("UNKNOWN coverage must not expose missing intervals");
			}
		} else {
			Objects.requireNonNull(missingIntervals, "missingIntervals must not be null");
			missingIntervals = List.copyOf(missingIntervals);
			if (status == IngestionCoverageStatus.COMPLETE && !missingIntervals.isEmpty()) {
				throw new IllegalArgumentException("COMPLETE coverage must not contain missing intervals");
			}
			if (status == IngestionCoverageStatus.PARTIAL && missingIntervals.isEmpty()) {
				throw new IllegalArgumentException("PARTIAL coverage must contain missing intervals");
			}
			validateMergedIntervals(missingIntervals);
		}
	}

	/** Возвращает подтвержденную полноту без пропусков. */
	public static IngestionCoverageEvidence complete() {
		return new IngestionCoverageEvidence(IngestionCoverageStatus.COMPLETE, List.of());
	}

	/** Возвращает частичную полноту с уже объединенными пропусками. */
	public static IngestionCoverageEvidence partial(
			List<IngestionCoverageInterval> missingIntervals
	) {
		return new IngestionCoverageEvidence(IngestionCoverageStatus.PARTIAL, missingIntervals);
	}

	/** Возвращает состояние, в котором полноту нельзя доказать. */
	public static IngestionCoverageEvidence unknown() {
		return new IngestionCoverageEvidence(IngestionCoverageStatus.UNKNOWN, null);
	}

	private static void validateMergedIntervals(List<IngestionCoverageInterval> intervals) {
		for (int index = 1; index < intervals.size(); index++) {
			IngestionCoverageInterval previous = intervals.get(index - 1);
			IngestionCoverageInterval current = intervals.get(index);
			if (!previous.to().isBefore(current.from())) {
				throw new IllegalArgumentException(
						"Missing intervals must be ordered, disjoint and non-adjacent");
			}
		}
	}
}
