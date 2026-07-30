package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import java.util.Objects;

/**
 * Итог одного ограниченного archive processing attempt.
 *
 * @param kind вид архива
 * @param outcome итог attempt
 * @param progress финальные подтвержденные counters
 * @param failure ожидаемый failure или {@code null}
 */
public record ArchiveProcessingResult(
		GdeltArchiveKind kind,
		ArchiveProcessingOutcome outcome,
		ArchiveProcessingProgress progress,
		ArchiveProcessingFailure failure
) {

	/** Проверяет соответствие outcome и failure projection. */
	public ArchiveProcessingResult {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(outcome, "outcome must not be null");
		Objects.requireNonNull(progress, "progress must not be null");
		if ((outcome == ArchiveProcessingOutcome.FAILED) != (failure != null)) {
			throw new IllegalArgumentException("only failed outcome must contain failure");
		}
		if (outcome == ArchiveProcessingOutcome.COMPLETED) {
			progress.requireCompleted();
		}
	}

	/** Создает успешный terminal result. */
	public static ArchiveProcessingResult completed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress
	) {
		return new ArchiveProcessingResult(
				kind,
				ArchiveProcessingOutcome.COMPLETED,
				progress,
				null);
	}

	/** Создает ожидаемый failed result. */
	public static ArchiveProcessingResult failed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingFailure failure
	) {
		return new ArchiveProcessingResult(
				kind,
				ArchiveProcessingOutcome.FAILED,
				progress,
				Objects.requireNonNull(
						failure,
						"failure must not be null"));
	}

	/** Создает concurrency outcome без внешнего failure. */
	public static ArchiveProcessingResult ownershipLost(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress
	) {
		return new ArchiveProcessingResult(
				kind,
				ArchiveProcessingOutcome.OWNERSHIP_LOST,
				progress,
				null);
	}
}
