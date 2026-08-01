package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import java.util.Objects;

/**
 * Итог одного ограниченного archive processing attempt.
 *
 * @param kind вид архива
 * @param outcome итог attempt
 * @param progress финальные подтвержденные counters
 * @param failure ожидаемый failure или {@code null}
 * @param receipt terminal receipt verification или {@code null}
 */
public record ArchiveProcessingResult(
		GdeltArchiveKind kind,
		ArchiveProcessingOutcome outcome,
		ArchiveProcessingProgress progress,
		ArchiveProcessingFailure failure,
		ArchiveReceiptVerification receipt
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
			if (receipt == null || !receipt.matched()) {
				throw new IllegalArgumentException(
						"completed outcome must contain matched receipt");
			}
		}
		if (outcome == ArchiveProcessingOutcome.OWNERSHIP_LOST && receipt != null) {
			throw new IllegalArgumentException(
					"ownership-lost outcome must not contain receipt");
		}
		if (outcome == ArchiveProcessingOutcome.FAILED
				&& receipt != null
				&& receipt.matched()) {
			throw new IllegalArgumentException(
					"failed outcome must not contain matched receipt");
		}
	}

	/** Создает успешный terminal result. */
	public static ArchiveProcessingResult completed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification receipt
	) {
		return new ArchiveProcessingResult(
				kind,
				ArchiveProcessingOutcome.COMPLETED,
				progress,
				null,
				Objects.requireNonNull(receipt, "receipt must not be null"));
	}

	/** Создает ожидаемый failed result. */
	public static ArchiveProcessingResult failed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingFailure failure
	) {
		return failed(kind, progress, failure, null);
	}

	/** Создает failed result с подтвержденным mismatched receipt evidence. */
	public static ArchiveProcessingResult failed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingFailure failure,
			ArchiveReceiptVerification receipt
	) {
		return new ArchiveProcessingResult(
				kind,
				ArchiveProcessingOutcome.FAILED,
				progress,
				Objects.requireNonNull(
						failure,
						"failure must not be null"),
				receipt);
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
				null,
				null);
	}
}
