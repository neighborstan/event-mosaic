package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;
import java.util.Optional;

/**
 * Typed claim outcome без исключения для ожидаемого maintenance или stale target.
 *
 * @param status outcome claim
 * @param attempt captured ownership только для {@link ArchiveProcessingClaimStatus#CLAIMED}
 */
public record ArchiveProcessingClaimResult(
		ArchiveProcessingClaimStatus status,
		ArchiveProcessingAttempt attempt
) {

	/** Проверяет согласованность status и optional ownership. */
	public ArchiveProcessingClaimResult {
		Objects.requireNonNull(status, "status must not be null");
		if ((status == ArchiveProcessingClaimStatus.CLAIMED) != (attempt != null)) {
			throw new IllegalArgumentException("Only CLAIMED result must contain an attempt");
		}
	}

	/** Возвращает успешную ownership attempt. */
	public static ArchiveProcessingClaimResult claimed(ArchiveProcessingAttempt attempt) {
		return new ArchiveProcessingClaimResult(
				ArchiveProcessingClaimStatus.CLAIMED,
				Objects.requireNonNull(attempt, "attempt must not be null"));
	}

	/** Возвращает ожидаемый outcome без ownership. */
	public static ArchiveProcessingClaimResult outcome(ArchiveProcessingClaimStatus status) {
		if (status == ArchiveProcessingClaimStatus.CLAIMED) {
			throw new IllegalArgumentException("CLAIMED outcome requires an attempt");
		}
		return new ArchiveProcessingClaimResult(status, null);
	}

	/** Возвращает attempt как Optional для claim-style orchestration. */
	public Optional<ArchiveProcessingAttempt> claimedAttempt() {
		return Optional.ofNullable(attempt);
	}
}
