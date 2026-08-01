package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import java.time.Instant;
import java.util.Objects;

/**
 * Aggregate receipt exact physical target с count и canonical identity digest.
 *
 * @param expectedDocumentCount ожидаемое число successful operations
 * @param actualDocumentCount count exact archive query
 * @param digestAlgorithm версия canonical digest algorithm
 * @param expectedIdentityDigest ожидаемый digest identities
 * @param actualIdentityDigest фактический digest identities
 * @param verifiedGenerationId generation, на которой выполнена проверка
 * @param verifiedIndexUuid exact Elasticsearch index UUID
 * @param verifiedAt время receipt query
 */
public record ArchiveProcessingReceipt(
		long expectedDocumentCount,
		long actualDocumentCount,
		String digestAlgorithm,
		String expectedIdentityDigest,
		String actualIdentityDigest,
		long verifiedGenerationId,
		String verifiedIndexUuid,
		Instant verifiedAt
) {

	/** Проверяет canonical digest contract и exact target evidence. */
	public ArchiveProcessingReceipt {
		if (expectedDocumentCount < 0 || actualDocumentCount < 0) {
			throw new IllegalArgumentException("Receipt counts must not be negative");
		}
		Objects.requireNonNull(digestAlgorithm, "digestAlgorithm must not be null");
		if (!ArchiveIdentityDigest.ALGORITHM.equals(digestAlgorithm)) {
			throw new IllegalArgumentException("Unsupported receipt digest algorithm");
		}
		new ArchiveIdentityDigest(expectedIdentityDigest);
		new ArchiveIdentityDigest(actualIdentityDigest);
		if (verifiedGenerationId <= 0) {
			throw new IllegalArgumentException("verifiedGenerationId must be positive");
		}
		Objects.requireNonNull(verifiedIndexUuid, "verifiedIndexUuid must not be null");
		if (verifiedIndexUuid.isBlank()
				|| verifiedIndexUuid.indexOf('*') >= 0
				|| verifiedIndexUuid.indexOf('?') >= 0
				|| verifiedIndexUuid.indexOf(',') >= 0
				|| verifiedIndexUuid.chars().anyMatch(Character::isWhitespace)) {
			throw new IllegalArgumentException("verifiedIndexUuid must be exact");
		}
		Objects.requireNonNull(verifiedAt, "verifiedAt must not be null");
	}

	/**
	 * Возвращает признак полного совпадения count и digest.
	 *
	 * @return {@code true}, если receipt подтверждает expected archive identities
	 */
	public boolean matched() {
		return expectedDocumentCount == actualDocumentCount
				&& expectedIdentityDigest.equals(actualIdentityDigest);
	}
}
