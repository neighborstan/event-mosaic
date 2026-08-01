package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Count-only receipt exact physical target для checkpoint BDH-C03.
 *
 * @param expectedDocumentCount ожидаемое число successful operations
 * @param actualDocumentCount count exact archive query
 * @param verifiedGenerationId generation, на которой выполнена проверка
 * @param verifiedIndexUuid exact Elasticsearch index UUID
 * @param verifiedAt время receipt query
 */
public record ArchiveProcessingReceipt(
		long expectedDocumentCount,
		long actualDocumentCount,
		long verifiedGenerationId,
		String verifiedIndexUuid,
		Instant verifiedAt
) {

	/** Проверяет exact count equality и target evidence. */
	public ArchiveProcessingReceipt {
		if (expectedDocumentCount < 0 || actualDocumentCount < 0) {
			throw new IllegalArgumentException("Receipt counts must not be negative");
		}
		if (expectedDocumentCount != actualDocumentCount) {
			throw new IllegalArgumentException("Count-only receipt must match expected count");
		}
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
}
