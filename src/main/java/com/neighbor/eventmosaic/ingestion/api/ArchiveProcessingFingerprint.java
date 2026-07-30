package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Неизменяемая идентичность входа и версии processing projection.
 *
 * @param sourceFingerprint fingerprint проверенного source artifact
 * @param projectionRevision версия mapping и Elasticsearch projection
 * @param processingFingerprint versioned SHA-256 всего processing contract
 */
public record ArchiveProcessingFingerprint(
		String sourceFingerprint,
		String projectionRevision,
		String processingFingerprint
) {

	private static final int MAX_SOURCE_FINGERPRINT_LENGTH = 128;
	private static final int MAX_PROJECTION_REVISION_LENGTH = 64;
	private static final Pattern SHA_256 = Pattern.compile("^[0-9a-f]{64}$");

	/**
	 * Проверяет bounded metadata и канонический lowercase SHA-256.
	 */
	public ArchiveProcessingFingerprint {
		requireNonBlankBounded(
				sourceFingerprint,
				"sourceFingerprint",
				MAX_SOURCE_FINGERPRINT_LENGTH);
		requireNonBlankBounded(
				projectionRevision,
				"projectionRevision",
				MAX_PROJECTION_REVISION_LENGTH);
		Objects.requireNonNull(processingFingerprint, "processingFingerprint must not be null");
		if (!SHA_256.matcher(processingFingerprint).matches()) {
			throw new IllegalArgumentException(
					"processingFingerprint must be a lowercase SHA-256");
		}
	}

	private static void requireNonBlankBounded(String value, String name, int maxLength) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isBlank() || value.length() > maxLength) {
			throw new IllegalArgumentException(name + " must be non-blank and bounded");
		}
	}
}
