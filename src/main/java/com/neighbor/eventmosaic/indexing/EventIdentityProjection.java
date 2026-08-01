package com.neighbor.eventmosaic.indexing;

/**
 * Узкая Elasticsearch projection source provenance существующего Event.
 *
 * @param sourceArchiveKey точный ключ исходного архива
 * @param sourceLineNumber физическая строка исходного CSV
 * @param processingFingerprint fingerprint processing projection
 */
record EventIdentityProjection(
		String sourceArchiveKey,
		Long sourceLineNumber,
		String processingFingerprint
) {
}
