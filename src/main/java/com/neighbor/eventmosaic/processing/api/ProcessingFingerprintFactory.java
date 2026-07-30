package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;

/**
 * Строит versioned processing fingerprint из source и projection revisions.
 */
public interface ProcessingFingerprintFactory {

	/**
	 * Вычисляет детерминированный fingerprint.
	 *
	 * @param sourceArchiveKey точный ключ версии архива
	 * @param kind вид архива
	 * @param projectionRevision revision processing mapping
	 * @param indexMappingRevision revision Elasticsearch mapping
	 * @return versioned lowercase SHA-256
	 */
	String create(
			String sourceArchiveKey,
			GdeltArchiveKind kind,
			String projectionRevision,
			String indexMappingRevision);
}
