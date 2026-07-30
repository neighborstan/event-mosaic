package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Строит processing fingerprint через versioned length-prefixed SHA-256.
 */
@Component
final class DefaultProcessingFingerprintFactory implements ProcessingFingerprintFactory {

	private static final String DOMAIN = "gdelt-processing-fingerprint-v1";

	@Override
	public String create(
			String sourceArchiveKey,
			GdeltArchiveKind kind,
			String projectionRevision,
			String indexMappingRevision
	) {
		requireText(sourceArchiveKey, "sourceArchiveKey");
		Objects.requireNonNull(kind, "kind must not be null");
		requireText(projectionRevision, "projectionRevision");
		requireText(indexMappingRevision, "indexMappingRevision");
		return FramedSha256.create()
				.putString(DOMAIN)
				.putString(sourceArchiveKey)
				.putString(kind.name())
				.putString(projectionRevision)
				.putString(indexMappingRevision)
				.finishHex();
	}

	private static void requireText(String value, String fieldName) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName + " must not be blank");
		}
	}
}
