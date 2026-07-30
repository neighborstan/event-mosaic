package com.neighbor.eventmosaic.processing;

/**
 * Раздельно строит identity raw Mention observation и source document.
 */
final class MentionIdentityFactory {

	private static final String RAW_PREFIX = "rm1-";
	private static final String RAW_DOMAIN = "gdelt-raw-mention-v1";
	private static final String DOCUMENT_PREFIX = "sd1-";
	private static final String DOCUMENT_DOMAIN = "gdelt-source-document-v1";

	String rawMentionId(String sourceArchiveKey, long sourceLineNumber) {
		requireText(sourceArchiveKey, "sourceArchiveKey");
		if (sourceLineNumber <= 0) {
			throw new IllegalArgumentException("sourceLineNumber must be positive");
		}
		return RAW_PREFIX + FramedSha256.create()
				.putString(RAW_DOMAIN)
				.putString(sourceArchiveKey)
				.putLong(sourceLineNumber)
				.finishHex();
	}

	String sourceDocumentKey(
			long globalEventId,
			int mentionType,
			String mentionIdentifier
	) {
		if (globalEventId <= 0) {
			throw new IllegalArgumentException("globalEventId must be positive");
		}
		requireText(mentionIdentifier, "mentionIdentifier");
		return DOCUMENT_PREFIX + FramedSha256.create()
				.putString(DOCUMENT_DOMAIN)
				.putLong(globalEventId)
				.putInt(mentionType)
				.putString(mentionIdentifier)
				.finishHex();
	}

	private static void requireText(String value, String fieldName) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(fieldName + " must not be blank");
		}
	}
}
