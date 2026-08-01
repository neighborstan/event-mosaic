package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Запрос проверки приемочной квитанции архива.
 *
 * @param kind вид документов
 * @param target exact physical target подтвержденной generation
 * @param sourceArchiveKey точный ключ исходного архива
 * @param processingFingerprint fingerprint версии processing projection
 * @param expectedDocumentCount ожидаемое число индексированных документов
 * @param expectedDigest digest document identities в физическом порядке source
 * @param pageSize максимальный размер bounded PIT/search_after страницы
 */
public record ArchiveReceiptQuery(
		GdeltIndexKind kind,
		ExactIndexTarget target,
		String sourceArchiveKey,
		String processingFingerprint,
		long expectedDocumentCount,
		ArchiveIdentityDigest expectedDigest,
		int pageSize
) {

	/**
	 * Проверяет обязательные параметры запроса.
	 */
	public ArchiveReceiptQuery {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(target, "target must not be null");
		if (!kind.accepts(target)) {
			throw new IllegalArgumentException("target must match query kind");
		}
		if (sourceArchiveKey == null || sourceArchiveKey.isBlank()) {
			throw new IllegalArgumentException("sourceArchiveKey must not be blank");
		}
		if (processingFingerprint == null || processingFingerprint.isBlank()) {
			throw new IllegalArgumentException("processingFingerprint must not be blank");
		}
		if (expectedDocumentCount < 0) {
			throw new IllegalArgumentException("expectedDocumentCount must not be negative");
		}
		Objects.requireNonNull(expectedDigest, "expectedDigest must not be null");
		if (pageSize <= 0 || pageSize > 10_000) {
			throw new IllegalArgumentException("pageSize must be between 1 and 10000");
		}
	}

}
