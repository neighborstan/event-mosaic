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
 */
public record ArchiveReceiptQuery(
		GdeltIndexKind kind,
		ExactIndexTarget target,
		String sourceArchiveKey,
		String processingFingerprint,
		long expectedDocumentCount
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
	}

}
