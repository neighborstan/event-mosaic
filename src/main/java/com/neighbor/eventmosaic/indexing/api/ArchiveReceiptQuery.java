package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Запрос проверки приемочной квитанции архива.
 *
 * @param kind вид документов
 * @param sourceArchiveKey точный ключ исходного архива
 * @param expectedDocumentCount ожидаемое число индексированных документов
 */
public record ArchiveReceiptQuery(
		GdeltIndexKind kind,
		String sourceArchiveKey,
		long expectedDocumentCount
) {

	/**
	 * Проверяет обязательные параметры запроса.
	 */
	public ArchiveReceiptQuery {
		Objects.requireNonNull(kind, "kind must not be null");
		if (sourceArchiveKey == null || sourceArchiveKey.isBlank()) {
			throw new IllegalArgumentException("sourceArchiveKey must not be blank");
		}
		if (expectedDocumentCount < 0) {
			throw new IllegalArgumentException("expectedDocumentCount must not be negative");
		}
	}

}
