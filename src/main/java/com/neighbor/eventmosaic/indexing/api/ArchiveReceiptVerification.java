package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Приемочная квитанция точного исходного архива.
 *
 * @param kind вид документов
 * @param expectedDocumentCount ожидаемое число документов
 * @param actualDocumentCount фактическое число документов
 * @param status результат сравнения
 */
public record ArchiveReceiptVerification(
		GdeltIndexKind kind,
		long expectedDocumentCount,
		long actualDocumentCount,
		ArchiveReceiptStatus status
) {

	/**
	 * Проверяет согласованность статуса и счетчиков.
	 */
	public ArchiveReceiptVerification {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(status, "status must not be null");
		if (expectedDocumentCount < 0 || actualDocumentCount < 0) {
			throw new IllegalArgumentException("receipt counters must not be negative");
		}
		if (status == ArchiveReceiptStatus.MATCHED && expectedDocumentCount != actualDocumentCount) {
			throw new IllegalArgumentException("matched receipt must contain equal counters");
		}
		if (status == ArchiveReceiptStatus.MISMATCHED && expectedDocumentCount == actualDocumentCount) {
			throw new IllegalArgumentException("mismatched receipt must contain different counters");
		}
		if (status == ArchiveReceiptStatus.INDEX_ABSENT && actualDocumentCount != 0) {
			throw new IllegalArgumentException("absent index must have zero actual documents");
		}
	}

	/**
	 * Возвращает признак успешной приемки архива.
	 *
	 * @return {@code true}, если число документов совпало
	 */
	public boolean matched() {
		return status == ArchiveReceiptStatus.MATCHED;
	}

}
