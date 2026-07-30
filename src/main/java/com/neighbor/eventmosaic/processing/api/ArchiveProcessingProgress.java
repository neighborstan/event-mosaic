package com.neighbor.eventmosaic.processing.api;

/**
 * Монотонный снимок ограниченных счетчиков обработки одного архива.
 *
 * @param deliveredRecords записи, доставленные CSV reader в processing
 * @param sourceInvalidRecords локально отклоненные CSV reader записи
 * @param mappingRejectedRecords локально отклоненные processing mapper записи
 * @param submittedOperations отправленные Elasticsearch операции
 * @param succeededOperations подтвержденные успешные операции
 * @param failedOperations подтвержденные неуспешные операции
 * @param receiptDocuments документы подтвержденной приемочной квитанции
 * @param firstFailedLineNumber первая строка подтвержденного bulk failure
 */
public record ArchiveProcessingProgress(
		long deliveredRecords,
		long sourceInvalidRecords,
		long mappingRejectedRecords,
		long submittedOperations,
		long succeededOperations,
		long failedOperations,
		long receiptDocuments,
		Long firstFailedLineNumber
) {

	/** Проверяет неотрицательные counters и арифметические invariants. */
	public ArchiveProcessingProgress {
		requireNonNegative(deliveredRecords, "deliveredRecords");
		requireNonNegative(sourceInvalidRecords, "sourceInvalidRecords");
		requireNonNegative(mappingRejectedRecords, "mappingRejectedRecords");
		requireNonNegative(submittedOperations, "submittedOperations");
		requireNonNegative(succeededOperations, "succeededOperations");
		requireNonNegative(failedOperations, "failedOperations");
		requireNonNegative(receiptDocuments, "receiptDocuments");
		if (mappingRejectedRecords > deliveredRecords) {
			throw new IllegalArgumentException(
					"mappingRejectedRecords must not exceed deliveredRecords");
		}
		long mappedRecords = deliveredRecords - mappingRejectedRecords;
		if (submittedOperations > mappedRecords) {
			throw new IllegalArgumentException(
					"submittedOperations must not exceed mapped records");
		}
		if (succeededOperations > submittedOperations
				|| failedOperations > submittedOperations - succeededOperations) {
			throw new IllegalArgumentException(
					"confirmed operations must not exceed submittedOperations");
		}
		if ((failedOperations == 0) != (firstFailedLineNumber == null)) {
			throw new IllegalArgumentException(
					"firstFailedLineNumber must be present exactly when failedOperations are present");
		}
		if (firstFailedLineNumber != null && firstFailedLineNumber <= 0) {
			throw new IllegalArgumentException("firstFailedLineNumber must be positive");
		}
	}

	/** Возвращает начальный пустой progress. */
	public static ArchiveProcessingProgress empty() {
		return new ArchiveProcessingProgress(0, 0, 0, 0, 0, 0, 0, null);
	}

	/**
	 * Проверяет полноту успешного terminal processing outcome.
	 */
	public void requireCompleted() {
		long mappedRecords = deliveredRecords - mappingRejectedRecords;
		if (failedOperations != 0
				|| submittedOperations != mappedRecords
				|| succeededOperations != submittedOperations
				|| receiptDocuments != succeededOperations) {
			throw new IllegalArgumentException(
					"COMPLETED progress must confirm every mapped operation and receipt");
		}
	}

	private static void requireNonNegative(long value, String fieldName) {
		if (value < 0) {
			throw new IllegalArgumentException(fieldName + " must not be negative");
		}
	}
}
