package com.neighbor.eventmosaic.ingestion.api;

/**
 * Абсолютные counters текущей processing attempt.
 *
 * @param deliveredRecords records, переданные source reader downstream consumer
 * @param sourceInvalidRecords records, локально отклоненные source reader
 * @param mappingRejectedRecords доставленные records, отклоненные mapping
 * @param submittedOperations отправленные Elasticsearch index operations
 * @param succeededOperations подтвержденные успешные operations
 * @param failedOperations подтвержденные failed operations
 * @param receiptDocuments документы exact archive, подтвержденные receipt query
 * @param firstFailedLine первая physical line failed bulk item или {@code null}
 */
public record ArchiveProcessingProgress(
		long deliveredRecords,
		long sourceInvalidRecords,
		long mappingRejectedRecords,
		long submittedOperations,
		long succeededOperations,
		long failedOperations,
		long receiptDocuments,
		Long firstFailedLine
) {

	/**
	 * Проверяет неотрицательность и арифметические invariants counters.
	 */
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
		if ((failedOperations == 0) != (firstFailedLine == null)) {
			throw new IllegalArgumentException(
					"firstFailedLine must be present exactly when failedOperations are present");
		}
		if (firstFailedLine != null && firstFailedLine <= 0) {
			throw new IllegalArgumentException("firstFailedLine must be positive");
		}
	}

	/**
	 * Возвращает начальный progress новой попытки.
	 *
	 * @return нулевые counters
	 */
	public static ArchiveProcessingProgress empty() {
		return new ArchiveProcessingProgress(0, 0, 0, 0, 0, 0, 0, null);
	}

	/**
	 * Проверяет полноту успешного terminal outcome перед `INDEXED`.
	 */
	public void requireIndexedCompletion() {
		long mappedRecords = deliveredRecords - mappingRejectedRecords;
		if (failedOperations != 0
				|| submittedOperations != mappedRecords
				|| succeededOperations != submittedOperations
				|| receiptDocuments != succeededOperations) {
			throw new IllegalArgumentException(
					"INDEXED progress must confirm every mapped operation and receipt");
		}
	}

	private static void requireNonNegative(long value, String name) {
		if (value < 0) {
			throw new IllegalArgumentException(name + " must not be negative");
		}
	}
}
