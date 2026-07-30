package com.neighbor.eventmosaic.gdelt.api;

import java.util.Map;
import java.util.Objects;

/**
 * Неизменяемый итог успешного чтения одного GDELT CSV.
 *
 * @param kind тип прочитанного GDELT archive
 * @param physicalRecords число физических records
 * @param validRecords число валидных records
 * @param invalidRecords число локально отклоненных records
 * @param rejectionCounts число отклонений по каждой встреченной причине
 * @param firstRejectedLineNumbers первая 1-based строка для каждой причины
 */
public record GdeltCsvReadSummary(
		GdeltArchiveKind kind,
		long physicalRecords,
		long validRecords,
		long invalidRecords,
		Map<GdeltCsvRecordErrorCode, Long> rejectionCounts,
		Map<GdeltCsvRecordErrorCode, Long> firstRejectedLineNumbers
) {

	/**
	 * Проверяет арифметические invariants и создает immutable copies diagnostics.
	 */
	public GdeltCsvReadSummary {
		Objects.requireNonNull(kind, "kind must not be null");
		requireNonNegative(physicalRecords, "physicalRecords");
		requireNonNegative(validRecords, "validRecords");
		requireNonNegative(invalidRecords, "invalidRecords");
		rejectionCounts = Map.copyOf(rejectionCounts);
		firstRejectedLineNumbers = Map.copyOf(firstRejectedLineNumbers);

		if (validRecords > Long.MAX_VALUE - invalidRecords
				|| physicalRecords != validRecords + invalidRecords) {
			throw new IllegalArgumentException(
					"physicalRecords must equal validRecords plus invalidRecords");
		}
		if (!rejectionCounts.keySet().equals(firstRejectedLineNumbers.keySet())) {
			throw new IllegalArgumentException(
					"rejection counts and first line numbers must have equal reasons");
		}

		long rejectionTotal = 0;
		for (Map.Entry<GdeltCsvRecordErrorCode, Long> entry : rejectionCounts.entrySet()) {
			long count = entry.getValue();
			if (count <= 0) {
				throw new IllegalArgumentException("rejection count must be positive");
			}
			if (rejectionTotal > Long.MAX_VALUE - count) {
				throw new IllegalArgumentException("rejection count total exceeds long range");
			}
			rejectionTotal += count;
		}
		if (rejectionTotal != invalidRecords) {
			throw new IllegalArgumentException(
					"rejection count total must equal invalidRecords");
		}

		for (long lineNumber : firstRejectedLineNumbers.values()) {
			if (lineNumber <= 0 || lineNumber > physicalRecords) {
				throw new IllegalArgumentException(
						"first rejected line number must reference a physical record");
			}
		}
	}

	private static void requireNonNegative(long value, String name) {
		if (value < 0) {
			throw new IllegalArgumentException(name + " must not be negative");
		}
	}
}
