package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import java.util.EnumMap;
import java.util.Map;

/**
 * Накапливает только bounded counters и первый line number каждой причины.
 */
final class GdeltCsvCounters {

	private final EnumMap<GdeltCsvRecordErrorCode, Long> rejectionCounts =
			new EnumMap<>(GdeltCsvRecordErrorCode.class);
	private final EnumMap<GdeltCsvRecordErrorCode, Long> firstRejectedLineNumbers =
			new EnumMap<>(GdeltCsvRecordErrorCode.class);

	private long physicalRecords;
	private long shapeCompatibleRecords;
	private long validRecords;
	private long invalidRecords;

	void shapeCompatible() {
		shapeCompatibleRecords++;
	}

	void accepted() {
		physicalRecords++;
		validRecords++;
	}

	void rejected(long lineNumber, GdeltCsvRecordErrorCode reason) {
		physicalRecords++;
		invalidRecords++;
		rejectionCounts.merge(reason, 1L, Long::sum);
		firstRejectedLineNumbers.putIfAbsent(reason, lineNumber);
	}

	boolean hasShapeCompatibleRecords() {
		return shapeCompatibleRecords > 0;
	}

	long validRecords() {
		return validRecords;
	}

	long invalidRecords() {
		return invalidRecords;
	}

	Map<GdeltCsvRecordErrorCode, Long> rejectionCounts() {
		return Map.copyOf(rejectionCounts);
	}

	GdeltCsvReadSummary toSummary(GdeltArchiveKind kind) {
		return new GdeltCsvReadSummary(
				kind,
				physicalRecords,
				validRecords,
				invalidRecords,
				rejectionCounts,
				firstRejectedLineNumbers);
	}
}
