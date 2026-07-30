package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import java.util.Objects;

/**
 * Результат mapping одной shape-compatible строки: raw value или одна
 * bounded-причина локального отклонения.
 */
record GdeltCsvMappingResult<T>(T value, GdeltCsvRecordErrorCode rejectionReason) {

	GdeltCsvMappingResult {
		if ((value == null) == (rejectionReason == null)) {
			throw new IllegalArgumentException("Exactly one mapping result branch must be present");
		}
	}

	static <T> GdeltCsvMappingResult<T> accepted(T value) {
		return new GdeltCsvMappingResult<>(Objects.requireNonNull(value, "value must not be null"), null);
	}

	static <T> GdeltCsvMappingResult<T> rejected(GdeltCsvRecordErrorCode reason) {
		return new GdeltCsvMappingResult<>(null, Objects.requireNonNull(reason, "reason must not be null"));
	}

	boolean accepted() {
		return value != null;
	}
}
