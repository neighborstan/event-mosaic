package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import java.util.Objects;

/**
 * Внутренний легковесный сигнал локального отклонения одной CSV record.
 */
final class GdeltCsvValueException extends RuntimeException {

	private final GdeltCsvRecordErrorCode reason;

	GdeltCsvValueException(GdeltCsvRecordErrorCode reason) {
		super(null, null, false, false);
		this.reason = Objects.requireNonNull(reason, "reason must not be null");
	}

	GdeltCsvRecordErrorCode reason() {
		return reason;
	}
}
