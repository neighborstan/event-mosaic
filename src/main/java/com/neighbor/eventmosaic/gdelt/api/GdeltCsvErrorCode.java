package com.neighbor.eventmosaic.gdelt.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Стабильный каталог file-level ошибок GDELT CSV.
 */
public enum GdeltCsvErrorCode implements ApplicationErrorCode {

	CSV_SCHEMA_MISMATCH("GDELT CSV content does not match the expected schema"),
	CSV_ENCODING_INVALID("GDELT CSV contains invalid UTF-8"),
	CSV_RECORD_LIMIT_EXCEEDED("GDELT CSV record exceeds the configured size limit"),
	CSV_FILESYSTEM_IO_FAILURE("Local GDELT CSV filesystem operation failed"),
	CSV_OPERATION_INTERRUPTED("GDELT CSV operation was interrupted");

	private final String safeMessage;

	GdeltCsvErrorCode(String safeMessage) {
		this.safeMessage = safeMessage;
	}

	@Override
	public String code() {
		return name();
	}

	@Override
	public String safeMessage() {
		return safeMessage;
	}
}
