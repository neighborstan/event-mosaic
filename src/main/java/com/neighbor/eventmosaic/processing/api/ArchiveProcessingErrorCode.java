package com.neighbor.eventmosaic.processing.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Стабильные безопасные коды ожидаемых processing failures.
 */
public enum ArchiveProcessingErrorCode implements ApplicationErrorCode {

	CSV_SOURCE_SCHEMA_FAILURE("GDELT CSV source does not satisfy the processing schema"),
	CSV_SOURCE_ACCESS_FAILURE("GDELT CSV source cannot be read"),
	CSV_SOURCE_INTERRUPTED("GDELT CSV processing was interrupted"),
	BULK_PARTIAL_FAILURE("Elasticsearch rejected part of a processing batch"),
	INDEXING_OPERATION_FAILURE("Elasticsearch indexing operation failed"),
	INDEXING_INTERRUPTED("Elasticsearch indexing operation was interrupted"),
	INDEX_RECEIPT_MISMATCH("Elasticsearch archive receipt has an identity mismatch"),
	INDEX_RECEIPT_SURPLUS("Elasticsearch archive receipt contains surplus documents"),
	EVENT_IDENTITY_CONFLICT("Elasticsearch Event identity conflicts with stored provenance"),
	OPERATION_DEADLINE_EXCEEDED("GDELT archive processing exceeded the shared deadline");

	private final String safeMessage;

	ArchiveProcessingErrorCode(String safeMessage) {
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
