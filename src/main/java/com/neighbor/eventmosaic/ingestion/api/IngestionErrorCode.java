package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Стабильный каталог ошибок acquisition и continuity модуля ingestion.
 */
public enum IngestionErrorCode implements ApplicationErrorCode {

	MANIFEST_HTTP_ERROR("Manifest request failed"),
	MANIFEST_TIMEOUT("Manifest request exceeded the configured deadline"),
	MANIFEST_HTTP_STATUS_REJECTED("Manifest request returned a non-retryable HTTP status"),
	MANIFEST_SIZE_LIMIT_EXCEEDED("Manifest exceeds the configured size limit"),
	MANIFEST_MALFORMED_LINE("Manifest contains a malformed line"),
	MANIFEST_SOURCE_URI_REJECTED("Manifest source URI is outside the allowlist"),
	MANIFEST_REQUIRED_ARCHIVE_MISSING("Manifest does not contain the required archive pair"),
	MANIFEST_DUPLICATE_ARCHIVE("Manifest contains a duplicate supported archive"),
	MANIFEST_TIMESTAMP_MISMATCH("Manifest archive timestamps do not match"),
	ARCHIVE_METADATA_CONFLICT("Archive metadata conflicts with the registered update"),
	DOWNLOAD_HTTP_ERROR("Archive request failed"),
	DOWNLOAD_TIMEOUT("Archive request exceeded the configured deadline"),
	DOWNLOAD_HTTP_STATUS_REJECTED("Archive request returned a non-retryable HTTP status"),
	DOWNLOAD_SIZE_LIMIT_EXCEEDED("Archive exceeds the configured size limit"),
	DOWNLOAD_SIZE_MISMATCH("Archive size does not match manifest metadata"),
	DOWNLOAD_MD5_MISMATCH("Archive checksum does not match manifest metadata"),
	ZIP_PATH_REJECTED("ZIP entry path is unsafe"),
	ZIP_LIMIT_EXCEEDED("ZIP extraction exceeds the configured limit"),
	ZIP_CONTENT_MISMATCH("ZIP content does not match the expected archive"),
	STAGING_ARTIFACT_CONFLICT("Existing staging artifact conflicts with expected metadata"),
	STAGING_PATH_REJECTED("Staging path contains an unsafe filesystem indirection"),
	STAGING_ATOMIC_PUBLICATION_UNSUPPORTED("Filesystem does not support required atomic publication"),
	FILESYSTEM_IO_FAILURE("Local ingestion filesystem operation failed"),
	OPERATION_DEADLINE_EXCEEDED("Ingestion operation exceeded the shared deadline"),
	OPERATION_INTERRUPTED("Ingestion operation was interrupted"),
	INTERNAL_ERROR("Unexpected internal ingestion error");

	private final String safeMessage;

	IngestionErrorCode(String safeMessage) {
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
