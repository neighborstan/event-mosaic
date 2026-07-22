package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Объединяет нарушения безопасности и структуры содержимого ZIP archive.
 */
public final class ArchiveContentViolationException extends NonRetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;

	/** Создает нарушение содержимого archive с точным error code. */
	public ArchiveContentViolationException(IngestionErrorCode code) {
		super(code);
		this.failure = new IngestionFailure(code, false);
	}

	/** Создает нарушение содержимого archive с исходной причиной. */
	public ArchiveContentViolationException(
			IngestionErrorCode code,
			Throwable cause
	) {
		super(
				code,
				Objects.requireNonNull(cause, "cause must not be null"));
		this.failure = new IngestionFailure(code, false);
	}

	@Override
	public final IngestionFailure failure() {
		return failure;
	}
}
