package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Объединяет нарушения контрактов локального staging storage.
 */
public final class StagingStorageException extends NonRetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;

	/** Создает нарушение staging storage с точным error code. */
	public StagingStorageException(IngestionErrorCode code) {
		super(code);
		this.failure = new IngestionFailure(code, false);
	}

	/** Создает нарушение staging storage с исходной причиной. */
	public StagingStorageException(
			IngestionErrorCode code,
			Throwable cause
	) {
		super(
				code,
				Objects.requireNonNull(cause, "cause must not be null"));
		this.failure = new IngestionFailure(code, false);
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}
}
