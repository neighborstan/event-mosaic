package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.RetryableException;

/**
 * Объединяет устранимые повторным получением нарушения целостности archive.
 */
public final class TransferredArtifactIntegrityException extends RetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;

	/** Создает retryable нарушение целостности с точным error code. */
	public TransferredArtifactIntegrityException(IngestionErrorCode code) {
		super(code);
		this.failure = new IngestionFailure(code, true);
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}
}
