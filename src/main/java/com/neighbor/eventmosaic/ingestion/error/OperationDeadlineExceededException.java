package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.RetryableException;

/**
 * Останавливает cycle после исчерпания monotonic budget, не меняя interrupt flag.
 */
public final class OperationDeadlineExceededException extends RetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure = new IngestionFailure(
			IngestionErrorCode.OPERATION_DEADLINE_EXCEEDED,
			true);

	/** Создает typed deadline outcome текущей operation. */
	public OperationDeadlineExceededException() {
		super(IngestionErrorCode.OPERATION_DEADLINE_EXCEEDED);
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}

	@Override
	public boolean abortsCycle() {
		return true;
	}
}
