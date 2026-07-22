package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.util.Objects;

/**
 * Сигнализирует о прерывании ingestion operation и останавливает текущий cycle.
 */
public final class IngestionInterruptedException extends RetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure = new IngestionFailure(
			IngestionErrorCode.OPERATION_INTERRUPTED,
			true);

	/**
	 * Создает ошибку прерывания, обнаруженного по interrupt flag в cooperative checkpoint.
	 */
	public IngestionInterruptedException() {
		super(IngestionErrorCode.OPERATION_INTERRUPTED);
	}

	/**
	 * Создает ошибку прерывания текущей operation.
	 *
	 * @param cause исходная interruption-related причина
	 */
	public IngestionInterruptedException(Throwable cause) {
		super(IngestionErrorCode.OPERATION_INTERRUPTED, Objects.requireNonNull(cause, "cause must not be null"));
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
