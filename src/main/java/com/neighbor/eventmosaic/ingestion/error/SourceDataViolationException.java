package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Объединяет нарушения структуры и согласованности входных source data.
 */
public final class SourceDataViolationException extends NonRetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;
	private final IngestionErrorContext context;

	/** Создает нарушение source data без дополнительного контекста. */
	public SourceDataViolationException(IngestionErrorCode code) {
		this(code, IngestionErrorContext.empty());
	}

	/** Создает нарушение source data с безопасным контекстом. */
	public SourceDataViolationException(
			IngestionErrorCode code,
			IngestionErrorContext context
	) {
		super(code);
		this.failure = new IngestionFailure(code, false);
		this.context = Objects.requireNonNull(context, "context must not be null");
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}

	@Override
	public IngestionErrorContext context() {
		return context;
	}
}
