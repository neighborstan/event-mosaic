package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Объединяет ответы удаленного источника, отклоненные локальной политикой.
 */
public final class RemoteResponseRejectedException extends NonRetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;
	private final IngestionErrorContext context;

	/** Создает отклоненный remote response без дополнительного контекста. */
	public RemoteResponseRejectedException(IngestionErrorCode code) {
		this(code, IngestionErrorContext.empty());
	}

	/** Создает отклоненный remote response с безопасным контекстом. */
	public RemoteResponseRejectedException(
			IngestionErrorCode code,
			IngestionErrorContext context
	) {
		super(code);
		this.failure = new IngestionFailure(code, false);
		this.context = Objects.requireNonNull(context, "context must not be null");
	}

	@Override
	public final IngestionFailure failure() {
		return failure;
	}

	@Override
	public final IngestionErrorContext context() {
		return context;
	}
}
