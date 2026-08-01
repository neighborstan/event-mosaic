package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.time.Duration;
import java.util.Objects;

/**
 * Объединяет временные отказы доступа к удаленным GDELT-ресурсам.
 */
public final class RemoteSourceAccessException extends RetryableException
		implements IngestionFailureContract {

	private final IngestionFailure failure;
	private final IngestionErrorContext context;
	private final Duration retryAfter;

	/** Создает временный отказ удаленного источника без дополнительного контекста. */
	public RemoteSourceAccessException(IngestionErrorCode code) {
		this(code, IngestionErrorContext.empty(), Duration.ZERO, null);
	}

	/** Создает временный отказ удаленного источника с исходной причиной. */
	public RemoteSourceAccessException(
			IngestionErrorCode code,
			Throwable cause
	) {
		this(
				code,
				IngestionErrorContext.empty(),
				Duration.ZERO,
				Objects.requireNonNull(cause, "cause must not be null"));
	}

	/** Создает временный отказ удаленного источника с безопасным контекстом. */
	public RemoteSourceAccessException(
			IngestionErrorCode code,
			IngestionErrorContext context
	) {
		this(code, context, Duration.ZERO, null);
	}

	/** Создает временный HTTP-отказ с безопасным bounded Retry-After hint. */
	public RemoteSourceAccessException(
			IngestionErrorCode code,
			IngestionErrorContext context,
			Duration retryAfter
	) {
		this(code, context, retryAfter, null);
	}

	private RemoteSourceAccessException(
			IngestionErrorCode code,
			IngestionErrorContext context,
			Duration retryAfter,
			Throwable cause
	) {
		super(code, cause);
		this.failure = new IngestionFailure(code, true);
		this.context = Objects.requireNonNull(context, "context must not be null");
		this.retryAfter = Objects.requireNonNull(retryAfter, "retryAfter must not be null");
		if (retryAfter.isNegative()) {
			throw new IllegalArgumentException("retryAfter must not be negative");
		}
	}

	@Override
	public IngestionFailure failure() {
		return failure;
	}

	@Override
	public IngestionErrorContext context() {
		return context;
	}

	@Override
	public Duration retryAfter() {
		return retryAfter;
	}
}
