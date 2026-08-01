package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;

/** Fail-closed отказ inspect или execute partition rebuild. */
public class PartitionRebuildException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	private final PartitionRebuildErrorCode errorCode;

	/** Создает отказ без раскрытия внешнего exception message. */
	public PartitionRebuildException(PartitionRebuildErrorCode errorCode) {
		super(errorCode.name());
		this.errorCode = Objects.requireNonNull(errorCode, "errorCode must not be null");
	}

	/** Создает отказ с исходной диагностикой в cause. */
	public PartitionRebuildException(
			PartitionRebuildErrorCode errorCode,
			Throwable cause
	) {
		super(errorCode.name(), cause);
		this.errorCode = Objects.requireNonNull(errorCode, "errorCode must not be null");
	}

	/** Возвращает bounded code для CLI и durable failure. */
	public PartitionRebuildErrorCode errorCode() {
		return errorCode;
	}

	/** Возвращает признак безопасного retry. */
	public boolean retryable() {
		return errorCode.retryable();
	}
}
