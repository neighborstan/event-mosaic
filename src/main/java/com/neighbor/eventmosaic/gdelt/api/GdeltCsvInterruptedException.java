package com.neighbor.eventmosaic.gdelt.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.util.Objects;

/**
 * Сигнализирует о cooperative interruption текущего GDELT CSV чтения.
 *
 * <p>Concrete тип намеренно сохраняет отдельное interruption behavior поверх
 * общей retryable ветки application errors.</p>
 */
@SuppressWarnings("java:S110")
public final class GdeltCsvInterruptedException extends RetryableException {

	/** Создает отказ, обнаруженный по установленному interrupt flag. */
	public GdeltCsvInterruptedException() {
		super(GdeltCsvErrorCode.CSV_OPERATION_INTERRUPTED);
	}

	/**
	 * Создает interruption failure с исходной причиной.
	 *
	 * @param cause исходная interruption-related причина
	 */
	public GdeltCsvInterruptedException(Throwable cause) {
		super(
				GdeltCsvErrorCode.CSV_OPERATION_INTERRUPTED,
				Objects.requireNonNull(cause, "cause must not be null"));
	}
}
