package com.neighbor.eventmosaic.gdelt.api;

import com.neighbor.eventmosaic.shared.error.RetryableException;
import java.util.Objects;

/**
 * Объединяет повторяемые filesystem failures при чтении GDELT CSV.
 *
 * <p>Concrete тип намеренно сохраняет семантику CSV поверх общей retryable
 * ветки application errors.</p>
 */
@SuppressWarnings("java:S110")
public final class GdeltCsvAccessException extends RetryableException {

	/**
	 * Создает временный отказ filesystem с catalog code.
	 *
	 * @param code стабильный код ошибки
	 */
	public GdeltCsvAccessException(GdeltCsvErrorCode code) {
		super(code);
	}

	/**
	 * Создает временный отказ filesystem с исходной причиной.
	 *
	 * @param code стабильный код ошибки
	 * @param cause исходная причина
	 */
	public GdeltCsvAccessException(GdeltCsvErrorCode code, Throwable cause) {
		super(code, Objects.requireNonNull(cause, "cause must not be null"));
	}
}
