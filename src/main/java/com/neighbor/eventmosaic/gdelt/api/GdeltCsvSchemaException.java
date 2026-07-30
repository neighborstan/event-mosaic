package com.neighbor.eventmosaic.gdelt.api;

import com.neighbor.eventmosaic.shared.error.NonRetryableException;
import java.util.Objects;

/**
 * Объединяет неповторяемые нарушения schema, encoding и record limits GDELT CSV.
 *
 * <p>Concrete тип намеренно сохраняет семантику CSV поверх общей non-retryable
 * ветки application errors.</p>
 */
@SuppressWarnings("java:S110")
public final class GdeltCsvSchemaException extends NonRetryableException {

	/**
	 * Создает file-level source violation с catalog code.
	 *
	 * @param code стабильный код ошибки
	 */
	public GdeltCsvSchemaException(GdeltCsvErrorCode code) {
		super(code);
	}

	/**
	 * Создает file-level source violation с исходной технической причиной.
	 *
	 * @param code стабильный код ошибки
	 * @param cause исходная причина
	 */
	public GdeltCsvSchemaException(GdeltCsvErrorCode code, Throwable cause) {
		super(code, Objects.requireNonNull(cause, "cause must not be null"));
	}
}
