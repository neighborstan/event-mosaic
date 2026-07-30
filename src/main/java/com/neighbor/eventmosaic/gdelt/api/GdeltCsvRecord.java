package com.neighbor.eventmosaic.gdelt.api;

import java.util.Objects;

/**
 * Raw GDELT value с номером исходной физической строки.
 *
 * @param lineNumber номер строки, начиная с единицы
 * @param value разобранная raw-запись
 * @param <T> тип Event или Mention
 */
public record GdeltCsvRecord<T>(long lineNumber, T value) {

	/** Проверяет положительный номер строки и наличие значения. */
	public GdeltCsvRecord {
		if (lineNumber <= 0) {
			throw new IllegalArgumentException("lineNumber must be positive");
		}
		Objects.requireNonNull(value, "value must not be null");
	}
}
