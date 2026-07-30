package com.neighbor.eventmosaic.gdelt.csv;

import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Типизированное ограничение памяти одной физической GDELT CSV record.
 *
 * @param maxRecordChars максимальное число символов одной physical record
 */
@Validated
@ConfigurationProperties("event-mosaic.gdelt.csv")
public record GdeltCsvProperties(
		@DefaultValue("1048576") @Positive int maxRecordChars
) {

	/** Проверяет положительный limit при прямом создании вне Spring binding. */
	public GdeltCsvProperties {
		if (maxRecordChars <= 0) {
			throw new IllegalArgumentException("maxRecordChars must be positive");
		}
	}
}
