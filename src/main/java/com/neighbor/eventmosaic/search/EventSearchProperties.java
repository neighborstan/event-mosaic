package com.neighbor.eventmosaic.search;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Ограничения минимального Event details query.
 *
 * @param maxSourcesPerEvent максимальное число уникальных sources в ответе
 */
@Validated
@ConfigurationProperties("event-mosaic.search")
public record EventSearchProperties(
		@DefaultValue("20") @Min(1) @Max(100) int maxSourcesPerEvent
) {

	/** Проверяет limit при прямом создании вне Spring binding. */
	public EventSearchProperties {
		if (maxSourcesPerEvent < 1 || maxSourcesPerEvent > 100) {
			throw new IllegalArgumentException(
					"maxSourcesPerEvent must be between 1 and 100");
		}
	}
}
