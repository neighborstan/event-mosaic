package com.neighbor.eventmosaic.search;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.time.Duration;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Настройки одного серверного снимка по странам. Они выбирают точную версию
 * опубликованной геометрии и задают небольшой запас времени, после которого
 * закрытый 15-минутный интервал можно включать в снимок.
 *
 * @param geometryVersion точная версия опубликованной геометрии стран
 * @param ingestionGrace запас времени после границы исходного интервала
 */
@Validated
@ConfigurationProperties("event-mosaic.map.country-snapshot")
public record CountryMapSnapshotProperties(
		@DefaultValue("country-v1")
		@NotBlank
		@Pattern(regexp = "country-v[1-9][0-9]*")
		String geometryVersion,
		@DefaultValue("15m") @NotNull Duration ingestionGrace
) {

	private static final java.util.regex.Pattern GEOMETRY_VERSION_PATTERN =
			java.util.regex.Pattern.compile("country-v[1-9][0-9]*");
	private static final Duration SOURCE_CADENCE = Duration.ofMinutes(15);
	private static final Duration MAXIMUM_INGESTION_GRACE = Duration.ofHours(1);

	/** Проверяет версию и ограниченный запас также при создании без Spring binding. */
	public CountryMapSnapshotProperties {
		Objects.requireNonNull(geometryVersion, "geometryVersion must not be null");
		Objects.requireNonNull(ingestionGrace, "ingestionGrace must not be null");
		if (!GEOMETRY_VERSION_PATTERN.matcher(geometryVersion).matches()) {
			throw new IllegalArgumentException(
					"geometryVersion must match country-vN with N greater than zero");
		}
		if (ingestionGrace.isNegative()) {
			throw new IllegalArgumentException("ingestionGrace must not be negative");
		}
		if (ingestionGrace.getNano() != 0) {
			throw new IllegalArgumentException("ingestionGrace must use whole seconds");
		}
		if (ingestionGrace.getSeconds() % SOURCE_CADENCE.getSeconds() != 0) {
			throw new IllegalArgumentException(
					"ingestionGrace must be a multiple of 15 minutes");
		}
		if (ingestionGrace.compareTo(MAXIMUM_INGESTION_GRACE) > 0) {
			throw new IllegalArgumentException("ingestionGrace must not exceed one hour");
		}
	}
}
