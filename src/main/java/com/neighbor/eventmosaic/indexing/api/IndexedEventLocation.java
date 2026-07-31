package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Единственная выбранная географическая точка индексируемого события.
 *
 * @param role роль точки в исходной строке
 * @param geoType тип географического совпадения GDELT или {@code null}
 * @param name отображаемое имя
 * @param countryCode код страны
 * @param admin1Code код административного региона первого уровня
 * @param admin2Code код административного региона второго уровня
 * @param featureId идентификатор географического объекта
 * @param point географическая точка Elasticsearch
 */
public record IndexedEventLocation(
		IndexedLocationRole role,
		Integer geoType,
		String name,
		String countryCode,
		String admin1Code,
		String admin2Code,
		String featureId,
		IndexedGeoPoint point
) {

	/**
	 * Проверяет обязательные поля и диапазон координат.
	 */
	public IndexedEventLocation {
		Objects.requireNonNull(role, "role must not be null");
		Objects.requireNonNull(name, "name must not be null");
		Objects.requireNonNull(countryCode, "countryCode must not be null");
		Objects.requireNonNull(admin1Code, "admin1Code must not be null");
		Objects.requireNonNull(admin2Code, "admin2Code must not be null");
		Objects.requireNonNull(featureId, "featureId must not be null");
		Objects.requireNonNull(point, "point must not be null");
	}

}
