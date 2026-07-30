package com.neighbor.eventmosaic.indexing.api;

/**
 * Координаты в формате Elasticsearch {@code geo_point}.
 *
 * @param lat широта
 * @param lon долгота
 */
public record IndexedGeoPoint(double lat, double lon) {

	/**
	 * Проверяет конечность и допустимый диапазон координат.
	 */
	public IndexedGeoPoint {
		if (!Double.isFinite(lat) || lat < -90.0 || lat > 90.0) {
			throw new IllegalArgumentException("lat must be finite and within [-90, 90]");
		}
		if (!Double.isFinite(lon) || lon < -180.0 || lon > 180.0) {
			throw new IllegalArgumentException("lon must be finite and within [-180, 180]");
		}
	}

}
