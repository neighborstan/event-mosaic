package com.neighbor.eventmosaic.search;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Неизменяемый каталог связи опубликованных регионов с точными кодами GDELT.
 * Порядок регионов берется из проверенного manifest и затем используется как
 * единственный канонический порядок плотного ответа карты.
 */
final class CountryGeometryCatalog {

	private final String geometryVersion;
	private final List<Region> regions;
	private final Map<String, String> regionIdByGdeltCountryCode;
	private final Map<String, UnmappedReason> unmappedReasonByGdeltCountryCode;
	private final Set<String> knownGdeltCountryCodes;

	CountryGeometryCatalog(
			String geometryVersion,
			List<Region> regions,
			Map<String, String> regionIdByGdeltCountryCode,
			Map<String, UnmappedReason> unmappedReasonByGdeltCountryCode
	) {
		this.geometryVersion = Objects.requireNonNull(
				geometryVersion, "geometryVersion must not be null");
		this.regions = List.copyOf(Objects.requireNonNull(
				regions, "regions must not be null"));
		this.regionIdByGdeltCountryCode = Map.copyOf(Objects.requireNonNull(
				regionIdByGdeltCountryCode,
				"regionIdByGdeltCountryCode must not be null"));
		this.unmappedReasonByGdeltCountryCode = Map.copyOf(Objects.requireNonNull(
				unmappedReasonByGdeltCountryCode,
				"unmappedReasonByGdeltCountryCode must not be null"));

		var knownCodes = new HashSet<>(this.regionIdByGdeltCountryCode.keySet());
		knownCodes.addAll(this.unmappedReasonByGdeltCountryCode.keySet());
		this.knownGdeltCountryCodes = Set.copyOf(knownCodes);
	}

	String geometryVersion() {
		return geometryVersion;
	}

	List<Region> regions() {
		return regions;
	}

	Map<String, String> regionIdByGdeltCountryCode() {
		return regionIdByGdeltCountryCode;
	}

	Map<String, UnmappedReason> unmappedReasonByGdeltCountryCode() {
		return unmappedReasonByGdeltCountryCode;
	}

	Set<String> knownGdeltCountryCodes() {
		return knownGdeltCountryCodes;
	}

	/**
	 * Одна строка канонического каталога. Пустой список кодов допустим: такой
	 * регион все равно должен появляться в плотном снимке с нулевыми счетчиками.
	 *
	 * @param regionId стабильный идентификатор региона
	 * @param gdeltCountryCodes отсортированные точные коды GDELT этого региона
	 */
	record Region(String regionId, List<String> gdeltCountryCodes) {

		Region {
			Objects.requireNonNull(regionId, "regionId must not be null");
			gdeltCountryCodes = List.copyOf(Objects.requireNonNull(
					gdeltCountryCodes, "gdeltCountryCodes must not be null"));
		}
	}

	/** Ограниченные причины, с которыми manifest явно оставляет код без региона. */
	enum UnmappedReason {
		NO_TERRITORIAL_GEOMETRY,
		SOURCE_GEOMETRY_NOT_INDEPENDENT,
		AMBIGUOUS_MULTIPLE_REGIONS,
		UNSUPPORTED_NON_COUNTRY_CODE
	}
}
