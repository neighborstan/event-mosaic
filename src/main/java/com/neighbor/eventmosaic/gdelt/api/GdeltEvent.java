package com.neighbor.eventmosaic.gdelt.api;

import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.Objects;

/**
 * Неизменяемая raw-запись GDELT Event из 61 поля в точном provider order.
 *
 * <p>Строковые значения сохраняются verbatim и не могут быть {@code null}.
 * Пустые необязательные typed values представлены как {@code null}.</p>
 *
 * @param globalEventId глобальный идентификатор события
 * @param day календарный день события
 * @param monthYear месяц события
 * @param year год события
 * @param fractionDate дробное представление даты
 * @param actor1Code полный CAMEO-код первого участника
 * @param actor1Name имя первого участника
 * @param actor1CountryCode код страны первого участника
 * @param actor1KnownGroupCode код известной группы первого участника
 * @param actor1EthnicCode этнический код первого участника
 * @param actor1Religion1Code первый религиозный код первого участника
 * @param actor1Religion2Code второй религиозный код первого участника
 * @param actor1Type1Code первый код типа первого участника
 * @param actor1Type2Code второй код типа первого участника
 * @param actor1Type3Code третий код типа первого участника
 * @param actor2Code полный CAMEO-код второго участника
 * @param actor2Name имя второго участника
 * @param actor2CountryCode код страны второго участника
 * @param actor2KnownGroupCode код известной группы второго участника
 * @param actor2EthnicCode этнический код второго участника
 * @param actor2Religion1Code первый религиозный код второго участника
 * @param actor2Religion2Code второй религиозный код второго участника
 * @param actor2Type1Code первый код типа второго участника
 * @param actor2Type2Code второй код типа второго участника
 * @param actor2Type3Code третий код типа второго участника
 * @param isRootEvent признак корневого события
 * @param eventCode полный CAMEO-код действия
 * @param eventBaseCode базовый CAMEO-код действия
 * @param eventRootCode корневой CAMEO-код действия
 * @param quadClass класс CAMEO-квадранта
 * @param goldsteinScale оценка по шкале Гольдштейна
 * @param numMentions число упоминаний
 * @param numSources число источников
 * @param numArticles число документов
 * @param avgTone средний тон документов
 * @param actor1GeoType тип географического совпадения первого участника
 * @param actor1GeoFullName полное имя географии первого участника
 * @param actor1GeoCountryCode код страны географии первого участника
 * @param actor1GeoAdm1Code код ADM1 географии первого участника
 * @param actor1GeoAdm2Code код ADM2 географии первого участника
 * @param actor1GeoLat широта географии первого участника
 * @param actor1GeoLong долгота географии первого участника
 * @param actor1GeoFeatureId feature id географии первого участника
 * @param actor2GeoType тип географического совпадения второго участника
 * @param actor2GeoFullName полное имя географии второго участника
 * @param actor2GeoCountryCode код страны географии второго участника
 * @param actor2GeoAdm1Code код ADM1 географии второго участника
 * @param actor2GeoAdm2Code код ADM2 географии второго участника
 * @param actor2GeoLat широта географии второго участника
 * @param actor2GeoLong долгота географии второго участника
 * @param actor2GeoFeatureId feature id географии второго участника
 * @param actionGeoType тип географического совпадения действия
 * @param actionGeoFullName полное имя географии действия
 * @param actionGeoCountryCode код страны географии действия
 * @param actionGeoAdm1Code код ADM1 географии действия
 * @param actionGeoAdm2Code код ADM2 географии действия
 * @param actionGeoLat широта географии действия
 * @param actionGeoLong долгота географии действия
 * @param actionGeoFeatureId feature id географии действия
 * @param dateAdded UTC timestamp добавления события
 * @param sourceUrl URL или citation исходного документа
 */
public record GdeltEvent(
		long globalEventId,
		LocalDate day,
		YearMonth monthYear,
		Integer year,
		Double fractionDate,
		String actor1Code,
		String actor1Name,
		String actor1CountryCode,
		String actor1KnownGroupCode,
		String actor1EthnicCode,
		String actor1Religion1Code,
		String actor1Religion2Code,
		String actor1Type1Code,
		String actor1Type2Code,
		String actor1Type3Code,
		String actor2Code,
		String actor2Name,
		String actor2CountryCode,
		String actor2KnownGroupCode,
		String actor2EthnicCode,
		String actor2Religion1Code,
		String actor2Religion2Code,
		String actor2Type1Code,
		String actor2Type2Code,
		String actor2Type3Code,
		Integer isRootEvent,
		String eventCode,
		String eventBaseCode,
		String eventRootCode,
		Integer quadClass,
		Double goldsteinScale,
		Integer numMentions,
		Integer numSources,
		Integer numArticles,
		Double avgTone,
		Integer actor1GeoType,
		String actor1GeoFullName,
		String actor1GeoCountryCode,
		String actor1GeoAdm1Code,
		String actor1GeoAdm2Code,
		Double actor1GeoLat,
		Double actor1GeoLong,
		String actor1GeoFeatureId,
		Integer actor2GeoType,
		String actor2GeoFullName,
		String actor2GeoCountryCode,
		String actor2GeoAdm1Code,
		String actor2GeoAdm2Code,
		Double actor2GeoLat,
		Double actor2GeoLong,
		String actor2GeoFeatureId,
		Integer actionGeoType,
		String actionGeoFullName,
		String actionGeoCountryCode,
		String actionGeoAdm1Code,
		String actionGeoAdm2Code,
		Double actionGeoLat,
		Double actionGeoLong,
		String actionGeoFeatureId,
		Instant dateAdded,
		String sourceUrl
) {

	/** Проверяет verbatim-строки и конечность присутствующих decimal values. */
	public GdeltEvent {
		requireFinite(fractionDate, "fractionDate");
		requireString(actor1Code, "actor1Code");
		requireString(actor1Name, "actor1Name");
		requireString(actor1CountryCode, "actor1CountryCode");
		requireString(actor1KnownGroupCode, "actor1KnownGroupCode");
		requireString(actor1EthnicCode, "actor1EthnicCode");
		requireString(actor1Religion1Code, "actor1Religion1Code");
		requireString(actor1Religion2Code, "actor1Religion2Code");
		requireString(actor1Type1Code, "actor1Type1Code");
		requireString(actor1Type2Code, "actor1Type2Code");
		requireString(actor1Type3Code, "actor1Type3Code");
		requireString(actor2Code, "actor2Code");
		requireString(actor2Name, "actor2Name");
		requireString(actor2CountryCode, "actor2CountryCode");
		requireString(actor2KnownGroupCode, "actor2KnownGroupCode");
		requireString(actor2EthnicCode, "actor2EthnicCode");
		requireString(actor2Religion1Code, "actor2Religion1Code");
		requireString(actor2Religion2Code, "actor2Religion2Code");
		requireString(actor2Type1Code, "actor2Type1Code");
		requireString(actor2Type2Code, "actor2Type2Code");
		requireString(actor2Type3Code, "actor2Type3Code");
		requireString(eventCode, "eventCode");
		requireString(eventBaseCode, "eventBaseCode");
		requireString(eventRootCode, "eventRootCode");
		requireFinite(goldsteinScale, "goldsteinScale");
		requireFinite(avgTone, "avgTone");
		requireString(actor1GeoFullName, "actor1GeoFullName");
		requireString(actor1GeoCountryCode, "actor1GeoCountryCode");
		requireString(actor1GeoAdm1Code, "actor1GeoAdm1Code");
		requireString(actor1GeoAdm2Code, "actor1GeoAdm2Code");
		requireFinite(actor1GeoLat, "actor1GeoLat");
		requireFinite(actor1GeoLong, "actor1GeoLong");
		requireString(actor1GeoFeatureId, "actor1GeoFeatureId");
		requireString(actor2GeoFullName, "actor2GeoFullName");
		requireString(actor2GeoCountryCode, "actor2GeoCountryCode");
		requireString(actor2GeoAdm1Code, "actor2GeoAdm1Code");
		requireString(actor2GeoAdm2Code, "actor2GeoAdm2Code");
		requireFinite(actor2GeoLat, "actor2GeoLat");
		requireFinite(actor2GeoLong, "actor2GeoLong");
		requireString(actor2GeoFeatureId, "actor2GeoFeatureId");
		requireString(actionGeoFullName, "actionGeoFullName");
		requireString(actionGeoCountryCode, "actionGeoCountryCode");
		requireString(actionGeoAdm1Code, "actionGeoAdm1Code");
		requireString(actionGeoAdm2Code, "actionGeoAdm2Code");
		requireFinite(actionGeoLat, "actionGeoLat");
		requireFinite(actionGeoLong, "actionGeoLong");
		requireString(actionGeoFeatureId, "actionGeoFeatureId");
		requireString(sourceUrl, "sourceUrl");
	}

	private static void requireString(String value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
	}

	private static void requireFinite(Double value, String name) {
		if (value != null && !Double.isFinite(value)) {
			throw new IllegalArgumentException(name + " must be finite");
		}
	}
}
