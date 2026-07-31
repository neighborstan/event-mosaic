package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Индексируемый документ события GDELT для details, filters и карты.
 *
 * @param globalEventId глобальный идентификатор события
 * @param eventDay календарный день события
 * @param dateAdded время добавления события в GDELT
 * @param actor1Code код первого участника
 * @param actor1Name имя первого участника
 * @param actor1CountryCode код страны первого участника
 * @param actor1KnownGroupCode код известной группы первого участника
 * @param actor1EthnicCode этнический код первого участника
 * @param actor1Religion1Code первый религиозный код первого участника
 * @param actor1Religion2Code второй религиозный код первого участника
 * @param actor1Type1Code первый код типа первого участника
 * @param actor1Type2Code второй код типа первого участника
 * @param actor1Type3Code третий код типа первого участника
 * @param actor2Code код второго участника
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
 * @param eventCode код события CAMEO
 * @param eventBaseCode базовый код события CAMEO
 * @param eventRootCode корневой код события CAMEO
 * @param quadClass укрупненный класс события
 * @param goldsteinScale шкала Goldstein
 * @param averageTone средняя тональность
 * @param numMentions число упоминаний
 * @param numSources число источников
 * @param numArticles число документов
 * @param location выбранная географическая точка или {@code null}
 * @param sourceUrl точный URL или citation исходного документа
 * @param sourceUpdateTime время выпуска исходного архива
 * @param sourceArchiveKey точный ключ исходного архива
 * @param sourceLineNumber физический номер строки CSV
 * @param processingFingerprint fingerprint версии processing projection и mapping
 */
@SuppressWarnings("java:S107")
public record IndexedEventDocument(
		long globalEventId,
		LocalDate eventDay,
		Instant dateAdded,
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
		Double averageTone,
		Integer numMentions,
		Integer numSources,
		Integer numArticles,
		IndexedEventLocation location,
		String sourceUrl,
		Instant sourceUpdateTime,
		String sourceArchiveKey,
		long sourceLineNumber,
		String processingFingerprint
) implements GdeltIndexedDocument {

	/**
	 * Проверяет обязательные значения индексного контракта.
	 */
	public IndexedEventDocument {
		if (globalEventId <= 0) {
			throw new IllegalArgumentException("globalEventId must be positive");
		}
		Objects.requireNonNull(eventDay, "eventDay must not be null");
		Objects.requireNonNull(dateAdded, "dateAdded must not be null");
		Objects.requireNonNull(actor1Code, "actor1Code must not be null");
		Objects.requireNonNull(actor1Name, "actor1Name must not be null");
		Objects.requireNonNull(actor1CountryCode, "actor1CountryCode must not be null");
		Objects.requireNonNull(
				actor1KnownGroupCode,
				"actor1KnownGroupCode must not be null");
		Objects.requireNonNull(actor1EthnicCode, "actor1EthnicCode must not be null");
		Objects.requireNonNull(
				actor1Religion1Code,
				"actor1Religion1Code must not be null");
		Objects.requireNonNull(
				actor1Religion2Code,
				"actor1Religion2Code must not be null");
		Objects.requireNonNull(actor1Type1Code, "actor1Type1Code must not be null");
		Objects.requireNonNull(actor1Type2Code, "actor1Type2Code must not be null");
		Objects.requireNonNull(actor1Type3Code, "actor1Type3Code must not be null");
		Objects.requireNonNull(actor2Code, "actor2Code must not be null");
		Objects.requireNonNull(actor2Name, "actor2Name must not be null");
		Objects.requireNonNull(actor2CountryCode, "actor2CountryCode must not be null");
		Objects.requireNonNull(
				actor2KnownGroupCode,
				"actor2KnownGroupCode must not be null");
		Objects.requireNonNull(actor2EthnicCode, "actor2EthnicCode must not be null");
		Objects.requireNonNull(
				actor2Religion1Code,
				"actor2Religion1Code must not be null");
		Objects.requireNonNull(
				actor2Religion2Code,
				"actor2Religion2Code must not be null");
		Objects.requireNonNull(actor2Type1Code, "actor2Type1Code must not be null");
		Objects.requireNonNull(actor2Type2Code, "actor2Type2Code must not be null");
		Objects.requireNonNull(actor2Type3Code, "actor2Type3Code must not be null");
		Objects.requireNonNull(eventCode, "eventCode must not be null");
		Objects.requireNonNull(eventBaseCode, "eventBaseCode must not be null");
		Objects.requireNonNull(eventRootCode, "eventRootCode must not be null");
		requireFinite(goldsteinScale, "goldsteinScale");
		requireFinite(averageTone, "averageTone");
		Objects.requireNonNull(sourceUrl, "sourceUrl must not be null");
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		if (sourceArchiveKey == null || sourceArchiveKey.isBlank()) {
			throw new IllegalArgumentException("sourceArchiveKey must not be blank");
		}
		if (sourceLineNumber <= 0) {
			throw new IllegalArgumentException("sourceLineNumber must be positive");
		}
		if (processingFingerprint == null || processingFingerprint.isBlank()) {
			throw new IllegalArgumentException("processingFingerprint must not be blank");
		}
	}

	@Override
	public GdeltIndexKind kind() {
		return GdeltIndexKind.EVENT;
	}

	@Override
	public String documentId() {
		return Long.toString(globalEventId);
	}

	private static void requireFinite(Double value, String fieldName) {
		if (value != null && !Double.isFinite(value)) {
			throw new IllegalArgumentException(fieldName + " must be finite");
		}
	}

}
