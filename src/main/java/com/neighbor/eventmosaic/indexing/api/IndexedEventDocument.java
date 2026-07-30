package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Минимальный индексируемый документ события GDELT.
 *
 * @param globalEventId глобальный идентификатор события
 * @param eventDay календарный день события
 * @param dateAdded время добавления события в GDELT
 * @param actor1Name имя первого участника
 * @param actor1Code код первого участника
 * @param actor2Name имя второго участника
 * @param actor2Code код второго участника
 * @param eventCode код события CAMEO
 * @param eventBaseCode базовый код события CAMEO
 * @param eventRootCode корневой код события CAMEO
 * @param quadClass укрупненный класс события
 * @param goldsteinScale шкала Goldstein
 * @param averageTone средняя тональность
 * @param location выбранная географическая точка или {@code null}
 * @param sourceUpdateTime время выпуска исходного архива
 * @param sourceArchiveKey точный ключ исходного архива
 * @param sourceLineNumber физический номер строки CSV
 */
@SuppressWarnings("java:S107")
public record IndexedEventDocument(
		long globalEventId,
		LocalDate eventDay,
		Instant dateAdded,
		String actor1Name,
		String actor1Code,
		String actor2Name,
		String actor2Code,
		String eventCode,
		String eventBaseCode,
		String eventRootCode,
		Integer quadClass,
		Double goldsteinScale,
		Double averageTone,
		IndexedEventLocation location,
		Instant sourceUpdateTime,
		String sourceArchiveKey,
		long sourceLineNumber
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
		Objects.requireNonNull(actor1Name, "actor1Name must not be null");
		Objects.requireNonNull(actor1Code, "actor1Code must not be null");
		Objects.requireNonNull(actor2Name, "actor2Name must not be null");
		Objects.requireNonNull(actor2Code, "actor2Code must not be null");
		Objects.requireNonNull(eventCode, "eventCode must not be null");
		Objects.requireNonNull(eventBaseCode, "eventBaseCode must not be null");
		Objects.requireNonNull(eventRootCode, "eventRootCode must not be null");
		requireFinite(goldsteinScale, "goldsteinScale");
		requireFinite(averageTone, "averageTone");
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		if (sourceArchiveKey == null || sourceArchiveKey.isBlank()) {
			throw new IllegalArgumentException("sourceArchiveKey must not be blank");
		}
		if (sourceLineNumber <= 0) {
			throw new IllegalArgumentException("sourceLineNumber must be positive");
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
