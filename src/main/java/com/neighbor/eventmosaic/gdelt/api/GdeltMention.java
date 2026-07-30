package com.neighbor.eventmosaic.gdelt.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Неизменяемая raw-запись GDELT Mention из 16 полей в точном provider order.
 *
 * <p>Строковые значения сохраняются verbatim. Пустой физический {@code Extras}
 * представлен строкой {@code ""}, а не {@code null}.</p>
 *
 * @param globalEventId идентификатор упомянутого события
 * @param eventTimeDate UTC timestamp первой регистрации события
 * @param mentionTimeDate UTC timestamp текущего Mention update
 * @param mentionType тип исходной коллекции документа
 * @param mentionSourceName имя источника документа
 * @param mentionIdentifier внешний идентификатор документа
 * @param sentenceId номер предложения с упоминанием
 * @param actor1CharOffset позиция первого участника
 * @param actor2CharOffset позиция второго участника
 * @param actionCharOffset позиция действия
 * @param inRawText признак обнаружения в исходном тексте
 * @param confidence уверенность извлечения
 * @param mentionDocLen длина исходного документа
 * @param mentionDocTone тон исходного документа
 * @param mentionDocTranslationInfo информация о переводе
 * @param extras зарезервированные дополнительные данные
 */
public record GdeltMention(
		long globalEventId,
		Instant eventTimeDate,
		Instant mentionTimeDate,
		Integer mentionType,
		String mentionSourceName,
		String mentionIdentifier,
		Integer sentenceId,
		Integer actor1CharOffset,
		Integer actor2CharOffset,
		Integer actionCharOffset,
		Integer inRawText,
		Integer confidence,
		Integer mentionDocLen,
		Double mentionDocTone,
		String mentionDocTranslationInfo,
		String extras
) {

	/** Проверяет обязательный identifier, verbatim-строки и finite tone. */
	public GdeltMention {
		Objects.requireNonNull(mentionSourceName, "mentionSourceName must not be null");
		Objects.requireNonNull(mentionIdentifier, "mentionIdentifier must not be null");
		if (mentionIdentifier.isBlank()) {
			throw new IllegalArgumentException("mentionIdentifier must not be blank");
		}
		if (mentionDocTone != null && !Double.isFinite(mentionDocTone)) {
			throw new IllegalArgumentException("mentionDocTone must be finite");
		}
		Objects.requireNonNull(
				mentionDocTranslationInfo,
				"mentionDocTranslationInfo must not be null");
		Objects.requireNonNull(extras, "extras must not be null");
	}
}
