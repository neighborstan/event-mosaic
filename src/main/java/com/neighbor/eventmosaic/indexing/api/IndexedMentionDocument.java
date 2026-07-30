package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;

/**
 * Индексируемый документ упоминания с сохранением всех 16 исходных полей GDELT.
 *
 * @param rawMentionId детерминированный идентификатор raw observation
 * @param sourceDocumentKey устойчивый ключ исходного документа
 * @param globalEventId глобальный идентификатор события
 * @param eventTimeDate время события из строки упоминания
 * @param mentionTimeDate время появления упоминания
 * @param mentionType тип упоминания
 * @param mentionSourceName имя источника
 * @param mentionIdentifier идентификатор материала источника
 * @param sentenceId номер предложения
 * @param actor1CharOffset смещение первого участника
 * @param actor2CharOffset смещение второго участника
 * @param actionCharOffset смещение действия
 * @param inRawText признак присутствия в исходном тексте
 * @param confidence уверенность GDELT
 * @param mentionDocLen длина документа
 * @param mentionDocTone тональность документа
 * @param mentionDocTranslationInfo информация о переводе
 * @param extras дополнительные данные строки
 * @param sourceUpdateTime время выпуска исходного архива
 * @param sourceArchiveKey точный ключ исходного архива
 * @param sourceLineNumber физический номер строки CSV
 */
@SuppressWarnings("java:S107")
public record IndexedMentionDocument(
		String rawMentionId,
		String sourceDocumentKey,
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
		String extras,
		Instant sourceUpdateTime,
		String sourceArchiveKey,
		long sourceLineNumber
) implements GdeltIndexedDocument {

	/**
	 * Проверяет обязательные значения индексного контракта.
	 */
	public IndexedMentionDocument {
		if (rawMentionId == null || rawMentionId.isBlank()) {
			throw new IllegalArgumentException("rawMentionId must not be blank");
		}
		if (sourceDocumentKey == null || sourceDocumentKey.isBlank()) {
			throw new IllegalArgumentException("sourceDocumentKey must not be blank");
		}
		if (globalEventId <= 0) {
			throw new IllegalArgumentException("globalEventId must be positive");
		}
		Objects.requireNonNull(eventTimeDate, "eventTimeDate must not be null");
		Objects.requireNonNull(mentionTimeDate, "mentionTimeDate must not be null");
		Objects.requireNonNull(mentionType, "mentionType must not be null");
		Objects.requireNonNull(mentionSourceName, "mentionSourceName must not be null");
		if (mentionIdentifier == null || mentionIdentifier.isBlank()) {
			throw new IllegalArgumentException("mentionIdentifier must not be blank");
		}
		Objects.requireNonNull(mentionDocTranslationInfo, "mentionDocTranslationInfo must not be null");
		Objects.requireNonNull(extras, "extras must not be null");
		if (mentionDocTone != null && !Double.isFinite(mentionDocTone)) {
			throw new IllegalArgumentException("mentionDocTone must be finite");
		}
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
		return GdeltIndexKind.MENTION;
	}

	@Override
	public String documentId() {
		return rawMentionId;
	}

}
