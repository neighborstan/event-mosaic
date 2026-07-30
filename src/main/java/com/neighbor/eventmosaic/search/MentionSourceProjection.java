package com.neighbor.eventmosaic.search;

import java.time.Instant;
import java.util.Objects;

/**
 * Узкая внутренняя проекция полей Mention, необходимых Event details API.
 *
 * @param mentionSourceName имя источника
 * @param mentionIdentifier идентификатор материала источника
 * @param mentionTimeDate время появления упоминания
 * @param mentionDocTone тональность документа
 */
record MentionSourceProjection(
		String mentionSourceName,
		String mentionIdentifier,
		Instant mentionTimeDate,
		Double mentionDocTone
) {

	MentionSourceProjection {
		Objects.requireNonNull(mentionSourceName, "mentionSourceName must not be null");
		if (mentionIdentifier == null || mentionIdentifier.isBlank()) {
			throw new IllegalArgumentException("mentionIdentifier must not be blank");
		}
		Objects.requireNonNull(mentionTimeDate, "mentionTimeDate must not be null");
		if (mentionDocTone != null && !Double.isFinite(mentionDocTone)) {
			throw new IllegalArgumentException("mentionDocTone must be finite");
		}
	}

}
