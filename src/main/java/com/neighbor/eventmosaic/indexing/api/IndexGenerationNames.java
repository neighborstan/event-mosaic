package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Exact physical имена Event и Mention индексов одного поколения.
 *
 * @param eventIndexName exact Event index name
 * @param mentionIndexName exact Mention index name
 */
public record IndexGenerationNames(String eventIndexName, String mentionIndexName) {
	private static final Pattern EVENT_NAME = Pattern.compile(
			"^gdelt-events-v1-p[0-9]{8}-g[0-9]{4,}$");
	private static final Pattern MENTION_NAME = Pattern.compile(
			"^gdelt-mentions-v1-p[0-9]{8}-g[0-9]{4,}$");

	/** Проверяет обязательные и различные exact names. */
	public IndexGenerationNames {
		requireName(eventIndexName, "eventIndexName", EVENT_NAME);
		requireName(mentionIndexName, "mentionIndexName", MENTION_NAME);
		if (eventIndexName.equals(mentionIndexName)) {
			throw new IllegalArgumentException("Event and Mention index names must differ");
		}
	}

	private static void requireName(String value, String field, Pattern pattern) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.length() > 255 || !pattern.matcher(value).matches()) {
			throw new IllegalArgumentException(
					field + " must be an exact schema v1 physical index name");
		}
	}
}
