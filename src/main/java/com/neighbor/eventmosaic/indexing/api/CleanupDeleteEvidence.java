package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Фиксирует свежие внешние проверки непосредственно перед durable
 * DELETE_REQUESTED.
 */
public record CleanupDeleteEvidence(
		IndexGenerationNames names,
		String eventIndexUuid,
		String mentionIndexUuid,
		boolean aliasesConfirmedAbsent,
		boolean currentReceiptsConfirmed,
		boolean replaySourcesConfirmed
) {

	/** Проверяет exact names и nullable UUID подтвержденных targets. */
	public CleanupDeleteEvidence {
		Objects.requireNonNull(names, "names");
		requireOptionalText(eventIndexUuid, "eventIndexUuid");
		requireOptionalText(mentionIndexUuid, "mentionIndexUuid");
	}

	private static void requireOptionalText(String value, String name) {
		if (value != null && value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым, если задан");
		}
	}
}
