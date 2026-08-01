package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/**
 * Подтверждает результат точной проверки обоих физических индексов после
 * delete. Ledger принимает CLEANED только когда оба ожидаемых UUID отсутствуют.
 */
public record CleanupCompletionEvidence(
		IndexGenerationNames names,
		String eventIndexUuid,
		String mentionIndexUuid,
		boolean eventIndexAbsent,
		boolean mentionIndexAbsent
) {

	/** Проверяет exact names и nullable UUID удаленных targets. */
	public CleanupCompletionEvidence {
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
