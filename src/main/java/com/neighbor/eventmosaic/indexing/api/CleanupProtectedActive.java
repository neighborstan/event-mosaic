package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.UUID;

/**
 * Снимок текущей ACTIVE generation, которую нельзя незаметно заменить во время
 * очистки старой SUPERSEDED generation.
 */
public record CleanupProtectedActive(
		long generationId,
		UUID generationUuid,
		long generationVersion,
		IndexGenerationNames names,
		String eventIndexUuid,
		String mentionIndexUuid
) {

	/** Проверяет coherent identity текущей защищаемой generation. */
	public CleanupProtectedActive {
		if (generationId <= 0) {
			throw new IllegalArgumentException("generationId должен быть положительным");
		}
		Objects.requireNonNull(generationUuid, "generationUuid");
		if (generationVersion < 0) {
			throw new IllegalArgumentException(
					"generationVersion не может быть отрицательным");
		}
		Objects.requireNonNull(names, "names");
		requireText(eventIndexUuid, "eventIndexUuid");
		requireText(mentionIndexUuid, "mentionIndexUuid");
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым");
		}
	}
}
