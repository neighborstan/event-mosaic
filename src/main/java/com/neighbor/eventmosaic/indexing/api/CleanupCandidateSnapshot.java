package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Снимок generation, которую оператор может рассмотреть для fenced cleanup.
 * Наличие записи не означает, что удаление уже разрешено: внешний слой еще
 * проверяет aliases, physical UUID, receipts и replay sources.
 */
public record CleanupCandidateSnapshot(
		String partitionKey,
		long partitionVersion,
		boolean repairOpen,
		long generationId,
		UUID generationUuid,
		long generationNumber,
		IndexGenerationStatus status,
		long generationVersion,
		IndexGenerationNames names,
		String eventIndexUuid,
		String mentionIndexUuid,
		String failureOrigin,
		Instant heartbeatAt,
		Instant supersededAt,
		Instant failedAt,
		CleanupOrphanOwner orphanOwner,
		CleanupBuildWriteOutcome buildWriteOutcome,
		boolean activeProcessing,
		CleanupProtectedActive protectedActive
) {

	/** Проверяет identity и обязательные evidence выбранного состояния. */
	public CleanupCandidateSnapshot {
		requireText(partitionKey, "partitionKey");
		if (partitionVersion < 0) {
			throw new IllegalArgumentException(
					"partitionVersion не может быть отрицательным");
		}
		if (generationId <= 0) {
			throw new IllegalArgumentException(
					"generationId должен быть положительным");
		}
		Objects.requireNonNull(generationUuid, "generationUuid");
		if (generationNumber <= 0) {
			throw new IllegalArgumentException("generationNumber должен быть положительным");
		}
		Objects.requireNonNull(status, "status");
		if (generationVersion < 0) {
			throw new IllegalArgumentException(
					"generationVersion не может быть отрицательным");
		}
		Objects.requireNonNull(names, "names");
		Objects.requireNonNull(heartbeatAt, "heartbeatAt");
		Objects.requireNonNull(buildWriteOutcome, "buildWriteOutcome");
		if (status != IndexGenerationStatus.BUILDING
				&& status != IndexGenerationStatus.FAILED
				&& status != IndexGenerationStatus.SUPERSEDED) {
			throw new IllegalArgumentException(
					"Cleanup поддерживает только BUILDING, FAILED и SUPERSEDED generation");
		}
		if (status == IndexGenerationStatus.BUILDING && orphanOwner == null) {
			throw new IllegalArgumentException(
					"BUILDING candidate должен содержать owner operation");
		}
		if (status == IndexGenerationStatus.FAILED
				&& (failedAt == null || failureOrigin == null || failureOrigin.isBlank())) {
			throw new IllegalArgumentException(
					"FAILED candidate должен содержать failure evidence");
		}
		if (status == IndexGenerationStatus.SUPERSEDED
				&& (supersededAt == null || protectedActive == null)) {
			throw new IllegalArgumentException(
					"SUPERSEDED candidate должен содержать protected ACTIVE snapshot");
		}
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым");
		}
	}
}
