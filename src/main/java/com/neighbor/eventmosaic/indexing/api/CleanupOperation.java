package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Представляет durable cleanup operation. Ее token остается неизменным от
 * claim до подтвержденного CLEANED, включая восстановление после сбоя.
 */
public record CleanupOperation(
		long operationId,
		String partitionKey,
		IndexMaintenancePhase phase,
		UUID operationToken,
		long operationVersion,
		Instant leaseExpiresAt,
		long partitionVersion,
		long generationId,
		long generationVersion,
		Long protectedActiveGenerationId,
		Long protectedActiveGenerationVersion,
		String planFingerprint,
		Instant planExpiresAt,
		String actor,
		String reasonCode
) {

	/** Проверяет durable identity, версии и незавершенную cleanup phase. */
	public CleanupOperation {
		if (operationId <= 0) {
			throw new IllegalArgumentException("operationId должен быть положительным");
		}
		requireText(partitionKey, "partitionKey");
		Objects.requireNonNull(phase, "phase");
		if (phase != IndexMaintenancePhase.CLEANUP_PENDING
				&& phase != IndexMaintenancePhase.DELETE_REQUESTED) {
			throw new IllegalArgumentException(
					"Cleanup operation должна быть в незавершенной cleanup phase");
		}
		Objects.requireNonNull(operationToken, "operationToken");
		if (operationVersion < 0 || partitionVersion < 0 || generationVersion < 0) {
			throw new IllegalArgumentException(
					"Версии cleanup operation не могут быть отрицательными");
		}
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
		if (generationId <= 0) {
			throw new IllegalArgumentException("generationId должен быть положительным");
		}
		if (protectedActiveGenerationId != null && protectedActiveGenerationId <= 0) {
			throw new IllegalArgumentException(
					"protectedActiveGenerationId должен быть положительным");
		}
		if ((protectedActiveGenerationId == null)
				!= (protectedActiveGenerationVersion == null)) {
			throw new IllegalArgumentException(
					"Protected ACTIVE id и version должны задаваться вместе");
		}
		if (protectedActiveGenerationVersion != null && protectedActiveGenerationVersion < 0) {
			throw new IllegalArgumentException(
					"protectedActiveGenerationVersion не может быть отрицательным");
		}
		requireText(planFingerprint, "planFingerprint");
		Objects.requireNonNull(planExpiresAt, "planExpiresAt");
		requireText(actor, "actor");
		requireText(reasonCode, "reasonCode");
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым");
		}
	}
}
