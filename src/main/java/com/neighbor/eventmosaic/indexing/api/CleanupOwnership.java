package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.UUID;

/**
 * CAS-ожидания владельца cleanup operation для следующего перехода.
 */
public record CleanupOwnership(
		String partitionKey,
		UUID operationToken,
		long operationVersion,
		long partitionVersion,
		long generationId,
		long generationVersion,
		String planFingerprint
) {

	/** Проверяет fencing token и полный набор CAS-версий. */
	public CleanupOwnership {
		requireText(partitionKey, "partitionKey");
		Objects.requireNonNull(operationToken, "operationToken");
		if (operationVersion < 0 || partitionVersion < 0 || generationVersion < 0) {
			throw new IllegalArgumentException(
					"Версии cleanup ownership не могут быть отрицательными");
		}
		if (generationId <= 0) {
			throw new IllegalArgumentException("generationId должен быть положительным");
		}
		requireText(planFingerprint, "planFingerprint");
	}

    /** Создает CAS-ожидания из последнего durable snapshot операции. */
	public static CleanupOwnership from(CleanupOperation operation) {
		Objects.requireNonNull(operation, "operation");
		return new CleanupOwnership(
				operation.partitionKey(),
				operation.operationToken(),
				operation.operationVersion(),
				operation.partitionVersion(),
				operation.generationId(),
				operation.generationVersion(),
				operation.planFingerprint());
	}

	private static void requireText(String value, String name) {
		if (value == null || value.isBlank()) {
			throw new IllegalArgumentException(name + " не должен быть пустым");
		}
	}
}
