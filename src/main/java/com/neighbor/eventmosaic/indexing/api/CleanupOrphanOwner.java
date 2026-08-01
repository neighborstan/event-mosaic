package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Фиксирует операцию, которая владела осиротевшей BUILDING generation.
 * Поля нужны для повторной проверки lease, heartbeat и версии под блокировкой.
 */
public record CleanupOrphanOwner(
		long operationId,
		IndexMaintenanceType type,
		IndexMaintenancePhase phase,
		UUID operationToken,
		long operationVersion,
		Instant leaseExpiresAt,
		Instant heartbeatAt
) {

	/** Проверяет immutable identity, lease и heartbeat прежнего владельца. */
	public CleanupOrphanOwner {
		if (operationId <= 0) {
			throw new IllegalArgumentException("operationId должен быть положительным");
		}
		Objects.requireNonNull(type, "type");
		Objects.requireNonNull(phase, "phase");
		Objects.requireNonNull(operationToken, "operationToken");
		if (operationVersion < 0) {
			throw new IllegalArgumentException(
					"operationVersion не может быть отрицательным");
		}
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt");
		Objects.requireNonNull(heartbeatAt, "heartbeatAt");
	}
}
