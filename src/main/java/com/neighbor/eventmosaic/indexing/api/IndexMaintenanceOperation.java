package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Durable operation, которую можно загрузить после потери process-local state.
 *
 * @param id database identity
 * @param partitionKey owning logical partition
 * @param type вид operation
 * @param phase текущая durable phase
 * @param token immutable ownership token
 * @param leaseExpiresAt lease boundary
 * @param partitionVersion captured fencing version
 * @param baseGenerationId исходная ACTIVE generation либо {@code null}
 * @param buildingGenerationId создаваемая BUILDING generation
 * @param operationVersion monotonic operation fencing version
 */
public record IndexMaintenanceOperation(
		long id,
		String partitionKey,
		IndexMaintenanceType type,
		IndexMaintenancePhase phase,
		UUID token,
		Instant leaseExpiresAt,
		long partitionVersion,
		Long baseGenerationId,
		long buildingGenerationId,
		long operationVersion
) {

	/** Проверяет обязательную identity и captured ownership. */
	public IndexMaintenanceOperation {
		if (id <= 0) {
			throw new IllegalArgumentException("id must be positive");
		}
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		if (partitionKey.isBlank()) {
			throw new IllegalArgumentException("partitionKey must not be blank");
		}
		Objects.requireNonNull(type, "type must not be null");
		Objects.requireNonNull(phase, "phase must not be null");
		Objects.requireNonNull(token, "token must not be null");
		Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
		if (partitionVersion <= 0) {
			throw new IllegalArgumentException("partitionVersion must be positive");
		}
		if (baseGenerationId != null && baseGenerationId <= 0) {
			throw new IllegalArgumentException("baseGenerationId must be positive");
		}
		if (buildingGenerationId <= 0) {
			throw new IllegalArgumentException("buildingGenerationId must be positive");
		}
		if (operationVersion < 0) {
			throw new IllegalArgumentException("operationVersion must not be negative");
		}
	}
}
