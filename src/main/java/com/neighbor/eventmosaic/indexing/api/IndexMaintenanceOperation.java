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
 * @param repairCause причина rebuild либо {@code null}
 * @param planFingerprint подтвержденный fingerprint либо {@code null}
 * @param planExpiresAt срок исходного плана либо {@code null}
 * @param actor bounded identity оператора либо {@code null}
 * @param reasonCode bounded reason code либо {@code null}
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
		long operationVersion,
		IndexRepairCause repairCause,
		String planFingerprint,
		Instant planExpiresAt,
		String actor,
		String reasonCode
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
		boolean planAbsent = planFingerprint == null
				&& planExpiresAt == null
				&& actor == null
				&& reasonCode == null;
		boolean planPresent = planFingerprint != null
				&& planExpiresAt != null
				&& actor != null
				&& reasonCode != null;
		if (!planAbsent && !planPresent) {
			throw new IllegalArgumentException("plan audit fields must be present together");
		}
		if (planPresent) {
			requirePattern(planFingerprint, "[0-9a-f]{64}", "planFingerprint");
			requirePattern(actor, "[A-Za-z0-9._-]{1,64}", "actor");
			requirePattern(reasonCode, "[A-Z][A-Z0-9_]{0,63}", "reasonCode");
		}
		if (repairCause != null && type != IndexMaintenanceType.REBUILD) {
			throw new IllegalArgumentException("repairCause is supported only for rebuild");
		}
	}

	/** Создает operation прежнего lifecycle без fingerprinted repair plan. */
	public IndexMaintenanceOperation(
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
		this(
				id,
				partitionKey,
				type,
				phase,
				token,
				leaseExpiresAt,
				partitionVersion,
				baseGenerationId,
				buildingGenerationId,
				operationVersion,
				null,
				null,
				null,
				null,
				null);
	}

	private static void requirePattern(String value, String pattern, String field) {
		if (!value.matches(pattern)) {
			throw new IllegalArgumentException(field + " has invalid format");
		}
	}
}
