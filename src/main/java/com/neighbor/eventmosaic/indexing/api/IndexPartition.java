package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;

/**
 * Durable logical partition и ее current generation binding.
 *
 * @param definition exact P7D identity и boundaries
 * @param stateVersion monotonic fencing version
 * @param activeGenerationId current generation либо {@code null} до initial promotion
 * @param repairCause незакрытая причина repair либо {@code null}
 * @param repairRequestedAt время фиксации repair cause либо {@code null}
 */
public record IndexPartition(
		IndexPartitionDefinition definition,
		long stateVersion,
		Long activeGenerationId,
		IndexRepairCause repairCause,
		Instant repairRequestedAt
) {

	/** Проверяет identity и неотрицательную fencing version. */
	public IndexPartition {
		java.util.Objects.requireNonNull(definition, "definition must not be null");
		if (stateVersion < 0) {
			throw new IllegalArgumentException("stateVersion must not be negative");
		}
		if (activeGenerationId != null && activeGenerationId <= 0) {
			throw new IllegalArgumentException("activeGenerationId must be positive");
		}
		if ((repairCause == null) != (repairRequestedAt == null)) {
			throw new IllegalArgumentException(
					"repairCause and repairRequestedAt must be present together");
		}
	}

	/** Создает partition без зафиксированной repair cause. */
	public IndexPartition(
			IndexPartitionDefinition definition,
			long stateVersion,
			Long activeGenerationId
	) {
		this(definition, stateVersion, activeGenerationId, null, null);
	}
}
