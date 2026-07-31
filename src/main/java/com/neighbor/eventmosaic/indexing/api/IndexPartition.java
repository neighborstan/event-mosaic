package com.neighbor.eventmosaic.indexing.api;

/**
 * Durable logical partition и ее current generation binding.
 *
 * @param definition exact P7D identity и boundaries
 * @param stateVersion monotonic fencing version
 * @param activeGenerationId current generation либо {@code null} до initial promotion
 */
public record IndexPartition(
		IndexPartitionDefinition definition,
		long stateVersion,
		Long activeGenerationId
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
	}
}
