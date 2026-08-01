package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;

/** Deterministic P7D partition и schema-v1 names заданной generation. */
public record IndexPartitionGenerationResolution(
		IndexPartitionDefinition partition,
		IndexGenerationNames names
) {

	public IndexPartitionGenerationResolution {
		Objects.requireNonNull(partition, "partition must not be null");
		Objects.requireNonNull(names, "names must not be null");
	}
}
