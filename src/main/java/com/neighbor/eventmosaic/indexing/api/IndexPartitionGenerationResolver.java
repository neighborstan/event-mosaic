package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;

/** Deterministic resolver Monday-UTC P7D partition и canonical generation names. */
public interface IndexPartitionGenerationResolver {

	IndexPartitionGenerationResolution resolve(Instant sourceUpdateTime, int generationNumber);
}
