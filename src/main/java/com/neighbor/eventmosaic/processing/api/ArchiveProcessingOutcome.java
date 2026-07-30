package com.neighbor.eventmosaic.processing.api;

/**
 * Ограниченный итог одного processing attempt.
 */
public enum ArchiveProcessingOutcome {
	COMPLETED,
	FAILED,
	OWNERSHIP_LOST
}
