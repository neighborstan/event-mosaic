package com.neighbor.eventmosaic.indexing.api;

/** Bounded outcome разрешения exact writable generation. */
public enum IndexTargetResolutionStatus {
	READY,
	MAINTENANCE_DEFERRED,
	OWNERSHIP_LOST,
	MISSING,
	WRITE_BLOCKED
}
