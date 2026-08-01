package com.neighbor.eventmosaic.ingestion.api;

/** Результат попытки связать processing ownership с exact ACTIVE target. */
public enum ArchiveProcessingClaimStatus {
	CLAIMED,
	NOT_CLAIMABLE,
	MAINTENANCE_DEFERRED,
	OWNERSHIP_LOST
}
