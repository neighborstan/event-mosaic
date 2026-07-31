package com.neighbor.eventmosaic.indexing.api;

/** Вид fenced lifecycle operation над logical partition. */
public enum IndexMaintenanceType {
	INITIAL_PROMOTION,
	REBUILD,
	CLEANUP
}
