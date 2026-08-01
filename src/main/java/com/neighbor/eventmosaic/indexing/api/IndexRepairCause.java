package com.neighbor.eventmosaic.indexing.api;

/**
 * Ограниченный набор причин, по которым разрешено перестроить logical
 * partition целиком.
 */
public enum IndexRepairCause {
	SURPLUS,
	MISSING_CURRENT,
	CORRUPT_CURRENT
}
