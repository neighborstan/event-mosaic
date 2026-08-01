package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/** Ограниченные operational outcomes явной очистки technical generation. */
public enum GenerationCleanupErrorCode implements ApplicationErrorCode {
	UNKNOWN_PARTITION("Cleanup partition is unknown"),
	CLEANUP_CANDIDATE_NOT_FOUND("Cleanup generation is not an eligible candidate"),
	CLEANUP_NOT_ELIGIBLE("Cleanup evidence does not permit deletion"),
	STALE_CLEANUP_PLAN("Cleanup plan is stale"),
	CLEANUP_MAINTENANCE_BUSY("Cleanup partition has an active maintenance operation"),
	CLEANUP_OWNERSHIP_LOST("Cleanup operation ownership was lost"),
	CLEANUP_EXACT_TARGET_CONFLICT("Cleanup exact index identity changed"),
	CLEANUP_ALIAS_CONFLICT("Cleanup target has an alias membership"),
	CLEANUP_RECONCILIATION_REQUIRED("Cleanup requires lifecycle reconciliation"),
	CLEANUP_SOURCE_INVALID("Cleanup replay source is unavailable or changed"),
	CLEANUP_RECEIPT_MISMATCH("Cleanup current generation evidence changed"),
	CLEANUP_OPERATION_UNAVAILABLE("Cleanup dependency is temporarily unavailable");

	private final String safeMessage;

	GenerationCleanupErrorCode(String safeMessage) {
		this.safeMessage = safeMessage;
	}

	@Override
	public String code() {
		return name();
	}

	@Override
	public String safeMessage() {
		return safeMessage;
	}
}
