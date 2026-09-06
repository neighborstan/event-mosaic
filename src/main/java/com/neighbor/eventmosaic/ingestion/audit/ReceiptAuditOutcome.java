package com.neighbor.eventmosaic.ingestion.audit;

/** Ограниченный набор результатов фоновой проверки сохраненных документов. */
enum ReceiptAuditOutcome {
	MATCHED,
	SHORTAGE,
	SURPLUS,
	IDENTITY_MISMATCH,
	MISSING_GENERATION,
	STALE_BINDING,
	INFRASTRUCTURE_FAILURE
}
