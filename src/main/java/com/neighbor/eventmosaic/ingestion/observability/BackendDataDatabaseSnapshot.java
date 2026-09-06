package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;
import java.util.Set;

/**
 * Содержит агрегированное operational state из PostgreSQL. Exact index names
 * остаются внутренним входом alias check и наружу не публикуются.
 */
record BackendDataDatabaseSnapshot(
		long lagSeconds,
		long openGaps,
		BackendDataOperationalSnapshot.RetryCounts sourcePollRetries,
		BackendDataOperationalSnapshot.RetryCounts acquisitionRetries,
		BackendDataOperationalSnapshot.RetryCounts processingRetries,
		long permanentFailures,
		BackendDataOperationalSnapshot.ReceiptCounts receipts,
		BackendDataOperationalSnapshot.GenerationCounts generations,
		long repairRequiredPartitions,
		long openMaintenanceOperations,
		long aliasTransitionOperations,
		Set<String> activeEventIndices,
		Set<String> activeMentionIndices,
		BackendDataOperationalSnapshot.LiveCounts live
) {

	BackendDataDatabaseSnapshot {
		if (lagSeconds < 0
				|| openGaps < 0
				|| permanentFailures < 0
				|| repairRequiredPartitions < 0
				|| openMaintenanceOperations < 0
				|| aliasTransitionOperations < 0) {
			throw new IllegalArgumentException("operational counts must not be negative");
		}
		Objects.requireNonNull(sourcePollRetries, "sourcePollRetries must not be null");
		Objects.requireNonNull(acquisitionRetries, "acquisitionRetries must not be null");
		Objects.requireNonNull(processingRetries, "processingRetries must not be null");
		Objects.requireNonNull(receipts, "receipts must not be null");
		Objects.requireNonNull(generations, "generations must not be null");
		Objects.requireNonNull(live, "live must not be null");
		activeEventIndices = Set.copyOf(Objects.requireNonNull(
				activeEventIndices,
				"activeEventIndices must not be null"));
		activeMentionIndices = Set.copyOf(Objects.requireNonNull(
				activeMentionIndices,
				"activeMentionIndices must not be null"));
	}
}
