package com.neighbor.eventmosaic.ingestion.api;

import java.util.Optional;

public interface IngestionArchiveLedger {

	IngestionArchiveState registerDiscovered(DiscoveredArchive archive);

	Optional<IngestionArchiveState> findByIdempotencyKey(String idempotencyKey);
}
