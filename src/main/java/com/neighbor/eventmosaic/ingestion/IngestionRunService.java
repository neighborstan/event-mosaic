package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import org.springframework.stereotype.Service;

@Service
public class IngestionRunService {

	private final IngestionArchiveLedger archiveLedger;

	public IngestionRunService(IngestionArchiveLedger archiveLedger) {
		this.archiveLedger = archiveLedger;
	}

	public IngestionArchiveState registerDiscoveredArchive(DiscoveredArchive archive) {
		return archiveLedger.registerDiscovered(archive);
	}
}
