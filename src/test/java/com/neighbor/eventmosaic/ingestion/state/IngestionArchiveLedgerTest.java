package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class IngestionArchiveLedgerTest {

	@Autowired
	private IngestionArchiveLedger archiveLedger;

	@Test
	void registersDiscoveredArchiveIdempotently() {
		DiscoveredArchive archive = new DiscoveredArchive(
				"20260614000000.translation.export.CSV.zip",
				URI.create("https://data.gdeltproject.org/gdeltv2/20260614000000.translation.export.CSV.zip"),
				"abc123",
				ArchiveType.TRANSLATION_EVENTS,
				1024L
		);

		var first = archiveLedger.registerDiscovered(archive);
		var second = archiveLedger.registerDiscovered(archive);

		assertThat(second.idempotencyKey()).isEqualTo(first.idempotencyKey());
		assertThat(second.status()).isEqualTo(IngestionArchiveStatus.DISCOVERED);
		assertThat(archiveLedger.findByIdempotencyKey(archive.idempotencyKey())).contains(second);
	}
}
