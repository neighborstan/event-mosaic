package com.neighbor.eventmosaic.ingestion.api;

import java.net.URI;
import java.util.Locale;
import java.util.Objects;

public record DiscoveredArchive(
		String archiveName,
		URI archiveUri,
		String expectedMd5,
		ArchiveType archiveType,
		Long fileSizeBytes
) {

	public DiscoveredArchive {
		Objects.requireNonNull(archiveName, "archiveName must not be null");
		Objects.requireNonNull(archiveUri, "archiveUri must not be null");
		Objects.requireNonNull(archiveType, "archiveType must not be null");
	}

	public String idempotencyKey() {
		String checksum = expectedMd5 == null || expectedMd5.isBlank()
				? "no-md5"
				: expectedMd5.toLowerCase(Locale.ROOT);
		return archiveName + ":" + checksum;
	}
}
