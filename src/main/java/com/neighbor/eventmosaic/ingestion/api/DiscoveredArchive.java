package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.gdelt.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.GdeltSourceContract;
import java.net.URI;
import java.time.Instant;
import java.util.Objects;

/**
 * Неизменяемое описание поддерживаемого GDELT archive, обнаруженного в manifest.
 *
 * @param sourceUpdateTime время source update в UTC
 * @param archiveName каноническое имя ZIP-файла GDELT
 * @param metadataUri исходный URI строки manifest
 * @param expectedMd5 ожидаемая MD5 checksum из manifest
 * @param archiveType тип архива ingestion
 * @param expectedSizeBytes ожидаемый размер ZIP в байтах
 */
public record DiscoveredArchive(
		Instant sourceUpdateTime,
		String archiveName,
		URI metadataUri,
		String expectedMd5,
		ArchiveType archiveType,
		long expectedSizeBytes
) {

	/**
	 * Проверяет согласованность имени, времени, типа и source fingerprint.
	 */
	public DiscoveredArchive {
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		Objects.requireNonNull(archiveType, "archiveType must not be null");
		GdeltArchiveName parsedName = GdeltArchiveName.requireSupported(archiveName);
		String metadataFileName = GdeltSourceContract.requireMetadataFileName(metadataUri);
		expectedMd5 = GdeltSourceContract.normalizeMd5(expectedMd5);
		ArchiveType parsedType = ArchiveType.fromGdeltKind(parsedName.kind());
		if (!sourceUpdateTime.equals(parsedName.updateTime()) || archiveType != parsedType) {
			throw new IllegalArgumentException("archiveName, sourceUpdateTime and archiveType must be consistent");
		}
		if (!archiveName.equals(metadataFileName)) {
			throw new IllegalArgumentException("metadataUri and archiveName must be consistent");
		}
		if (expectedSizeBytes <= 0) {
			throw new IllegalArgumentException("expectedSizeBytes must be positive");
		}
	}

	/**
	 * Возвращает стабильный ключ идемпотентности для конкретной версии archive.
	 *
	 * @return комбинация имени archive и нормализованной checksum
	 */
	public String idempotencyKey() {
		return archiveName + ":" + expectedMd5;
	}
}
