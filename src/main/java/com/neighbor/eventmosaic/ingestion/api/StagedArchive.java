package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.gdelt.GdeltSourceContract;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Финальные пути и фактическая fingerprint опубликованных staging artifacts.
 *
 * @param archivePath путь опубликованного ZIP
 * @param csvPath путь опубликованного CSV
 * @param actualSizeBytes фактический размер ZIP
 * @param actualMd5 фактическая MD5 checksum ZIP
 */
public record StagedArchive(
		Path archivePath,
		Path csvPath,
		long actualSizeBytes,
		String actualMd5
) {

	/**
	 * Проверяет обязательные пути, положительный размер и нормализует checksum.
	 */
	public StagedArchive {
		Objects.requireNonNull(archivePath, "archivePath must not be null");
		Objects.requireNonNull(csvPath, "csvPath must not be null");
		actualMd5 = GdeltSourceContract.normalizeMd5(actualMd5);
		if (actualSizeBytes <= 0) {
			throw new IllegalArgumentException("actualSizeBytes must be positive");
		}
	}
}
