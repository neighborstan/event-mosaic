package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Проверенный результат скачивания или безопасного повторного использования ZIP.
 *
 * @param path путь ZIP в staging
 * @param sizeBytes фактический размер файла
 * @param md5 фактическая MD5 checksum
 * @param reused признак повторного использования уже опубликованного ZIP
 */
public record DownloadedArchive(
		Path path,
		long sizeBytes,
		String md5,
		boolean reused
) {

	/**
	 * Проверяет путь, положительный размер и нормализует checksum.
	 */
	public DownloadedArchive {
		Objects.requireNonNull(path, "path must not be null");
		if (sizeBytes <= 0) {
			throw new IllegalArgumentException("sizeBytes must be positive");
		}
		md5 = GdeltSourceContract.normalizeMd5(md5);
	}
}
