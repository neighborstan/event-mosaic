package com.neighbor.eventmosaic.ingestion.staging;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Связанный набор final и temporary paths одного archive attempt.
 *
 * @param root доверенная корневая директория staging
 * @param archivePath финальный ZIP
 * @param archivePartPath временный ZIP текущего attempt
 * @param csvPath финальный CSV
 * @param csvPartPath временный CSV текущего attempt
 */
public record StagingPaths(
		Path root,
		Path archivePath,
		Path archivePartPath,
		Path csvPath,
		Path csvPartPath
) {

	/**
	 * Нормализует пути и проверяет их лексическое нахождение в staging root.
	 */
	public StagingPaths {
		root = normalize(root, "root");
		archivePath = requireInside(root, archivePath, "archivePath");
		archivePartPath = requireInside(root, archivePartPath, "archivePartPath");
		csvPath = requireInside(root, csvPath, "csvPath");
		csvPartPath = requireInside(root, csvPartPath, "csvPartPath");
	}

	private static Path requireInside(Path root, Path path, String name) {
		Path normalized = normalize(path, name);
		if (!normalized.startsWith(root)) {
			throw new IllegalArgumentException(name + " must stay inside staging root");
		}
		return normalized;
	}

	private static Path normalize(Path path, String name) {
		return Objects.requireNonNull(path, name + " must not be null")
				.toAbsolutePath()
				.normalize();
	}
}
