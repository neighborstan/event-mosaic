package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;

/**
 * Создает staging directories без перехода через symbolic links ниже root.
 */
final class StagingPathGuard {

	private StagingPathGuard() {
	}

	/**
	 * Создает целевой каталог и отклоняет symbolic link на любом сегменте ниже root.
	 *
	 * @param root доверенная корневая директория
	 * @param directory создаваемая директория внутри root
	 * @throws IOException при ошибке filesystem operation
	 */
	static void prepareDirectory(Path root, Path directory) throws IOException {
		Files.createDirectories(root);
		if (Files.isSymbolicLink(root) || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
			throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
		}

		Path current = root;
		for (Path segment : root.relativize(directory)) {
			current = current.resolve(segment);
			if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
				if (Files.isSymbolicLink(current)
						|| !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
					throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
				}
			} else {
				Files.createDirectory(current);
			}
		}

		if (!directory.toRealPath().startsWith(root.toRealPath())) {
			throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
		}
	}
}
