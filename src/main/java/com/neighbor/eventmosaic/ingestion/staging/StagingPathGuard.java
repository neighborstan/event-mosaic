package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.time.Duration;

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
		prepareDirectory(root, directory, OperationBudget.start(Duration.ofDays(1)));
	}

	static void prepareDirectory(
			Path root,
			Path directory,
			OperationBudget budget
	) throws IOException {
		ensureAvailable(budget);
		Files.createDirectories(root);
		ensureAvailable(budget);
		boolean symbolicRoot = Files.isSymbolicLink(root);
		ensureAvailable(budget);
		if (symbolicRoot || !Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
			throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
		}

		Path current = root;
		for (Path segment : root.relativize(directory)) {
			ensureAvailable(budget);
			current = current.resolve(segment);
			if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
				ensureAvailable(budget);
				boolean symbolic = Files.isSymbolicLink(current);
				ensureAvailable(budget);
				if (symbolic || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
					throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
				}
			} else {
				ensureAvailable(budget);
				Files.createDirectory(current);
			}
		}

		ensureAvailable(budget);
		Path realDirectory = directory.toRealPath();
		ensureAvailable(budget);
		if (!realDirectory.startsWith(root.toRealPath())) {
			throw new StagingStorageException(IngestionErrorCode.STAGING_PATH_REJECTED);
		}
	}

	private static void ensureAvailable(OperationBudget budget) {
		try {
			budget.requireAvailable();
		} catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}
}
