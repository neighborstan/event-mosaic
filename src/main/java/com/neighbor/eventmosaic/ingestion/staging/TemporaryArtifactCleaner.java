package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import org.slf4j.Logger;

/**
 * Удаляет attempt-scoped temporary artifacts и наблюдаемо сообщает об отказах.
 */
final class TemporaryArtifactCleaner {

	private TemporaryArtifactCleaner() {
	}

	/**
	 * Best-effort удаляет один temporary artifact и наблюдаемо сообщает об ошибке.
	 *
	 * @param path удаляемый temporary path
	 * @param metrics publisher метрик
	 * @param logger owning component logger
	 */
	static void delete(
			Path path,
			IngestionMetrics metrics,
			Logger logger
	) {
		Objects.requireNonNull(path, "path must not be null");
		Objects.requireNonNull(metrics, "metrics must not be null");
		Objects.requireNonNull(logger, "logger must not be null");
		try {
			Files.deleteIfExists(path);
		} catch (IOException exception) {
			report(metrics, logger, exception);
		}
	}

	private static void report(
			IngestionMetrics metrics,
			Logger logger,
			IOException exception
	) {
		metrics.cleanupFailed();
		logger.atWarn()
				.setCause(SafeExceptionProjection.from(
						exception,
						"Temporary staging artifact cleanup failed"))
				.addKeyValue("event", "gdelt.cleanup.failed")
				.log("Temporary staging artifact cleanup failed");
	}
}
