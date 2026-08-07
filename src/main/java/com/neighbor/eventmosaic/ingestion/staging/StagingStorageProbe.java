package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Проверяет, что настроенный staging root безопасен и доступен для реальной
 * записи, не раскрывая наружу filesystem paths и исключения.
 */
@Component
public class StagingStorageProbe {

	private static final byte[] PROBE_CONTENT = {0};
	private static final String PROBE_PREFIX = ".event-mosaic-storage-probe-";
	private static final String PROBE_SUFFIX = ".tmp";

	private final Path root;

	/**
	 * Создает probe для настроенного staging root.
	 *
	 * @param properties настройки ingestion
	 */
	@Autowired
	public StagingStorageProbe(GdeltIngestionProperties properties) {
		this(properties.stagingRoot());
	}

	StagingStorageProbe(Path root) {
		this.root = Objects.requireNonNull(root, "root must not be null")
				.toAbsolutePath()
				.normalize();
	}

	/**
	 * Безопасно подготавливает root, создает, записывает и удаляет маленький
	 * временный файл, а также измеряет доступное место filesystem.
	 *
	 * @return ограниченный результат без path, exception и свободного текста
	 */
	public StagingStorageProbeResult probe() {
		StagingStorageProbeResult rootFailure = prepareRoot();
		if (rootFailure != null) {
			return rootFailure;
		}

		Path probeFile;
		try {
			probeFile = Files.createTempFile(root, PROBE_PREFIX, PROBE_SUFFIX);
		} catch (IOException | RuntimeException exception) {
			return StagingStorageProbeResult.unavailable(
					StagingStorageProbeErrorCode.WRITE_CHECK_FAILED);
		}

		try {
			Files.write(
					probeFile,
					PROBE_CONTENT,
					StandardOpenOption.WRITE,
					StandardOpenOption.TRUNCATE_EXISTING);
		} catch (IOException | RuntimeException exception) {
			return failureAfterCleanup(
					probeFile,
					StagingStorageProbeErrorCode.WRITE_CHECK_FAILED);
		}

		long usableBytes;
		try {
			usableBytes = Files.getFileStore(probeFile).getUsableSpace();
		} catch (IOException | RuntimeException exception) {
			return failureAfterCleanup(
					probeFile,
					StagingStorageProbeErrorCode.USABLE_SPACE_CHECK_FAILED);
		}
		if (usableBytes < 0) {
			return failureAfterCleanup(
					probeFile,
					StagingStorageProbeErrorCode.USABLE_SPACE_CHECK_FAILED);
		}

		try {
			Files.delete(probeFile);
		} catch (IOException | RuntimeException exception) {
			return StagingStorageProbeResult.unavailable(
					StagingStorageProbeErrorCode.CLEANUP_CHECK_FAILED);
		}

		return StagingStorageProbeResult.writable(usableBytes);
	}

	private StagingStorageProbeResult prepareRoot() {
		try {
			StagingPathGuard.prepareDirectory(root, root);
			return null;
		} catch (StagingStorageException exception) {
			return StagingStorageProbeResult.unsafe(
					StagingStorageProbeErrorCode.PATH_REJECTED);
		} catch (IOException | RuntimeException exception) {
			return StagingStorageProbeResult.unavailable(
					StagingStorageProbeErrorCode.ROOT_ACCESS_FAILED);
		}
	}

	private StagingStorageProbeResult failureAfterCleanup(
			Path probeFile,
			StagingStorageProbeErrorCode originalError
	) {
		try {
			Files.delete(probeFile);
			return StagingStorageProbeResult.unavailable(originalError);
		} catch (IOException | RuntimeException exception) {
			return StagingStorageProbeResult.unavailable(
					StagingStorageProbeErrorCode.CLEANUP_CHECK_FAILED);
		}
	}
}
