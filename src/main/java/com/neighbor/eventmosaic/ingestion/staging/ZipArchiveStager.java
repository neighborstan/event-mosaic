package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.ArchiveContentViolationException;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Objects;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Безопасно извлекает единственный ожидаемый CSV из GDELT ZIP и публикует
 * final hard link без перезаписи существующего файла.
 */
@Component
public class ZipArchiveStager {

	private static final int BUFFER_SIZE = 8192;
	private static final Logger LOGGER = LoggerFactory.getLogger(ZipArchiveStager.class);

	private final int maxEntries;
	private final long maxEntryBytes;
	private final long maxTotalBytes;
	private final IngestionMetrics metrics;

	/**
	 * Создает stager с ограничениями ZIP из runtime properties.
	 *
	 * @param properties ограничения количества entry и распакованного размера
	 * @param metrics publisher cleanup diagnostics
	 */
	@Autowired
	public ZipArchiveStager(GdeltIngestionProperties properties, IngestionMetrics metrics) {
		this(
				properties.zip().maxEntries(),
				properties.zip().maxEntryBytes(),
				properties.zip().maxTotalBytes(),
				metrics
		);
	}

	ZipArchiveStager(
			int maxEntries,
			long maxEntryBytes,
			long maxTotalBytes,
			IngestionMetrics metrics
	) {
		if (maxEntries <= 0 || maxEntryBytes <= 0 || maxTotalBytes <= 0) {
			throw new IllegalArgumentException("ZIP limits must be positive");
		}
		if (maxEntryBytes > maxTotalBytes) {
			throw new IllegalArgumentException("maxEntryBytes must not exceed maxTotalBytes");
		}
		this.maxEntries = maxEntries;
		this.maxEntryBytes = maxEntryBytes;
		this.maxTotalBytes = maxTotalBytes;
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	/**
	 * Извлекает и публикует CSV либо проверяет ранее опубликованный artifact.
	 *
	 * @param attempt текущее владение archive
	 * @param downloadedArchive проверенный ZIP
	 * @param paths безопасные staging paths
	 * @return финальные пути и fingerprint staged archive
	 */
	public StagedArchive stage(
			ArchiveAttempt attempt,
			DownloadedArchive downloadedArchive,
			StagingPaths paths
	) {
		return stage(
				attempt,
				downloadedArchive,
				paths,
				OperationBudget.start(Duration.ofDays(1)));
	}

	/** Извлекает CSV в пределах общей границы времени и владения циклом. */
	public StagedArchive stage(
			ArchiveAttempt attempt,
			DownloadedArchive downloadedArchive,
			StagingPaths paths,
			OperationBudget budget
	) {
		IngestionInterruption.throwIfRequested();
		ensureAvailable(budget);
		try {
			StagingPathGuard.prepareDirectory(
					paths.root(),
					paths.csvPath().getParent(),
					budget);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
		ensureAvailable(budget);
		if (!Files.isRegularFile(downloadedArchive.path(), LinkOption.NOFOLLOW_LINKS)) {
			throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
		}
		try {
			ensureAvailable(budget);
			Files.deleteIfExists(paths.csvPartPath());
			ensureAvailable(budget);
			if (Files.exists(paths.csvPath(), LinkOption.NOFOLLOW_LINKS)) {
				verifyExistingCsv(
						attempt.archive(),
						downloadedArchive.path(),
						paths.csvPath(),
						budget);
				return staged(downloadedArchive, paths.csvPath());
			}
			extractExpectedCsv(attempt, downloadedArchive.path(), paths, budget);
			IngestionInterruption.throwIfRequested();
			if (!publishAtomically(paths.csvPartPath(), paths.csvPath(), budget)) {
				verifyExistingCsv(
						attempt.archive(),
						downloadedArchive.path(),
						paths.csvPath(),
						budget);
			}
			return staged(downloadedArchive, paths.csvPath());
		} catch (ZipException exception) {
			throw new ArchiveContentViolationException(
					IngestionErrorCode.ZIP_CONTENT_MISMATCH,
					exception);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		} finally {
			deleteTemporary(paths.csvPartPath());
		}
	}

	/**
	 * Повторно проверяет неизменность verified ZIP и производного final CSV без
	 * записи во staging.
	 *
	 * @param state durable STAGED archive
	 * @param budget общая граница времени и проверка владения циклом
	 */
	public void verifyReplaySource(
			IngestionArchiveState state,
			OperationBudget budget
	) {
		Objects.requireNonNull(state, "state must not be null");
		Objects.requireNonNull(budget, "budget must not be null");
		if (state.status() != IngestionArchiveStatus.STAGED
				|| state.stagedArchive() == null) {
			throw new IllegalArgumentException("Replay source must be STAGED");
		}
		DiscoveredArchive archive = state.archive();
		StagedArchive staged = state.stagedArchive();
		try {
			ensureAvailable(budget);
			if (!Files.isRegularFile(staged.archivePath(), LinkOption.NOFOLLOW_LINKS)
					|| !isRegularFile(staged.csvPath(), budget)) {
				throw new StagingStorageException(
						IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			ensureAvailable(budget);
			long actualSize = Files.size(staged.archivePath());
			String actualMd5 = Md5Checksum.calculate(staged.archivePath(), budget);
			if (actualSize != archive.expectedSizeBytes()
					|| actualSize != staged.actualSizeBytes()
					|| !actualMd5.equals(archive.expectedMd5())
					|| !actualMd5.equals(staged.actualMd5())) {
				throw new StagingStorageException(
						IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			verifyExistingCsv(
					archive,
					staged.archivePath(),
					staged.csvPath(),
					budget);
		}
		catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(
					IngestionErrorCode.FILESYSTEM_IO_FAILURE,
					exception);
		}
	}

	private void extractExpectedCsv(
			ArchiveAttempt attempt,
			Path archivePath,
			StagingPaths paths,
			OperationBudget budget
	) throws IOException {
		ensureAvailable(budget);
		try (OutputStream output = Files.newOutputStream(
				paths.csvPartPath(),
				StandardOpenOption.CREATE_NEW)) {
			inspectExpectedCsv(
					attempt.archive(),
					archivePath,
					paths.csvPath().getParent(),
					output,
					budget);
		}
	}

	private EntryFingerprint inspectExpectedCsv(
			DiscoveredArchive archive,
			Path archivePath,
			Path dataRoot,
			OutputStream output,
			OperationBudget budget
	) throws IOException {
		String expectedName = expectedCsvName(archive);
		int entryCount = 0;
		long totalBytes = 0;
		MessageDigest digest = Md5Checksum.newDigest();
		ensureAvailable(budget);
		try (ZipInputStream zip = new ZipInputStream(Files.newInputStream(archivePath))) {
			while (true) {
				IngestionInterruption.throwIfRequested();
				ensureLoopAvailable(budget);
				ZipEntry entry = zip.getNextEntry();
				if (entry == null) {
					break;
				}
				entryCount++;
				validateEntry(entryCount, entry, expectedName, dataRoot);
				totalBytes = copyEntry(zip, output, digest, totalBytes, budget);
				ensureLoopAvailable(budget);
				zip.closeEntry();
			}
		}
		IngestionInterruption.throwIfRequested();
		ensureAvailable(budget);
		if (entryCount != 1) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		}
		return new EntryFingerprint(totalBytes, Md5Checksum.hex(digest));
	}

	private void validateEntry(
			int entryCount,
			ZipEntry entry,
			String expectedName,
			Path dataRoot
	) {
		if (entryCount > maxEntries) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_LIMIT_EXCEEDED);
		}
		validateEntryPath(entry, expectedName, dataRoot);
		if (entryCount > 1) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		}
	}

	private long copyEntry(
			ZipInputStream zip,
			OutputStream output,
			MessageDigest digest,
			long totalBytes,
			OperationBudget budget
	) throws IOException {
		long entryBytes = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		while (true) {
			IngestionInterruption.throwIfRequested();
			ensureLoopAvailable(budget);
			int read = zip.read(buffer);
			ensureLoopAvailable(budget);
			if (read == -1) {
				return totalBytes;
			}
			entryBytes += read;
			totalBytes += read;
			if (entryBytes > maxEntryBytes || totalBytes > maxTotalBytes) {
				throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_LIMIT_EXCEEDED);
			}
			ensureLoopAvailable(budget);
			output.write(buffer, 0, read);
			digest.update(buffer, 0, read);
		}
	}

	private static void validateEntryPath(ZipEntry entry, String expectedName, Path dataRoot) {
		Path entryPath;
		try {
			entryPath = Path.of(entry.getName());
		} catch (RuntimeException _) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_PATH_REJECTED);
		}
		Path resolved = dataRoot.resolve(entryPath).normalize();
		if (entryPath.isAbsolute() || !resolved.startsWith(dataRoot.toAbsolutePath().normalize())) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_PATH_REJECTED);
		}
		if (entry.isDirectory() || !entry.getName().equals(expectedName)) {
			throw new ArchiveContentViolationException(IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		}
	}

	private void verifyExistingCsv(
			DiscoveredArchive archive,
			Path archivePath,
			Path csvPath,
			OperationBudget budget
	) {
		try {
			ensureAvailable(budget);
			if (!Files.isRegularFile(csvPath, LinkOption.NOFOLLOW_LINKS)) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			ensureAvailable(budget);
			long existingSize = Files.size(csvPath);
			if (existingSize > maxEntryBytes) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			EntryFingerprint expected = inspectExpectedCsv(
					archive,
					archivePath,
					csvPath.getParent(),
					OutputStream.nullOutputStream(),
					budget);
			if (existingSize != expected.sizeBytes()
					|| !Md5Checksum.calculate(csvPath, budget).equals(expected.md5())) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
		} catch (ZipException exception) {
			throw new ArchiveContentViolationException(
					IngestionErrorCode.ZIP_CONTENT_MISMATCH,
					exception);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private static String expectedCsvName(DiscoveredArchive archive) {
		return GdeltArchiveName.requireSupported(archive.archiveName()).csvName();
	}

	private static StagedArchive staged(DownloadedArchive archive, Path csvPath) {
		return new StagedArchive(archive.path(), csvPath, archive.sizeBytes(), archive.md5());
	}

	private static boolean publishAtomically(
			Path partPath,
			Path finalPath,
			OperationBudget budget
	) {
		try {
			ensureAvailable(budget);
			if (!Files.isRegularFile(partPath, LinkOption.NOFOLLOW_LINKS)) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			ensureAvailable(budget);
			Files.createLink(finalPath, partPath);
			return true;
		} catch (UnsupportedOperationException exception) {
			throw new StagingStorageException(
					IngestionErrorCode.STAGING_ATOMIC_PUBLICATION_UNSUPPORTED,
					exception);
		} catch (FileAlreadyExistsException _) {
			return false;
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private void deleteTemporary(Path path) {
		TemporaryArtifactCleaner.delete(path, metrics, LOGGER);
	}

	private static boolean isRegularFile(Path path, OperationBudget budget) {
		ensureAvailable(budget);
		return Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
	}

	private static void ensureAvailable(OperationBudget budget) {
		try {
			budget.requireAvailable();
		} catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void ensureLoopAvailable(OperationBudget budget) {
		try {
			budget.requireLoopAvailable();
		} catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private record EntryFingerprint(long sizeBytes, String md5) {
	}
}
