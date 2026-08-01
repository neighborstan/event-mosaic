package com.neighbor.eventmosaic.ingestion.staging;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpBodyDeadline;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpStatusPolicy;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.ingestion.error.TransferredArtifactIntegrityException;
import com.neighbor.eventmosaic.ingestion.source.ArchiveDownloadUriResolver;
import com.neighbor.eventmosaic.ingestion.source.HttpRetryAfterParser;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Clock;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Потоково скачивает ZIP в attempt-scoped temporary path, проверяет размер и
 * checksum, затем публикует final hard link без перезаписи существующего файла.
 */
@Component
public class HttpArchiveDownloader {

	private static final int BUFFER_SIZE = 8192;
	private static final Logger LOGGER = LoggerFactory.getLogger(HttpArchiveDownloader.class);

	private final HttpClient httpClient;
	private final Duration requestTimeout;
	private final long maxArchiveBytes;
	private final IngestionMetrics metrics;
	private final ArchiveDownloadUriResolver downloadUriResolver;
	private final HttpRetryAfterParser retryAfterParser;

	/**
	 * Создает production downloader с общим HTTP transport и runtime limits.
	 *
	 * @param httpClient общий HTTP client GDELT
	 * @param properties timeout и ограничения размера archive
	 * @param metrics publisher cleanup diagnostics
	 * @param downloadUriResolver доверенная политика разрешения URI объекта
	 * @param retryAfterParser parser разрешенной серверной retry-подсказки
	 */
	@Autowired
	public HttpArchiveDownloader(
			HttpClient httpClient,
			GdeltIngestionProperties properties,
			IngestionMetrics metrics,
			ArchiveDownloadUriResolver downloadUriResolver,
			HttpRetryAfterParser retryAfterParser
	) {
		this(
				httpClient,
				properties.http().requestTimeout(),
				properties.http().maxArchiveBytes(),
				metrics,
				downloadUriResolver,
				retryAfterParser
		);
	}

	/** Создает adapter с system UTC parser для прямого wiring вне Spring. */
	public HttpArchiveDownloader(
			HttpClient httpClient,
			GdeltIngestionProperties properties,
			IngestionMetrics metrics,
			ArchiveDownloadUriResolver downloadUriResolver
	) {
		this(
				httpClient,
				properties,
				metrics,
				downloadUriResolver,
				new HttpRetryAfterParser(Clock.systemUTC()));
	}

	HttpArchiveDownloader(
			HttpClient httpClient,
			Duration requestTimeout,
			long maxArchiveBytes,
			IngestionMetrics metrics,
			ArchiveDownloadUriResolver downloadUriResolver
	) {
		this(
				httpClient,
				requestTimeout,
				maxArchiveBytes,
				metrics,
				downloadUriResolver,
				new HttpRetryAfterParser(Clock.systemUTC()));
	}

	HttpArchiveDownloader(
			HttpClient httpClient,
			Duration requestTimeout,
			long maxArchiveBytes,
			IngestionMetrics metrics,
			ArchiveDownloadUriResolver downloadUriResolver,
			HttpRetryAfterParser retryAfterParser
	) {
		if (maxArchiveBytes <= 0) {
			throw new IllegalArgumentException("maxArchiveBytes must be positive");
		}
		this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
		this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
		if (requestTimeout.isZero() || requestTimeout.isNegative()) {
			throw new IllegalArgumentException("requestTimeout must be positive");
		}
		this.maxArchiveBytes = maxArchiveBytes;
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
		this.downloadUriResolver = Objects.requireNonNull(
				downloadUriResolver,
				"downloadUriResolver must not be null");
		this.retryAfterParser = Objects.requireNonNull(
				retryAfterParser,
				"retryAfterParser must not be null");
	}

	/**
	 * Скачивает или повторно использует ZIP только после проверки manifest fingerprint.
	 *
	 * @param attempt текущее владение archive
	 * @param paths безопасные staging paths текущего attempt
	 * @return проверенный опубликованный ZIP
	 */
	public DownloadedArchive download(ArchiveAttempt attempt, StagingPaths paths) {
		return download(attempt, paths, OperationBudget.start(Duration.ofDays(1)));
	}

	/** Скачивает archive в пределах общего monotonic cycle budget. */
	public DownloadedArchive download(
			ArchiveAttempt attempt,
			StagingPaths paths,
			OperationBudget budget
	) {
		IngestionInterruption.throwIfRequested();
		throwIfExpired(budget);
		if (attempt.archive().expectedSizeBytes() > maxArchiveBytes) {
			throw new RemoteResponseRejectedException(
					IngestionErrorCode.DOWNLOAD_SIZE_LIMIT_EXCEEDED);
		}
		try {
			StagingPathGuard.prepareDirectory(
					paths.root(),
					paths.archivePath().getParent(),
					budget);
			Files.deleteIfExists(paths.archivePartPath());
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
		if (Files.exists(paths.archivePath(), LinkOption.NOFOLLOW_LINKS)) {
			return verifyExisting(attempt, paths.archivePath(), budget);
		}

		try {
			Duration effectiveTimeout = requireRemaining(budget);
			boolean cycleLimited = effectiveTimeout.compareTo(requestTimeout) < 0;
			long deadlineNanos = GdeltHttpBodyDeadline.deadlineAfter(effectiveTimeout);
			HttpResponse<InputStream> response = send(
					attempt,
					effectiveTimeout,
					budget,
					cycleLimited);
			DownloadedArchive downloaded;
			try (InputStream input = response.body();
					GdeltHttpBodyDeadline deadline = GdeltHttpBodyDeadline.start(input, deadlineNanos)) {
				if (response.statusCode() != 200) {
					int status = response.statusCode();
					if (GdeltHttpStatusPolicy.isTransient(status)) {
						throw new RemoteSourceAccessException(
								IngestionErrorCode.DOWNLOAD_HTTP_ERROR,
								IngestionErrorContext.forHttpStatus(status),
								retryAfterParser.parse(response.headers()));
					}
					throw new RemoteResponseRejectedException(
							IngestionErrorCode.DOWNLOAD_HTTP_STATUS_REJECTED,
							IngestionErrorContext.forHttpStatus(status));
				}
				validateContentLength(response, attempt);
				downloaded = writeAndVerify(
						input,
						attempt,
						paths.archivePartPath(),
						deadline,
						budget,
						cycleLimited);
				if (deadline.expired()) {
					throwIfCycleLimited(cycleLimited);
					throwIfExpired(budget);
					throw new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_TIMEOUT);
				}
			}
			IngestionInterruption.throwIfRequested();
			if (!publishAtomically(paths.archivePartPath(), paths.archivePath())) {
				return verifyExisting(attempt, paths.archivePath(), budget);
			}
			return new DownloadedArchive(
					paths.archivePath(),
					downloaded.sizeBytes(),
					downloaded.md5(),
					false
			);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, exception);
		} finally {
			deleteTemporary(paths.archivePartPath());
		}
	}

	private HttpResponse<InputStream> send(
			ArchiveAttempt attempt,
			Duration effectiveTimeout,
			OperationBudget budget,
			boolean cycleLimited
	) {
		HttpRequest request = HttpRequest.newBuilder(downloadUriResolver.resolve(
				GdeltArchiveName.requireSupported(attempt.archive().archiveName())))
				.timeout(effectiveTimeout)
				.GET()
				.build();
		try {
			return httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IngestionInterruptedException(exception);
		} catch (HttpTimeoutException exception) {
			throwIfCycleLimited(cycleLimited);
			throwIfExpired(budget);
			throw new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_TIMEOUT, exception);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throwIfExpired(budget);
			throw new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, exception);
		}
	}

	private DownloadedArchive verifyExisting(
			ArchiveAttempt attempt,
			Path archivePath,
			OperationBudget budget
	) {
		try {
			throwIfExpired(budget);
			if (!Files.isRegularFile(archivePath, LinkOption.NOFOLLOW_LINKS)) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			long size = Files.size(archivePath);
			if (size != attempt.archive().expectedSizeBytes() || size > maxArchiveBytes) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			String md5 = Md5Checksum.calculate(archivePath, budget);
			if (!md5.equalsIgnoreCase(attempt.archive().expectedMd5())) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			return new DownloadedArchive(archivePath, size, md5, true);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private void validateContentLength(HttpResponse<InputStream> response, ArchiveAttempt attempt) {
		long contentLength = response.headers().firstValueAsLong("Content-Length").orElse(-1);
		if (contentLength > maxArchiveBytes) {
			throw new RemoteResponseRejectedException(
					IngestionErrorCode.DOWNLOAD_SIZE_LIMIT_EXCEEDED);
		}
		if (contentLength >= 0 && contentLength != attempt.archive().expectedSizeBytes()) {
			throw new TransferredArtifactIntegrityException(
					IngestionErrorCode.DOWNLOAD_SIZE_MISMATCH);
		}
	}

	private DownloadedArchive writeAndVerify(
			InputStream input,
			ArchiveAttempt attempt,
			Path partPath,
			GdeltHttpBodyDeadline deadline,
			OperationBudget budget,
			boolean cycleLimited
	) {
		MessageDigest digest = Md5Checksum.newDigest();
		long total = 0;
		byte[] buffer = new byte[BUFFER_SIZE];
		try (OutputStream output = Files.newOutputStream(partPath, StandardOpenOption.CREATE_NEW)) {
			while (true) {
				IngestionInterruption.throwIfRequested();
				throwIfExpired(budget);
				int read = readChunk(input, buffer, deadline, budget, cycleLimited);
				throwIfExpired(budget);
				if (read == -1) {
					break;
				}
				total += read;
				if (total > attempt.archive().expectedSizeBytes()) {
					throw new TransferredArtifactIntegrityException(
							IngestionErrorCode.DOWNLOAD_SIZE_MISMATCH);
				}
				writeChunk(output, buffer, read);
				digest.update(buffer, 0, read);
			}
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
		String md5 = Md5Checksum.hex(digest);
		if (total != attempt.archive().expectedSizeBytes()) {
			throw new TransferredArtifactIntegrityException(IngestionErrorCode.DOWNLOAD_SIZE_MISMATCH);
		}
		if (!md5.equalsIgnoreCase(attempt.archive().expectedMd5())) {
			throw new TransferredArtifactIntegrityException(IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
		}
		return new DownloadedArchive(partPath, total, md5, false);
	}

	private static int readChunk(
			InputStream input,
			byte[] buffer,
			GdeltHttpBodyDeadline deadline,
			OperationBudget budget,
			boolean cycleLimited
	) {
		try {
			return input.read(buffer);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throwIfExpired(budget);
			if (deadline.expired()) {
				throwIfCycleLimited(cycleLimited);
				throw new RemoteSourceAccessException(
						IngestionErrorCode.DOWNLOAD_TIMEOUT,
						exception);
			}
			throw new RemoteSourceAccessException(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, exception);
		}
	}

	private static void writeChunk(OutputStream output, byte[] buffer, int length) {
		try {
			output.write(buffer, 0, length);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private static boolean publishAtomically(Path partPath, Path finalPath) {
		try {
			if (!Files.isRegularFile(partPath, LinkOption.NOFOLLOW_LINKS)) {
				throw new StagingStorageException(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
			}
			Files.createLink(finalPath, partPath);
			return true;
		} catch (FileAlreadyExistsException _) {
			return false;
		} catch (UnsupportedOperationException exception) {
			throw new StagingStorageException(
					IngestionErrorCode.STAGING_ATOMIC_PUBLICATION_UNSUPPORTED,
					exception);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throw new StagingStorageException(IngestionErrorCode.FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private void deleteTemporary(Path path) {
		TemporaryArtifactCleaner.delete(path, metrics, LOGGER);
	}

	private Duration requireRemaining(OperationBudget budget) {
		Duration timeout = budget.cap(requestTimeout);
		if (timeout.isZero()) {
			throw new OperationDeadlineExceededException();
		}
		return timeout;
	}

	private static void throwIfExpired(OperationBudget budget) {
		if (!budget.hasRemaining()) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void throwIfCycleLimited(boolean cycleLimited) {
		if (cycleLimited) {
			throw new OperationDeadlineExceededException();
		}
	}
}
