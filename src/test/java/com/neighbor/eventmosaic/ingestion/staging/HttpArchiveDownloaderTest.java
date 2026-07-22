package com.neighbor.eventmosaic.ingestion.staging;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.archive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.ingestion.error.TransferredArtifactIntegrityException;
import com.neighbor.eventmosaic.ingestion.source.ArchiveDownloadUriResolver;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Загрузка архивов по HTTP")
class HttpArchiveDownloaderTest {

	@TempDir
	Path tempDir;

	private HttpServer server;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.start();
	}

	@AfterEach
	void stopServer() {
		server.stop(0);
		Thread.interrupted();
	}

	@Test
	@DisplayName("Установленный interrupt flag отменяет download до HTTP request")
	void interruptFlagStopsBeforeHttpRequest() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/archive.zip", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, new byte[]{1}, true);
		});
		ArchiveAttempt attempt = attempt(1, md5(new byte[]{1}));
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		Thread.currentThread().interrupt();
		HttpArchiveDownloader downloader = downloader(1024);

		assertThatExceptionOfType(IngestionInterruptedException.class)
				.isThrownBy(() -> downloader.download(attempt, paths))
				.satisfies(exception -> {
					assertThat(exception).hasNoCause();
					assertThat(Thread.currentThread().isInterrupted()).isTrue();
				});
		assertThat(requests).hasValue(0);
		assertThat(paths.archivePartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Архив загружается, проверяется, публикуется и повторно используется")
	void downloadsVerifiesPublishesAndReusesArchive() throws IOException {
		byte[] body = "verified archive".getBytes(StandardCharsets.UTF_8);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/archive.zip", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, body, true);
		});
		ArchiveAttempt firstAttempt = attempt(body.length, md5(body));
		StagingLayout layout = new StagingLayout(tempDir);
		StagingPaths firstPaths = layout.pathsFor(firstAttempt);

		DownloadedArchive first = downloader(1024).download(firstAttempt, firstPaths);
		ArchiveAttempt retry = new ArchiveAttempt(
				firstAttempt.archive(),
				UUID.randomUUID(),
				Instant.parse("2026-07-20T12:01:00Z"),
				2,
				true);
		StagingPaths retryPaths = layout.pathsFor(retry);
		DownloadedArchive reused = downloader(1024).download(retry, retryPaths);

		assertThat(Files.readAllBytes(first.path())).isEqualTo(body);
		assertThat(first.reused()).isFalse();
		assertThat(reused.reused()).isTrue();
		assertThat(requests).hasValue(1);
		assertThat(firstPaths.archivePartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Попытка удаляет только свой временный файл")
	void doesNotDeletePartFileOwnedByAnotherAttempt() throws IOException {
		byte[] body = "verified archive".getBytes(StandardCharsets.UTF_8);
		ArchiveAttempt currentAttempt = attempt(body.length, md5(body));
		ArchiveAttempt otherAttempt = new ArchiveAttempt(
				currentAttempt.archive(),
				UUID.randomUUID(),
				Instant.parse("2026-07-20T12:02:00Z"),
				2,
				true);
		StagingLayout layout = new StagingLayout(tempDir);
		StagingPaths currentPaths = layout.pathsFor(currentAttempt);
		StagingPaths otherPaths = layout.pathsFor(otherAttempt);
		Files.createDirectories(currentPaths.archivePath().getParent());
		Files.write(currentPaths.archivePath(), body);
		Files.writeString(currentPaths.archivePartPath(), "stale current attempt");
		Files.writeString(otherPaths.archivePartPath(), "active other attempt");

		DownloadedArchive reused = downloader(1024).download(currentAttempt, currentPaths);

		assertThat(reused.reused()).isTrue();
		assertThat(currentPaths.archivePartPath()).doesNotExist();
		assertThat(otherPaths.archivePartPath()).hasContent("active other attempt");
	}

	@Test
	@DisplayName("Ошибка HTTP отклоняется, а временный файл удаляется")
	void rejectsHttpErrorAndCleansPartFile() {
		server.createContext("/archive.zip", exchange -> respond(exchange, 503, new byte[0], true));
		ArchiveAttempt attempt = attempt(1, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);

		assertFailure(
				() -> downloader(1024).download(attempt, paths),
				IngestionErrorCode.DOWNLOAD_HTTP_ERROR,
				RemoteSourceAccessException.class);
		assertThat(paths.archivePartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Deadline прерывает чтение незавершенного streaming archive")
	void deadlineStopsIncompleteStreamingArchive() {
		CountDownLatch releaseBody = new CountDownLatch(1);
		server.createContext("/archive.zip", exchange -> {
			exchange.sendResponseHeaders(200, 0);
			exchange.getResponseBody().write('x');
			exchange.getResponseBody().flush();
			try {
				releaseBody.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			} finally {
				exchange.close();
			}
		});
		ArchiveAttempt attempt = attempt(2, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);

		try {
			assertFailure(
					() -> downloader(1024, Duration.ofMillis(100)).download(attempt, paths),
					IngestionErrorCode.DOWNLOAD_TIMEOUT,
					RemoteSourceAccessException.class);
			assertThat(paths.archivePartPath()).doesNotExist();
		} finally {
			releaseBody.countDown();
		}
	}

	@Test
	@DisplayName("Request timeout до получения headers имеет отдельный код")
	void classifiesRequestTimeoutBeforeResponseHeaders() {
		CountDownLatch releaseResponse = new CountDownLatch(1);
		server.createContext("/archive.zip", exchange -> {
			try {
				releaseResponse.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			} finally {
				exchange.close();
			}
		});
		ArchiveAttempt attempt = attempt(1, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);

		try {
			assertFailure(
					() -> downloader(1024, Duration.ofMillis(100)).download(attempt, paths),
					IngestionErrorCode.DOWNLOAD_TIMEOUT,
					RemoteSourceAccessException.class);
			assertThat(paths.archivePartPath()).doesNotExist();
		} finally {
			releaseResponse.countDown();
		}
	}

	@Test
	@DisplayName("Неповторяемый HTTP status сохраняется в безопасном контексте")
	void rejectsPermanentHttpStatusWithoutAutomaticRetry() {
		server.createContext("/archive.zip", exchange -> respond(exchange, 404, new byte[0], true));
		ArchiveAttempt attempt = attempt(1, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		HttpArchiveDownloader downloader = downloader(1024);

		assertThatExceptionOfType(RemoteResponseRejectedException.class)
				.isThrownBy(() -> downloader.download(attempt, paths))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IngestionErrorCode.DOWNLOAD_HTTP_STATUS_REJECTED);
					assertThat(exception.context().httpStatus()).isEqualTo(404);
				});
	}

	@Test
	@DisplayName("Корректный файл от прежней попытки принимается при гонке публикации")
	void acceptsValidArtifactPublishedByExpiredAttempt() throws IOException {
		byte[] body = "verified archive".getBytes(StandardCharsets.UTF_8);
		ArchiveAttempt attempt = attempt(body.length, md5(body));
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		server.createContext("/archive.zip", exchange -> {
			Files.createDirectories(paths.archivePath().getParent());
			Files.write(paths.archivePath(), body);
			respond(exchange, 200, body, true);
		});

		DownloadedArchive downloaded = downloader(1024).download(attempt, paths);

		assertThat(downloaded.reused()).isTrue();
		assertThat(Files.readAllBytes(downloaded.path())).isEqualTo(body);
		assertThat(paths.archivePartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Архив сверх настроенного лимита отклоняется до сетевого запроса")
	void rejectsConfiguredLimitBeforeRequest() {
		byte[] body = "too large".getBytes(StandardCharsets.UTF_8);
		ArchiveAttempt attempt = attempt(body.length, md5(body));

		assertFailure(
				() -> downloader(body.length - 1).download(attempt, new StagingLayout(tempDir).pathsFor(attempt)),
				IngestionErrorCode.DOWNLOAD_SIZE_LIMIT_EXCEEDED,
				RemoteResponseRejectedException.class
		);
	}

	@Test
	@DisplayName("Несовпадение фактического размера и контрольной суммы отклоняется")
	void rejectsActualSizeAndChecksumMismatch() {
		byte[] body = "archive".getBytes(StandardCharsets.UTF_8);
		server.createContext("/archive.zip", exchange -> respond(exchange, 200, body, false));
		ArchiveAttempt wrongSize = attempt(body.length + 1, md5(body));
		assertFailure(
				() -> downloader(1024).download(wrongSize, new StagingLayout(tempDir.resolve("size")).pathsFor(wrongSize)),
				IngestionErrorCode.DOWNLOAD_SIZE_MISMATCH,
				TransferredArtifactIntegrityException.class
		);

		ArchiveAttempt wrongMd5 = attempt(body.length, "00000000000000000000000000000000");
		assertFailure(
				() -> downloader(1024).download(wrongMd5, new StagingLayout(tempDir.resolve("md5")).pathsFor(wrongMd5)),
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH,
				TransferredArtifactIntegrityException.class
		);
	}

	@Test
	@DisplayName("Chunked body прекращает читаться сразу после превышения ожидаемого размера")
	void stopsChunkedBodyAfterExpectedSizeIsExceeded() {
		byte[] body = "oversized".getBytes(StandardCharsets.UTF_8);
		server.createContext("/archive.zip", exchange -> respond(exchange, 200, body, false));
		ArchiveAttempt attempt = attempt(1, md5(new byte[]{body[0]}));
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);

		assertFailure(
				() -> downloader(1024).download(attempt, paths),
				IngestionErrorCode.DOWNLOAD_SIZE_MISMATCH,
				TransferredArtifactIntegrityException.class);
		assertThat(paths.archivePartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Конфликт итогового файла отклоняется без сетевого запроса")
	void rejectsConflictingFinalArtifactWithoutNetworkCall() throws IOException {
		byte[] body = "archive".getBytes(StandardCharsets.UTF_8);
		ArchiveAttempt attempt = attempt(body.length, md5(body));
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		Files.createDirectories(paths.archivePath().getParent());
		Files.writeString(paths.archivePath(), "different");

		assertFailure(
				() -> downloader(1024).download(attempt, paths),
				IngestionErrorCode.STAGING_ARTIFACT_CONFLICT,
				StagingStorageException.class
		);
	}

	@Test
	@DisplayName("Symbolic link внутри staging path отклоняется до сетевого запроса")
	void rejectsSymbolicLinkInsideStagingPath() throws IOException {
		ArchiveAttempt attempt = attempt(1, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		Path link = paths.archivePath().getParent();
		Path target = tempDir.resolve("redirect-target");
		Files.createDirectories(link.getParent());
		Files.createDirectories(target);
		try {
			Files.createSymbolicLink(link, target);
		} catch (UnsupportedOperationException | SecurityException _) {
			Assumptions.assumeTrue(false, "Filesystem does not support symbolic links");
		} catch (IOException _) {
			Assumptions.assumeTrue(false, "Symbolic link creation is not permitted");
		}
		HttpArchiveDownloader downloader = downloader(1024);

		assertThatExceptionOfType(StagingStorageException.class)
				.isThrownBy(() -> downloader.download(attempt, paths))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IngestionErrorCode.STAGING_PATH_REJECTED));
	}

	@Test
	@DisplayName("Ошибка удаления временного файла публикует отдельную cleanup-метрику")
	void observesTemporaryCleanupFailure() {
		ArchiveAttempt attempt = attempt(1, "00000000000000000000000000000000");
		StagingPaths paths = new StagingLayout(tempDir).pathsFor(attempt);
		server.createContext("/archive.zip", exchange -> {
			Files.createDirectories(paths.archivePartPath());
			Files.writeString(paths.archivePartPath().resolve("locked"), "content");
			respond(exchange, 200, new byte[]{1}, true);
		});
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		IngestionMetrics metrics = new IngestionMetrics(registry);
		HttpArchiveDownloader downloader = new HttpArchiveDownloader(
				HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
				Duration.ofSeconds(5),
				1024,
				metrics,
				downloadUriResolver()
		);

		assertFailure(
				() -> downloader.download(attempt, paths),
				IngestionErrorCode.FILESYSTEM_IO_FAILURE,
				StagingStorageException.class);

		assertThat(registry.get("event_mosaic.ingestion.cleanup")
				.tag("outcome", "failed")
				.counter()
				.count()).isEqualTo(1);
	}

	private HttpArchiveDownloader downloader(long maxBytes) {
		return downloader(maxBytes, Duration.ofSeconds(5));
	}

	private HttpArchiveDownloader downloader(long maxBytes, Duration timeout) {
		return new HttpArchiveDownloader(
				HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
				timeout,
				maxBytes,
				new IngestionMetrics(new SimpleMeterRegistry()),
				downloadUriResolver()
		);
	}

	private ArchiveAttempt attempt(long expectedSize, String expectedMd5) {
		var archive = archive(
				Instant.parse("2026-07-20T12:00:00Z"),
				ArchiveType.TRANSLATION_EVENTS,
				expectedMd5,
				expectedSize
		);
		return new ArchiveAttempt(
				archive,
				UUID.randomUUID(),
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
	}

	private ArchiveDownloadUriResolver downloadUriResolver() {
		URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/archive.zip");
		return archiveName -> uri;
	}

	private static <T extends ApplicationException & IngestionFailureContract> void assertFailure(
			ThrowingAction action,
			IngestionErrorCode code,
			Class<T> exceptionType
	) {
		assertThatExceptionOfType(exceptionType)
				.isThrownBy(action::run)
				.satisfies(exception -> {
					assertThat(exception.errorCode()).isEqualTo(code);
					assertThat(exception.failure().code()).isEqualTo(code);
				});
	}

	private static void respond(HttpExchange exchange, int status, byte[] body, boolean fixedLength) throws IOException {
		exchange.sendResponseHeaders(status, fixedLength ? body.length : 0);
		exchange.getResponseBody().write(body);
		exchange.close();
	}

	private static String md5(byte[] content) {
		try {
			return HexFormat.of().formatHex(MessageDigest.getInstance("MD5").digest(content));
		} catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException(exception);
		}
	}

	@FunctionalInterface
	private interface ThrowingAction {
		void run() throws Exception;
	}
}
