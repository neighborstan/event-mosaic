package com.neighbor.eventmosaic.ingestion.staging;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.archive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.error.ArchiveContentViolationException;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Подготовка ZIP-архивов")
class ZipArchiveStagerTest {

	@TempDir
	Path tempDir;

	@AfterEach
	void clearInterruptFlag() {
		Thread.interrupted();
	}

	@Test
	@DisplayName("Установленный interrupt flag отменяет staging до filesystem operations")
	void interruptFlagStopsBeforeFilesystemOperations() {
		Fixture fixture = fixture();
		DownloadedArchive downloaded = new DownloadedArchive(
				fixture.paths().archivePath(),
				1,
				fixture.attempt().archive().expectedMd5(),
				false);
		Thread.currentThread().interrupt();
		ZipArchiveStager stager = stager(4, 1024, 1024);
		ArchiveAttempt attempt = fixture.attempt();
		StagingPaths paths = fixture.paths();

		assertThatExceptionOfType(IngestionInterruptedException.class)
				.isThrownBy(() -> stager.stage(attempt, downloaded, paths))
				.satisfies(exception -> {
					assertThat(exception).hasNoCause();
					assertThat(Thread.currentThread().isInterrupted()).isTrue();
				});
		assertThat(fixture.paths().csvPartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Исчерпанный cycle budget запрещает начало ZIP staging")
	void expiredCycleBudgetPreventsZipStaging() {
		Fixture fixture = fixture();
		DownloadedArchive downloaded = new DownloadedArchive(
				fixture.paths().archivePath(),
				1,
				fixture.attempt().archive().expectedMd5(),
				false);
		AtomicLong monotonicNanos = new AtomicLong();
		OperationBudget budget = OperationBudget.start(
				Duration.ofNanos(1),
				monotonicNanos::get);
		monotonicNanos.incrementAndGet();

		assertThatExceptionOfType(OperationDeadlineExceededException.class)
				.isThrownBy(() -> stager(4, 1024, 1024).stage(
						fixture.attempt(),
						downloaded,
						fixture.paths(),
						budget));

		assertThat(fixture.paths().csvPath().getParent()).doesNotExist();
	}

	@Test
	@DisplayName("Ожидаемый CSV извлекается, публикуется и повторно используется")
	void extractsExpectedCsvAndReusesPublishedArtifact() throws IOException {
		Fixture fixture = fixture();
		writeZip(fixture.paths().archivePath(), List.of(new Entry(fixture.expectedCsvName(), "row\n")));
		DownloadedArchive downloaded = downloaded(fixture.paths().archivePath());
		ZipArchiveStager stager = stager(4, 1024, 1024);

		var first = stager.stage(fixture.attempt(), downloaded, fixture.paths());
		Files.writeString(fixture.paths().csvPartPath(), "stale current attempt");
		var reused = stager.stage(fixture.attempt(), downloaded, fixture.paths());

		assertThat(Files.readString(first.csvPath())).isEqualTo("row\n");
		assertThat(reused.csvPath()).isEqualTo(first.csvPath());
		assertThat(fixture.paths().csvPartPath()).doesNotExist();

		Files.writeString(first.csvPath(), "corrupted");
		assertFailure(fixture, stager, StagingStorageException.class,
				IngestionErrorCode.STAGING_ARTIFACT_CONFLICT);
	}

	@Test
	@DisplayName("Пустой архив и архив с лишней записью отклоняются")
	void rejectsEmptyAndExtraEntryArchives() throws IOException {
		Fixture empty = fixture(tempDir.resolve("empty"));
		writeZip(empty.paths().archivePath(), List.of());
		assertFailure(empty, stager(4, 1024, 1024), ArchiveContentViolationException.class,
				IngestionErrorCode.ZIP_CONTENT_MISMATCH);

		Fixture extra = fixture(tempDir.resolve("extra"));
		writeZip(extra.paths().archivePath(), List.of(
				new Entry(extra.expectedCsvName(), "row"),
				new Entry("extra.txt", "extra")
		));
		assertFailure(extra, stager(4, 1024, 2048), ArchiveContentViolationException.class,
				IngestionErrorCode.ZIP_CONTENT_MISMATCH);
		assertThat(extra.paths().csvPartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Rebuild повторно проверяет неизменные ZIP и CSV без записи")
	void verifiesUnchangedReplayArtifacts() throws IOException {
		Fixture fixture = fixture(tempDir.resolve("replay-valid"));
		writeZip(fixture.paths().archivePath(), List.of(
				new Entry(fixture.expectedCsvName(), "row\n")));
		ZipArchiveStager stager = stager(4, 1024, 1024);
		StagedArchive staged = stager.stage(
				fixture.attempt(),
				downloaded(fixture.paths().archivePath()),
				fixture.paths());
		IngestionArchiveState state = stagedState(fixture, staged);

		assertThatCode(() -> stager.verifyReplaySource(
				state,
				OperationBudget.start(Duration.ofSeconds(5))))
				.doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Rebuild отклоняет измененный verified ZIP или производный CSV")
	void rejectsChangedReplayArtifacts() throws IOException {
		Fixture zipFixture = fixture(tempDir.resolve("replay-zip-corrupt"));
		writeZip(zipFixture.paths().archivePath(), List.of(
				new Entry(zipFixture.expectedCsvName(), "row\n")));
		ZipArchiveStager stager = stager(4, 1024, 1024);
		StagedArchive zipStaged = stager.stage(
				zipFixture.attempt(),
				downloaded(zipFixture.paths().archivePath()),
				zipFixture.paths());
		IngestionArchiveState zipState = stagedState(zipFixture, zipStaged);
		Files.writeString(zipStaged.archivePath(), "corrupted ZIP");

		assertThatExceptionOfType(StagingStorageException.class)
				.isThrownBy(() -> stager.verifyReplaySource(
						zipState,
						OperationBudget.start(Duration.ofSeconds(5))))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT));

		Fixture csvFixture = fixture(tempDir.resolve("replay-csv-corrupt"));
		writeZip(csvFixture.paths().archivePath(), List.of(
				new Entry(csvFixture.expectedCsvName(), "row\n")));
		StagedArchive csvStaged = stager.stage(
				csvFixture.attempt(),
				downloaded(csvFixture.paths().archivePath()),
				csvFixture.paths());
		IngestionArchiveState csvState = stagedState(csvFixture, csvStaged);
		Files.writeString(csvStaged.csvPath(), "corrupted CSV");

		assertThatExceptionOfType(StagingStorageException.class)
				.isThrownBy(() -> stager.verifyReplaySource(
						csvState,
						OperationBudget.start(Duration.ofSeconds(5))))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT));
	}

	@Test
	@DisplayName("Zip Slip отклоняется без записи за пределами staging")
	void rejectsZipSlipWithoutWritingOutsideStaging() throws IOException {
		Fixture fixture = fixture(tempDir.resolve("slip"));
		writeZip(fixture.paths().archivePath(), List.of(new Entry("../outside.csv", "escape")));
		Path outside = fixture.paths().csvPath().getParent().resolve("../outside.csv").normalize();

		assertFailure(fixture, stager(4, 1024, 1024), ArchiveContentViolationException.class,
				IngestionErrorCode.ZIP_PATH_REJECTED);
		assertThat(outside).doesNotExist();
	}

	@Test
	@DisplayName("Распаковка прекращается при превышении допустимого размера")
	void stopsWhenExpansionLimitIsExceeded() throws IOException {
		Fixture fixture = fixture(tempDir.resolve("limit"));
		writeZip(fixture.paths().archivePath(), List.of(new Entry(fixture.expectedCsvName(), "12345")));

		assertFailure(fixture, stager(4, 4, 4), ArchiveContentViolationException.class,
				IngestionErrorCode.ZIP_LIMIT_EXCEEDED);
		assertThat(fixture.paths().csvPath()).doesNotExist();
		assertThat(fixture.paths().csvPartPath()).doesNotExist();
	}

	@Test
	@DisplayName("Ошибка удаления временной распаковки публикует отдельную cleanup-метрику")
	void observesTemporaryExtractionCleanupFailure() throws IOException {
		Fixture fixture = fixture(tempDir.resolve("cleanup"));
		writeZip(fixture.paths().archivePath(), List.of(new Entry(fixture.expectedCsvName(), "row")));
		Files.createDirectories(fixture.paths().csvPartPath());
		Files.writeString(fixture.paths().csvPartPath().resolve("locked"), "content");
		SimpleMeterRegistry registry = new SimpleMeterRegistry();
		IngestionMetrics metrics = new IngestionMetrics(registry);
		ZipArchiveStager stager = new ZipArchiveStager(4, 1024, 1024, metrics);

		assertFailure(fixture, stager, StagingStorageException.class,
				IngestionErrorCode.FILESYSTEM_IO_FAILURE);

		assertThat(registry.get("event_mosaic.ingestion.cleanup")
				.tag("outcome", "failed")
				.counter()
				.count()).isEqualTo(1);
	}

	private Fixture fixture() {
		return fixture(tempDir);
	}

	private Fixture fixture(Path root) {
		DiscoveredArchive archive = archive(
				Instant.parse("2026-07-20T12:00:00Z"),
				ArchiveType.TRANSLATION_EVENTS,
				"0123456789abcdef0123456789abcdef",
				1
		);
		String archiveName = archive.archiveName();
		ArchiveAttempt attempt = new ArchiveAttempt(
				archive,
				UUID.randomUUID(),
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false
		);
		StagingPaths paths = new StagingLayout(root).pathsFor(attempt);
		return new Fixture(attempt, paths, archiveName.substring(0, archiveName.length() - 4));
	}

	private static DownloadedArchive downloaded(Path path) throws IOException {
		return new DownloadedArchive(path, Files.size(path), Md5Checksum.calculate(path), false);
	}

	private static IngestionArchiveState stagedState(
			Fixture fixture,
			StagedArchive staged
	) {
		DiscoveredArchive verifiedArchive = archive(
				fixture.attempt().archive().sourceUpdateTime(),
				fixture.attempt().archive().archiveType(),
				staged.actualMd5(),
				staged.actualSizeBytes());
		Instant completedAt = fixture.attempt().leaseExpiresAt().minusSeconds(1);
		return new IngestionArchiveState(
				1,
				verifiedArchive,
				IngestionArchiveStatus.STAGED,
				new ArchiveAttemptState(
						1,
						null,
						completedAt,
						null,
						AutomaticRetryState.initial(3)),
				staged,
				null,
				completedAt,
				completedAt);
	}

	private static <T extends ApplicationException & IngestionFailureContract> void assertFailure(
			Fixture fixture,
			ZipArchiveStager stager,
			Class<T> exceptionType,
			IngestionErrorCode code
	) throws IOException {
		DownloadedArchive downloaded = downloaded(fixture.paths().archivePath());
		assertThatExceptionOfType(exceptionType)
				.isThrownBy(() -> stager.stage(fixture.attempt(), downloaded, fixture.paths()))
				.satisfies(exception -> assertThat(exception.errorCode()).isEqualTo(code));
	}

	private static ZipArchiveStager stager(int maxEntries, long maxEntryBytes, long maxTotalBytes) {
		return new ZipArchiveStager(
				maxEntries,
				maxEntryBytes,
				maxTotalBytes,
				new IngestionMetrics(new SimpleMeterRegistry()));
	}

	private static void writeZip(Path path, List<Entry> entries) throws IOException {
		Files.createDirectories(path.getParent());
		try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(path))) {
			for (Entry entry : entries) {
				zip.putNextEntry(new ZipEntry(entry.name()));
				zip.write(entry.content().getBytes(StandardCharsets.UTF_8));
				zip.closeEntry();
			}
		}
	}

	private record Fixture(ArchiveAttempt attempt, StagingPaths paths, String expectedCsvName) {
	}

	private record Entry(String name, String content) {
	}
}
