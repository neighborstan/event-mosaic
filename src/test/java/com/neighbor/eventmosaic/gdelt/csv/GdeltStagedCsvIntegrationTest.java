package com.neighbor.eventmosaic.gdelt.csv;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.staging.DownloadedArchive;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Интеграция staging и потокового GDELT CSV reader")
class GdeltStagedCsvIntegrationTest {

	@TempDir
	Path tempDirectory;

	private SimpleMeterRegistry meterRegistry;
	private GdeltIngestionProperties properties;
	private StagingLayout stagingLayout;
	private ZipArchiveStager archiveStager;
	private DefaultGdeltEventCsvReader eventReader;
	private DefaultGdeltMentionCsvReader mentionReader;

	@BeforeEach
	void setUp() {
		meterRegistry = new SimpleMeterRegistry();
		properties = GdeltTestFixtures.properties(tempDirectory.resolve("staging"), 1024 * 1024);
		stagingLayout = new StagingLayout(properties);
		archiveStager = new ZipArchiveStager(properties, new IngestionMetrics(meterRegistry));
		GdeltCsvMetrics csvMetrics = new GdeltCsvMetrics(meterRegistry);
		GdeltCsvProperties csvProperties = new GdeltCsvProperties(1_048_576);
		eventReader = new DefaultGdeltEventCsvReader(csvProperties, csvMetrics);
		mentionReader = new DefaultGdeltMentionCsvReader(csvProperties, csvMetrics);
	}

	@AfterEach
	void closeRegistry() {
		meterRegistry.close();
	}

	@Test
	@DisplayName("Публикует Event ZIP и передает staged CSV reader без ledger transition")
	void stagesAndReadsEventArchive() throws Exception {
		StagedArchive staged = stage(ArchiveType.TRANSLATION_EVENTS, resource("event-valid.csv"));
		List<GdeltEvent> events = new ArrayList<>();

		var summary = eventReader.read(staged.csvPath(), csvRecord -> events.add(csvRecord.value()));

		assertThat(staged.csvPath()).isRegularFile();
		assertThat(summary.validRecords()).isEqualTo(2);
		assertThat(events).extracting(GdeltEvent::globalEventId)
				.containsExactly(1_314_602_221L, 1_314_602_222L);
	}

	@Test
	@DisplayName("Публикует Mention ZIP и сохраняет физический Extras в staged CSV")
	void stagesAndReadsMentionArchive() throws Exception {
		StagedArchive staged = stage(ArchiveType.TRANSLATION_MENTIONS, resource("mention-valid.csv"));
		List<GdeltMention> mentions = new ArrayList<>();

		var summary = mentionReader.read(staged.csvPath(), csvRecord -> mentions.add(csvRecord.value()));

		assertThat(staged.csvPath()).isRegularFile();
		assertThat(summary.validRecords()).isEqualTo(2);
		assertThat(mentions).extracting(GdeltMention::extras)
				.containsExactly("", "future:opaque value");
	}

	private StagedArchive stage(ArchiveType type, Path csvFixture) throws Exception {
		String archiveName = GdeltTestFixtures.archiveName(GdeltTestFixtures.UPDATE_TIME, type);
		String csvName = GdeltArchiveName.requireSupported(archiveName).csvName();
		Path archivePath = createZip(type.name().toLowerCase() + ".zip", csvName, csvFixture);
		long size = Files.size(archivePath);
		String md5 = md5(archivePath);
		var discovered = GdeltTestFixtures.archive(
				GdeltTestFixtures.UPDATE_TIME,
				type,
				md5,
				size);
		var attempt = new ArchiveAttempt(
				discovered,
				UUID.randomUUID(),
				GdeltTestFixtures.UPDATE_TIME.plusSeconds(900),
				1,
				false);
		return archiveStager.stage(
				attempt,
				new DownloadedArchive(archivePath, size, md5, false),
				stagingLayout.pathsFor(attempt));
	}

	private Path createZip(String fileName, String entryName, Path content) throws IOException {
		Path archive = tempDirectory.resolve(fileName);
		try (ZipOutputStream output = new ZipOutputStream(Files.newOutputStream(archive))) {
			output.putNextEntry(new ZipEntry(entryName));
			Files.copy(content, output);
			output.closeEntry();
		}
		return archive;
	}

	private Path resource(String name) throws URISyntaxException {
		return Path.of(Objects.requireNonNull(
				getClass().getResource("/gdelt/csv/" + name),
				"Missing test resource " + name).toURI());
	}

	private static String md5(Path path) throws IOException, NoSuchAlgorithmException {
		MessageDigest digest = MessageDigest.getInstance("MD5");
		byte[] buffer = new byte[8192];
		try (InputStream input = Files.newInputStream(path)) {
			int read;
			while ((read = input.read(buffer)) != -1) {
				digest.update(buffer, 0, read);
			}
		}
		return HexFormat.of().formatHex(digest.digest());
	}
}
