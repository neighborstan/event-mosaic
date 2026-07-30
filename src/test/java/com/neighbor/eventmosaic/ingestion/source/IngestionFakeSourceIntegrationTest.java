package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.UPDATE_TIME;
import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.archiveName;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.IngestionRunService;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Интеграция полной загрузки с локальным HTTP-источником")
class IngestionFakeSourceIntegrationTest {

	@TempDir
	Path tempDir;

	@Autowired
	private IngestionArchiveLedger ledger;

	@Autowired
	private JdbcClient jdbcClient;

	private HttpServer server;
	private HttpClient httpClient;

	@BeforeEach
	void setUp() throws IOException {
		jdbcClient.sql("""
				truncate table ingestion_gaps, ingestion_source_state, ingestion_archives, ingestion_runs
				restart identity cascade
				""").update();
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.start();
		httpClient = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NEVER)
				.build();
	}

	@AfterEach
	void tearDown() {
		try {
			if (server != null) {
				server.stop(0);
			}
		} finally {
			if (httpClient != null) {
				httpClient.close();
			}
		}
	}

	@Test
	@DisplayName("Реальные client, parser, downloader и stager повторно используют опубликованные файлы")
	void stagesCompleteManifestAndDoesNotDownloadArchivesTwice() throws IOException {
		String eventArchiveName = archiveName(UPDATE_TIME, ArchiveType.TRANSLATION_EVENTS);
		String mentionArchiveName = archiveName(UPDATE_TIME, ArchiveType.TRANSLATION_MENTIONS);
		byte[] eventZip = zip(eventArchiveName.replaceFirst("\\.zip$", ""), "event-row\n");
		byte[] mentionZip = zip(mentionArchiveName.replaceFirst("\\.zip$", ""), "mention-row\n");
		String manifest = manifestLine(eventArchiveName, eventZip)
				+ manifestLine(mentionArchiveName, mentionZip);
		AtomicInteger manifestRequests = new AtomicInteger();
		AtomicInteger eventRequests = new AtomicInteger();
		AtomicInteger mentionRequests = new AtomicInteger();
		server.createContext("/manifest", exchange -> respond(exchange, manifest.getBytes(StandardCharsets.UTF_8), manifestRequests));
		server.createContext("/objects/" + eventArchiveName, exchange -> respond(exchange, eventZip, eventRequests));
		server.createContext("/objects/" + mentionArchiveName, exchange -> respond(exchange, mentionZip, mentionRequests));

		URI serverRoot = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		GdeltManifestClient manifestClient = new GdeltManifestClient(
				httpClient,
				serverRoot.resolve("/manifest"),
				Duration.ofSeconds(5),
				65536
		);
		GdeltManifestParser parser = new GdeltManifestParser();
		GdeltIngestionProperties properties = properties();
		IngestionMetrics metrics = new IngestionMetrics(new SimpleMeterRegistry());
		ArchiveDownloadUriResolver downloadUriResolver = archiveName ->
				serverRoot.resolve("/objects/" + archiveName.value());
		IngestionRunService service = new IngestionRunService(
				manifestClient,
				parser,
				ledger,
				new StagingLayout(properties),
				new HttpArchiveDownloader(httpClient, properties, metrics, downloadUriResolver),
				new ZipArchiveStager(properties, metrics),
				properties,
				metrics
		);

		var first = service.runLatestUpdate();
		var repeated = service.runLatestUpdate();

		assertThat(first.status()).isEqualTo(IngestionRunStatus.STAGED);
		assertThat(repeated.status()).isEqualTo(IngestionRunStatus.STAGED);
		assertThat(first.archives()).allSatisfy(archive -> {
			assertThat(archive.stagedArchive().archivePath()).isRegularFile();
			assertThat(archive.stagedArchive().csvPath()).isRegularFile();
		});
		assertThat(manifestRequests).hasValue(2);
		assertThat(eventRequests).hasValue(1);
		assertThat(mentionRequests).hasValue(1);
	}

	private GdeltIngestionProperties properties() {
		return GdeltTestFixtures.properties(tempDir, 1024 * 1024);
	}

	private static String manifestLine(String archiveName, byte[] content) {
		return content.length + " " + md5(content)
				+ " http://data.gdeltproject.org/gdeltv2/" + archiveName + "\n";
	}

	private static byte[] zip(String entryName, String content) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
			zip.putNextEntry(new ZipEntry(entryName));
			zip.write(content.getBytes(StandardCharsets.UTF_8));
			zip.closeEntry();
		}
		return bytes.toByteArray();
	}

	private static void respond(HttpExchange exchange, byte[] body, AtomicInteger requests) throws IOException {
		requests.incrementAndGet();
		exchange.sendResponseHeaders(200, body.length);
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
}
