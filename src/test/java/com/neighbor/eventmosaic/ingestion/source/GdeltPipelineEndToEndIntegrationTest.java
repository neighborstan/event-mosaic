package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.ingestion.GdeltPipelineService;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.IngestionRunService;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
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
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
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
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;

@Import(TestcontainersConfiguration.class)
@AutoConfigureMockMvc
@SpringBootTest
@DisplayName("Сквозной GDELT walking skeleton")
class GdeltPipelineEndToEndIntegrationTest {

	private static final Instant UPDATE_TIME = Instant.parse("2026-07-21T14:45:00Z");
	private static final long EVENT_ID = 1_314_602_221L;

	@TempDir
	Path tempDir;

	@Autowired
	private IngestionArchiveLedger ingestionLedger;

	@Autowired
	private ArchiveProcessingLedger processingLedger;

	@Autowired
	private GdeltArchiveProcessor archiveProcessor;

	@Autowired
	private ProcessingFingerprintFactory fingerprintFactory;

	@Autowired
	private GdeltIndexWriter indexWriter;

	@Autowired
	private ElasticsearchClient elasticsearchClient;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private MockMvc mockMvc;

	private HttpServer server;
	private HttpClient httpClient;

	@BeforeEach
	void setUp() throws IOException {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
		elasticsearchClient.indices().delete(request -> request
				.index(
						GdeltIndexKind.EVENT.indexName(),
						GdeltIndexKind.MENTION.indexName())
				.allowNoIndices(true)
				.ignoreUnavailable(true));
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
	@DisplayName("Проводит реальный compact update до API, skip и восстановления индекса")
	void processesRealUpdateAndRecoversDeletedIndex() throws Exception {
		SourceRequests requests = registerUpdateSource(
				resourceBytes("/gdelt/csv/event-valid.csv"),
				resourceBytes("/gdelt/csv/mention-valid.csv"));
		GdeltPipelineService pipeline = pipelineService();

		IngestionRunState first = pipeline.runLatestUpdate();

		assertThat(first.status()).isEqualTo(IngestionRunStatus.STAGED);
		assertIndexed(first, ArchiveType.TRANSLATION_EVENTS, 1);
		assertIndexed(first, ArchiveType.TRANSLATION_MENTIONS, 1);
		assertDocumentCounts(2, 2);
		assertApiResponse();

		IngestionRunState repeated = pipeline.runLatestUpdate();

		assertIndexed(repeated, ArchiveType.TRANSLATION_EVENTS, 1);
		assertIndexed(repeated, ArchiveType.TRANSLATION_MENTIONS, 1);
		assertDocumentCounts(2, 2);
		assertThat(requests.event()).hasValue(1);
		assertThat(requests.mention()).hasValue(1);

		elasticsearchClient.indices().delete(
				request -> request.index(GdeltIndexKind.EVENT.indexName()));

		IngestionRunState recovered = pipeline.runLatestUpdate();

		assertIndexed(recovered, ArchiveType.TRANSLATION_EVENTS, 2);
		assertIndexed(recovered, ArchiveType.TRANSLATION_MENTIONS, 1);
		assertDocumentCounts(2, 2);
		assertApiResponse();
		assertThat(requests.manifest()).hasValue(3);
		assertThat(requests.event()).hasValue(1);
		assertThat(requests.mention()).hasValue(1);
	}

	@Test
	@DisplayName("Нулевой receipt после отклонения всех строк остаётся terminal при повторе")
	void keepsAllRejectedArchivesIndexedOnRepeat() throws Exception {
		SourceRequests requests = registerUpdateSource(
				withBlankColumn(resourceBytes("/gdelt/csv/event-valid.csv"), 1),
				withBlankColumn(resourceBytes("/gdelt/csv/mention-valid.csv"), 2));
		GdeltPipelineService pipeline = pipelineService();

		IngestionRunState first = pipeline.runLatestUpdate();
		assertIndexed(first, ArchiveType.TRANSLATION_EVENTS, 1, 0);
		assertIndexed(first, ArchiveType.TRANSLATION_MENTIONS, 1, 0);

		IngestionRunState repeated = pipeline.runLatestUpdate();

		ArchiveProcessingState eventState = assertIndexed(
				repeated,
				ArchiveType.TRANSLATION_EVENTS,
				1,
				0);
		ArchiveProcessingState mentionState = assertIndexed(
				repeated,
				ArchiveType.TRANSLATION_MENTIONS,
				1,
				0);
		assertThat(eventState.progress().mappingRejectedRecords()).isEqualTo(2);
		assertThat(mentionState.progress().mappingRejectedRecords()).isEqualTo(2);
		assertDocumentCounts(0, 0);
		assertThat(requests.manifest()).hasValue(2);
		assertThat(requests.event()).hasValue(1);
		assertThat(requests.mention()).hasValue(1);
	}

	@Test
	@DisplayName("Лишний receipt останавливает автоматический replay без удаления данных")
	void recordsReceiptSurplusAsNonRetryable() throws Exception {
		SourceRequests requests = registerUpdateSource(
				resourceBytes("/gdelt/csv/event-valid.csv"),
				resourceBytes("/gdelt/csv/mention-valid.csv"));
		GdeltPipelineService pipeline = pipelineService();
		IngestionRunState first = pipeline.runLatestUpdate();
		String eventArchiveKey = archiveKey(first, ArchiveType.TRANSLATION_EVENTS);
		indexStaleEvent(eventArchiveKey);

		IngestionRunState detected = pipeline.runLatestUpdate();
		ArchiveProcessingState failed = processingState(
				detected,
				ArchiveType.TRANSLATION_EVENTS);

		assertThat(failed.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
		assertThat(failed.attempt().count()).isEqualTo(1);
		assertThat(failed.failure().failure().errorCode())
				.isEqualTo("INDEX_RECEIPT_SURPLUS");
		assertThat(failed.failure().failure().retryable()).isFalse();
		assertIndexed(detected, ArchiveType.TRANSLATION_MENTIONS, 1);
		assertDocumentCounts(3, 2);

		IngestionRunState repeated = pipeline.runLatestUpdate();

		ArchiveProcessingState unchanged = processingState(
				repeated,
				ArchiveType.TRANSLATION_EVENTS);
		assertThat(unchanged.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
		assertThat(unchanged.attempt().count()).isEqualTo(1);
		assertDocumentCounts(3, 2);
		assertThat(requests.manifest()).hasValue(3);
		assertThat(requests.event()).hasValue(1);
		assertThat(requests.mention()).hasValue(1);
	}

	private GdeltPipelineService pipelineService() {
		GdeltIngestionProperties properties =
				GdeltTestFixtures.properties(tempDir.resolve("staging"), 1024 * 1024);
		URI serverRoot = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		IngestionMetrics metrics = new IngestionMetrics(meterRegistry);
		ArchiveDownloadUriResolver resolver = archiveName ->
				serverRoot.resolve("/objects/" + archiveName.value());
		IngestionRunService acquisition = new IngestionRunService(
				new GdeltManifestClient(
						httpClient,
						serverRoot.resolve("/manifest"),
						Duration.ofSeconds(5),
						65_536),
				new GdeltManifestParser(),
				ingestionLedger,
				new StagingLayout(properties),
				new HttpArchiveDownloader(httpClient, properties, metrics, resolver),
				new ZipArchiveStager(properties, metrics),
				properties,
				metrics);
		return new GdeltPipelineService(
				acquisition,
				processingLedger,
				archiveProcessor,
				fingerprintFactory,
				indexWriter,
				properties);
	}

	private void registerSource(
			String eventArchiveName,
			String mentionArchiveName,
			byte[] eventZip,
			byte[] mentionZip,
			String manifest,
			SourceRequests requests
	) {
		server.createContext(
				"/manifest",
				exchange -> respond(
						exchange,
						manifest.getBytes(StandardCharsets.UTF_8),
						requests.manifest()));
		server.createContext(
				"/objects/" + eventArchiveName,
				exchange -> respond(exchange, eventZip, requests.event()));
		server.createContext(
				"/objects/" + mentionArchiveName,
				exchange -> respond(exchange, mentionZip, requests.mention()));
	}

	private SourceRequests registerUpdateSource(
			byte[] eventCsv,
			byte[] mentionCsv
	) throws IOException {
		String eventArchiveName =
				GdeltTestFixtures.archiveName(UPDATE_TIME, ArchiveType.TRANSLATION_EVENTS);
		String mentionArchiveName =
				GdeltTestFixtures.archiveName(UPDATE_TIME, ArchiveType.TRANSLATION_MENTIONS);
		byte[] eventZip = zip(
				eventArchiveName.replaceFirst("\\.zip$", ""),
				eventCsv);
		byte[] mentionZip = zip(
				mentionArchiveName.replaceFirst("\\.zip$", ""),
				mentionCsv);
		String manifest = manifestLine(eventArchiveName, eventZip)
				+ manifestLine(mentionArchiveName, mentionZip);
		SourceRequests requests = new SourceRequests(
				new AtomicInteger(),
				new AtomicInteger(),
				new AtomicInteger());
		registerSource(
				eventArchiveName,
				mentionArchiveName,
				eventZip,
				mentionZip,
				manifest,
				requests);
		return requests;
	}

	private ArchiveProcessingState assertIndexed(
			IngestionRunState runState,
			ArchiveType archiveType,
			int expectedAttemptCount
	) {
		return assertIndexed(runState, archiveType, expectedAttemptCount, 2);
	}

	private ArchiveProcessingState assertIndexed(
			IngestionRunState runState,
			ArchiveType archiveType,
			int expectedAttemptCount,
			long expectedReceiptDocuments
	) {
		ArchiveProcessingState state = processingState(runState, archiveType);

		assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.INDEXED);
		assertThat(state.attempt().count()).isEqualTo(expectedAttemptCount);
		assertThat(state.progress().receiptDocuments()).isEqualTo(expectedReceiptDocuments);
		return state;
	}

	private ArchiveProcessingState processingState(
			IngestionRunState runState,
			ArchiveType archiveType
	) {
		return processingLedger
				.findByArchiveIdempotencyKey(archiveKey(runState, archiveType))
				.orElseThrow();
	}

	private static String archiveKey(
			IngestionRunState runState,
			ArchiveType archiveType
	) {
		return runState.archives().stream()
				.filter(state -> state.archive().archiveType() == archiveType)
				.findFirst()
				.orElseThrow()
				.archive()
				.idempotencyKey();
	}

	private void assertDocumentCounts(long events, long mentions) throws IOException {
		assertThat(elasticsearchClient.count(
				request -> request.index(GdeltIndexKind.EVENT.indexName())).count())
				.isEqualTo(events);
		assertThat(elasticsearchClient.count(
				request -> request.index(GdeltIndexKind.MENTION.indexName())).count())
				.isEqualTo(mentions);
	}

	private void indexStaleEvent(String sourceArchiveKey) throws IOException {
		elasticsearchClient.index(request -> request
				.index(GdeltIndexKind.EVENT.indexName())
				.id("stale-event")
				.refresh(Refresh.WaitFor)
				.document(Map.of(
						"globalEventId", 9_999_999_999L,
						"eventDay", "2026-07-30",
						"dateAdded", "2026-07-30T10:00:00Z",
						"sourceUpdateTime", "2026-07-30T10:15:00Z",
						"sourceArchiveKey", sourceArchiveKey,
						"sourceLineNumber", 999)));
	}

	private void assertApiResponse() throws Exception {
		mockMvc.perform(get("/api/v1/events/{eventId}", EVENT_ID))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.event.eventId").value(EVENT_ID))
				.andExpect(jsonPath("$.event.eventDate").value("2025-07-21"))
				.andExpect(jsonPath("$.actors.actor1.code").value("AFR"))
				.andExpect(jsonPath("$.classification.eventCode").value("062"))
				.andExpect(jsonPath("$.location.role").value("ACTION"))
				.andExpect(jsonPath("$.sources.length()").value(1))
				.andExpect(jsonPath("$.sources[0].sourceName")
						.value("eldiariony.com"))
				.andExpect(jsonPath("$.sources[0].identifier")
						.value("https://eldiariony.com/2026/07/21/"
								+ "reporte-del-departamento-de-estado-de-ee-uu-sobre-"
								+ "comunismo-en-cuba-menciona-a-independentistas-de-p-r-"
								+ "como-filiberto-ojeda/"));
	}

	private static byte[] resourceBytes(String name) throws IOException {
		try (var input = GdeltPipelineEndToEndIntegrationTest.class
				.getResourceAsStream(name)) {
			return Objects.requireNonNull(input, "Missing test resource " + name)
					.readAllBytes();
		}
	}

	private static byte[] withBlankColumn(byte[] content, int columnIndex) {
		String[] lines = new String(content, StandardCharsets.UTF_8).split("\\R", -1);
		StringBuilder transformed = new StringBuilder(content.length);
		for (String line : lines) {
			if (line.isEmpty()) {
				continue;
			}
			String[] fields = line.split("\\t", -1);
			if (columnIndex < 0 || columnIndex >= fields.length) {
				throw new IllegalArgumentException("columnIndex is outside CSV record");
			}
			fields[columnIndex] = "";
			transformed.append(String.join("\t", fields)).append('\n');
		}
		return transformed.toString().getBytes(StandardCharsets.UTF_8);
	}

	private static String manifestLine(String archiveName, byte[] content) {
		return content.length + " " + md5(content)
				+ " http://data.gdeltproject.org/gdeltv2/"
				+ archiveName
				+ "\n";
	}

	private static byte[] zip(String entryName, byte[] content) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
			zip.putNextEntry(new ZipEntry(entryName));
			zip.write(content);
			zip.closeEntry();
		}
		return bytes.toByteArray();
	}

	private static void respond(
			HttpExchange exchange,
			byte[] body,
			AtomicInteger requests
	) throws IOException {
		requests.incrementAndGet();
		exchange.sendResponseHeaders(200, body.length);
		try (var output = exchange.getResponseBody()) {
			output.write(body);
		} finally {
			exchange.close();
		}
	}

	private static String md5(byte[] content) {
		try {
			return HexFormat.of().formatHex(
					MessageDigest.getInstance("MD5").digest(content));
		} catch (NoSuchAlgorithmException exception) {
			throw new IllegalStateException(exception);
		}
	}

	private record SourceRequests(
			AtomicInteger manifest,
			AtomicInteger event,
			AtomicInteger mention
	) {
	}
}
