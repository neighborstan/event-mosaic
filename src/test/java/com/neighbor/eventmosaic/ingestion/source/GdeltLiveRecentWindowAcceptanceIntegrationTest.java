package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.neighbor.eventmosaic.EventMosaicApplication;
import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolver;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.audit.ReceiptAuditService;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.GdeltPipelineService;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.IngestionMetrics;
import com.neighbor.eventmosaic.ingestion.IngestionOneShotOutcome;
import com.neighbor.eventmosaic.ingestion.IngestionRunService;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlanner;
import com.neighbor.eventmosaic.ingestion.retry.RetryJitterSource;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.EnumMap;
import java.util.function.BooleanSupplier;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.JsonNode;

@Import({
		TestcontainersConfiguration.class,
		GdeltLiveRecentWindowAcceptanceIntegrationTest.AcceptanceTimeConfiguration.class
})
@SpringBootTest
@DisplayName("Приемка восстановления суточного GDELT recent-окна")
class GdeltLiveRecentWindowAcceptanceIntegrationTest {

	@Test
	@DisplayName("Обычный web-запуск сам заполняет карту, получает следующее обновление и сохраняет данные после перезапуска")
	void automaticWebStartupCompletesMapAndResumesWithoutDuplicateWork() throws Exception {
		source.close();
		var slots = new ArrayList<>(WINDOW_SLOTS);
		slots.add(WINDOW_TO);
		source = new FakeGdeltSource(slots, trace);
		source.frontier.set(FRONTIER);
		source.useCoherentMaster();
		var manifestEntered = new CountDownLatch(1);
		var releaseManifest = new CountDownLatch(1);
		var historicalEntered = new CountDownLatch(1);
		var releaseHistorical = new CountDownLatch(1);
		source.beforeManifest = () -> awaitRelease(manifestEntered, releaseManifest);
		source.beforeArchive = name -> {
			if (name.equals(source.artifact(FIRST_HISTORICAL,
					ArchiveType.TRANSLATION_EVENTS).archiveName())) {
				awaitRelease(historicalEntered, releaseHistorical);
			}
		};
		try (var runtime = automaticRuntime()) {
			assertThat(manifestEntered.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(runtime.getEnvironment().getProperty(
					"event-mosaic.ingestion.gdelt.one-shot-enabled", Boolean.class)).isFalse();
			// На совершенно пустом Elasticsearch еще нет read alias: API честно сообщает недоступность.
			assertThat(snapshotResponse(runtime).statusCode()).isEqualTo(503);
			assertThat(runCount()).isZero();
			releaseManifest.countDown();
			assertThat(historicalEntered.await(30, TimeUnit.SECONDS)).isTrue();
			assertThat(snapshot(runtime).path("coverage").path("status").asString()).isEqualTo("PARTIAL");
			assertThat(source.manifestRequests()).isEqualTo(1);
			// Пока первый цикл удерживает HTTP-ответ, конкурирующий trigger не начинает I/O.
			assertThat(runtime.getBean(IngestionCycleCoordinator.class)
					.runCycle()).isEqualTo(IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);
			assertThat(source.manifestRequests()).isEqualTo(1);
			releaseHistorical.countDown();
			Awaitility.await().atMost(Duration.ofMinutes(2)).untilAsserted(() -> {
				var response = snapshot(runtime);
				assertThat(response.path("coverage").path("status").asString()).isEqualTo("COMPLETE");
				assertThat(response.path("quality").path("eligibleEventCount").asLong()).isEqualTo(96);
				assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(96);
			});
			int polls = source.manifestRequests();
			Awaitility.await().atMost(Duration.ofSeconds(10))
					.until(() -> source.manifestRequests() > polls);
			assertThat(source.objectRequests()).isEqualTo(192);
			clock.advance(SLOT);
			source.frontier.set(WINDOW_TO);
			Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
				assertThat(runCount()).isEqualTo(97);
				assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(97);
				assertThat(snapshot(runtime).path("coverage").path("status").asString()).isEqualTo("COMPLETE");
			});
		}
		finally {
			releaseManifest.countDown();
			releaseHistorical.countDown();
		}
		var hashes = finalFileHashes();
		int downloads = source.objectRequests();
		long previousEpoch = jdbcClient.sql("select fencing_epoch from ingestion_cycle_state")
				.query(Long.class).single();
		try (var restarted = automaticRuntime()) {
			Awaitility.await().atMost(Duration.ofSeconds(15))
					.until(() -> jdbcClient.sql("""
							select count(*) from ingestion_cycle_state
							where fencing_epoch > :previousEpoch and status = 'IDLE' and last_outcome = 'UNCHANGED'
							""").param("previousEpoch", previousEpoch).query(Long.class).single() == 1);
			assertThat(snapshot(restarted).path("coverage").path("status").asString()).isEqualTo("COMPLETE");
			assertThat(runCount()).isEqualTo(97);
			assertThat(documentCount(GdeltIndexKind.EVENT)).isEqualTo(97);
			assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(97);
		}
		assertThat(source.objectRequests()).isEqualTo(downloads);
		assertThat(finalFileHashes()).isEqualTo(hashes);
		assertThat(jdbcClient.sql("select status from ingestion_cycle_state").query(String.class).single())
				.isEqualTo("IDLE");
	}

	private ConfigurableApplicationContext automaticRuntime() {
		return new SpringApplicationBuilder(
				EventMosaicApplication.class)
				.initializers(application -> {
					var context = (GenericApplicationContext) application;
					context.registerBean(DataSource.class, () -> dataSource,
							definition -> definition.setDestroyMethodName(""));
					context.registerBean(ElasticsearchClient.class, () -> elasticsearchClient,
							definition -> definition.setDestroyMethodName(""));
					context.registerBean("acceptanceClock", Clock.class, () -> clock,
							definition -> definition.setPrimary(true));
					context.registerBean("localManifest", GdeltManifestClient.class,
							() -> new GdeltManifestClient(httpClient, source.rootUri().resolve("/manifest"),
									Duration.ofSeconds(30), 65_536, new HttpRetryAfterParser(clock)),
							definition -> definition.setPrimary(true));
					context.registerBean("localCatalog", GdeltTranslationMasterCatalogClient.class,
							() -> new GdeltTranslationMasterCatalogClient(httpClient,
									source.rootUri().resolve("/masterfilelist-translation.txt"),
									Duration.ofSeconds(30), new HttpRetryAfterParser(clock),
									new GdeltTranslationMasterCatalogParser(new GdeltManifestLineParser())),
							definition -> definition.setPrimary(true));
					context.registerBean("localDownload", ArchiveDownloadUriResolver.class,
							() -> name -> source.rootUri().resolve("/objects/" + name.value()),
							definition -> definition.setPrimary(true));
					context.getBeanFactory().addBeanPostProcessor(new BeanPostProcessor() {
						@Override
						public Object postProcessAfterInitialization(Object bean, String name) {
							if (bean instanceof ConcurrentTaskScheduler scheduler) {
								scheduler.setClock(clock);
							}
							return bean;
						}
					});
				})
				.run("--server.port=0", "--spring.docker.compose.enabled=false",
						"--event-mosaic.ingestion.gdelt.automatic.enabled=true",
						"--event-mosaic.ingestion.gdelt.automatic.poll-delay=100ms",
						"--event-mosaic.ingestion.gdelt.staging-root=" + tempDir.resolve("staging"));
	}

	private JsonNode snapshot(ConfigurableApplicationContext runtime)
			throws Exception {
		var response = snapshotResponse(runtime);
		assertThat(response.statusCode()).isEqualTo(200);
		return new JsonMapper().readTree(response.body());
	}

	private HttpResponse<String> snapshotResponse(
			ConfigurableApplicationContext runtime) throws Exception {
		int port = ((WebServerApplicationContext) runtime)
				.getWebServer().getPort();
		return httpClient.send(HttpRequest.newBuilder(
				URI.create("http://127.0.0.1:" + port + "/api/v1/map/country-snapshot")).timeout(Duration.ofSeconds(10)).GET().build(),
				HttpResponse.BodyHandlers.ofString());
	}

	private static void awaitRelease(CountDownLatch entered,
			CountDownLatch release) {
		entered.countDown();
		try {
			if (!release.await(30, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Acceptance response was not released");
			}
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
		}
	}

	private static final Duration SLOT = GdeltSourceContract.UPDATE_INTERVAL;
	private static final Instant INITIAL_NOW = Instant.parse("2026-07-20T13:00:00Z");
	private static final Instant WINDOW_TO = Instant.parse("2026-07-20T12:45:00Z");
	private static final Instant WINDOW_FROM = WINDOW_TO.minus(Duration.ofHours(24));
	private static final List<Instant> WINDOW_SLOTS = windowSlots();
	private static final Instant FRONTIER = WINDOW_SLOTS.getLast();
	private static final Instant FIRST_HISTORICAL = WINDOW_SLOTS.get(94);
	private static final Instant INTERRUPTED_HISTORICAL = WINDOW_SLOTS.get(93);
	private static final Instant REGISTERED_BUT_NOT_STARTED = WINDOW_SLOTS.get(92);
	private static final Instant FAILED_MENTION = WINDOW_SLOTS.get(50);
	private static final int EXPECTED_WINDOW_SLOTS = 96;

	@TempDir
	Path tempDir;

	@Autowired
	private IngestionArchiveLedger ingestionArchiveLedger;

	@Autowired
	private SourcePollLedger sourcePollLedger;

	@Autowired
	private ArchiveProcessingLedger processingLedger;

	@Autowired
	private RecentRecoveryPlanLedger recentRecoveryPlanLedger;

	@Autowired
	private RecentWindowPlanner recentWindowPlanner;

	@Autowired
	private GdeltArchiveProcessor realArchiveProcessor;

	@Autowired
	private ProcessingFingerprintFactory fingerprintFactory;

	@Autowired
	private GdeltIndexWriter indexWriter;

	@Autowired
	private IndexTargetResolver indexTargetResolver;

	@Autowired
	private IngestionCoverageQuery coverageQuery;

	@Autowired
	private ElasticsearchClient elasticsearchClient;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private MeterRegistry meterRegistry;

	@Autowired
	private BackendDataStorageMonitor storageMonitor;

	@Autowired
	private AcceptanceClock clock;

	@Autowired
	private DataSource dataSource;

	private final List<String> trace = new CopyOnWriteArrayList<>();
	private HttpClient httpClient;
	private FakeGdeltSource source;

	@BeforeEach
	void setUp() throws Exception {
		clock.reset();
		Thread.interrupted();
		var previousIndices = jdbcClient.sql("""
				select event_index_name from index_generations
				union select mention_index_name from index_generations
				""").query(String.class).list();
		if (!previousIndices.isEmpty()) {
			elasticsearchClient.indices().delete(request -> request.index(previousIndices).ignoreUnavailable(true));
		}
		jdbcClient.sql("""
				truncate table
				    ingestion_receipt_audit_state,
				    ingestion_recent_recovery_plan,
				    ingestion_cycle_state,
				    ingestion_archive_processing,
				    ingestion_gaps,
				    ingestion_source_poll_state,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs,
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions
				restart identity cascade
				""").update();
		httpClient = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NEVER)
				.build();
		source = new FakeGdeltSource(WINDOW_SLOTS, trace);
		source.setLatestTerminalProbe(this::latestPairIsTerminal);
	}

	@AfterEach
	void tearDown() {
		Thread.interrupted();
		try {
			if (source != null) {
				source.close();
			}
		}
		finally {
			if (httpClient != null) {
				httpClient.close();
			}
		}
	}

	@Test
	@DisplayName("После длительной недоступности GDELT новая серия завершает суточное окно и перезапуск Spring сохраняет историю")
	void recoversAfterExhaustedOutageAndPreservesHistoryAcrossSpringRestart() throws Exception {
		source.useCoherentMaster();
		source.unavailableArchive.set(source.artifact(FIRST_HISTORICAL,
				ArchiveType.TRANSLATION_EVENTS).archiveName());
		assertThat(pipeline(realArchiveProcessor).runOneShot()).isEqualTo(IngestionOneShotOutcome.EXPECTED_FAILURE);
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status()).isEqualTo(IngestionCoverageStatus.PARTIAL);
		source.manifestUnavailable.set(true);
		for (int attempt = 0; attempt < 4; attempt++) {
			assertThatExceptionOfType(com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException.class)
					.isThrownBy(() -> pipeline(realArchiveProcessor).runOneShot());
			if (attempt < 3) {
				var next = sourcePollLedger.findBySourceName(GdeltSourceContract.SOURCE_NAME).orElseThrow()
						.attempt().retry().retryNotBefore();
				clock.advance(Duration.between(clock.instant(), next));
			}
		}
		var exhausted = sourcePollLedger.findBySourceName(GdeltSourceContract.SOURCE_NAME).orElseThrow().attempt();
		assertThat(exhausted.retry().automaticRetriesUsed()).isEqualTo(3);
		assertThat(exhausted.retry().retryNotBefore()).isEqualTo(clock.instant().plus(Duration.ofMinutes(15)));
		int polls = source.manifestRequests();
		assertThat(pipeline(realArchiveProcessor).runOneShot()).isEqualTo(IngestionOneShotOutcome.RETRY_DEFERRED);
		assertThat(source.manifestRequests()).isEqualTo(polls);

		source.manifestUnavailable.set(false);
		source.unavailableArchive.set(null);
		clock.advance(Duration.ofMinutes(15));
		assertThat(pipeline(realArchiveProcessor).runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status()).isEqualTo(IngestionCoverageStatus.COMPLETE);
		assertThat(sourcePollLedger.findBySourceName(GdeltSourceContract.SOURCE_NAME).orElseThrow().attempt().retry())
				.satisfies(retry -> {
					assertThat(retry.retrySequence()).isEqualTo(2);
					assertThat(retry.lastExhaustedErrorCode()).isEqualTo("MANIFEST_HTTP_ERROR");
				});
		assertThat(runCount()).isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(documentCount(GdeltIndexKind.EVENT)).isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(EXPECTED_WINDOW_SLOTS);
		Map<Path, String> finalFiles = finalFileHashes();
		assertThat(finalFiles).hasSize(EXPECTED_WINDOW_SLOTS * 4);

		// Два новых Spring runtime работают с тем же внешним хранилищем. Закрытие runtime не владеет жизнью БД/ES.
		for (int restart = 0; restart < 2; restart++) {
			try (var runtime = new SpringApplicationBuilder(
					EventMosaicApplication.class)
					.initializers(application -> {
						var context = (GenericApplicationContext) application;
						context.registerBean(DataSource.class, () -> dataSource,
								definition -> definition.setDestroyMethodName(""));
						context.registerBean(ElasticsearchClient.class, () -> elasticsearchClient,
								definition -> definition.setDestroyMethodName(""));
					})
					.run("--server.port=0", "--spring.docker.compose.enabled=false",
							"--event-mosaic.ingestion.gdelt.automatic.enabled=false",
							"--event-mosaic.ingestion.gdelt.one-shot-enabled=false",
							"--event-mosaic.ingestion.gdelt.staging-root=" + tempDir.resolve("staging"))) {
				assertThat(runtime.getBean(IngestionCoverageQuery.class).read(WINDOW_FROM, WINDOW_TO).status())
						.isEqualTo(IngestionCoverageStatus.COMPLETE);
				assertThat(runtime.getBean(JdbcClient.class).sql("select count(*) from ingestion_runs")
						.query(Long.class).single()).isEqualTo((long) EXPECTED_WINDOW_SLOTS);
			}
		}
		assertThat(finalFileHashes()).isEqualTo(finalFiles);
		assertThat(documentCount(GdeltIndexKind.EVENT)).isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(EXPECTED_WINDOW_SLOTS);
		int downloads = source.objectRequests();
		assertThat(pipeline(realArchiveProcessor).runOneShot()).isEqualTo(IngestionOneShotOutcome.UNCHANGED);
		assertThat(source.objectRequests()).isEqualTo(downloads);
	}

	private Map<Path, String> finalFileHashes() throws Exception {
		Map<Path, String> hashes = new LinkedHashMap<>();
		try (var paths = java.nio.file.Files.walk(tempDir.resolve("staging"))) {
			for (Path path : paths.filter(java.nio.file.Files::isRegularFile).toList()) {
				assertThat(path.getFileName().toString()).doesNotEndWith(".part");
				hashes.put(path, HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
						.digest(java.nio.file.Files.readAllBytes(path))));
			}
		}
		return hashes;
	}

	@Test
	@DisplayName("Latest-first, lagging master, interruption и restart завершают 96 Event-слотов без повторного полного tick")
	void completesRecentWindowAcrossLagInterruptionAndStatelessRestart() throws Exception {
		assertThat(runCount()).isZero();
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status())
				.isEqualTo(IngestionCoverageStatus.UNKNOWN);
		assertThat(source.globalEventIds()).hasSize(EXPECTED_WINDOW_SLOTS);

		source.useLaggingMaster(List.of(
				REGISTERED_BUT_NOT_STARTED,
				INTERRUPTED_HISTORICAL,
				FIRST_HISTORICAL));
		GdeltPipelineService interruptedGraph = pipeline(new TracingArchiveProcessor(
				realArchiveProcessor,
				trace,
				INTERRUPTED_HISTORICAL));

		try {
			assertThatExceptionOfType(IngestionInterruptedException.class)
					.isThrownBy(interruptedGraph::runOneShot);
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		}
		finally {
			Thread.interrupted();
		}

		assertLatestPairWasTerminalBeforeMasterRequest();
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status())
				.isEqualTo(IngestionCoverageStatus.PARTIAL);
		assertThat(recentRecoveryPlanLedger.currentPlan().orElseThrow().catalogStatus())
				.isEqualTo(RecentRecoveryPlanLedger.CatalogStatus.PENDING);
		assertThat(source.masterRequests()).isEqualTo(1);
		assertThat(registeredHistoricalRunCount()).isEqualTo(3);
		assertThat(processTrace(ArchiveType.TRANSLATION_EVENTS)).containsExactly(
				FRONTIER,
				FIRST_HISTORICAL,
				INTERRUPTED_HISTORICAL);
		assertThat(processingStatus(INTERRUPTED_HISTORICAL, ArchiveType.TRANSLATION_EVENTS))
				.isEqualTo("FAILED");
		assertThat(processingStatus(FIRST_HISTORICAL, ArchiveType.TRANSLATION_EVENTS))
				.isEqualTo("INDEXED");
		assertThat(processingStateCount(
				REGISTERED_BUT_NOT_STARTED,
				ArchiveType.TRANSLATION_EVENTS)).isZero();

		clock.advance(Duration.ofMinutes(2));
		source.useCoherentMaster();
		source.failMentionOnce(FAILED_MENTION);
		GdeltPipelineService restartedGraph = pipeline(new TracingArchiveProcessor(
				realArchiveProcessor,
				trace,
				null));

		assertThat(restartedGraph.runOneShot()).isEqualTo(IngestionOneShotOutcome.EXPECTED_FAILURE);

		assertThat(recentRecoveryPlanLedger.currentPlan().orElseThrow().catalogStatus())
				.isEqualTo(RecentRecoveryPlanLedger.CatalogStatus.CATALOG_COMPLETE);
		assertThat(source.masterRequests()).isEqualTo(2);
		assertThat(source.masterLatestTerminalObservations()).containsOnly(true);
		assertThat(source.masterRanges()).containsOnly("bytes=-131072");
		assertThat(processingAttemptCount(
				INTERRUPTED_HISTORICAL,
				ArchiveType.TRANSLATION_EVENTS)).isEqualTo(2);
		assertThat(indexedArchiveCount(ArchiveType.TRANSLATION_EVENTS))
				.isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(indexedArchiveCount(ArchiveType.TRANSLATION_MENTIONS))
				.isEqualTo(EXPECTED_WINDOW_SLOTS - 1);
		assertThat(retryableFailedArchiveCount(ArchiveType.TRANSLATION_MENTIONS))
				.isEqualTo(1);
		assertThat(source.failedMentionRequests()).isEqualTo(1);
		assertThat(runCount()).isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status())
				.isEqualTo(IngestionCoverageStatus.COMPLETE);
		assertThat(documentCount(GdeltIndexKind.EVENT)).isEqualTo(EXPECTED_WINDOW_SLOTS);
		assertThat(documentCount(GdeltIndexKind.MENTION))
				.isEqualTo(EXPECTED_WINDOW_SLOTS - 1);

		long runsBeforeRepeat = runCount();
		int downloadsBeforeRepeat = source.objectRequests();
		int masterBeforeRepeat = source.masterRequests();
		long eventDocumentsBeforeRepeat = documentCount(GdeltIndexKind.EVENT);
		long mentionDocumentsBeforeRepeat = documentCount(GdeltIndexKind.MENTION);
		int manifestsBeforeRepeat = source.manifestRequests();
		GdeltPipelineService repeatedGraph = pipeline(new TracingArchiveProcessor(
				realArchiveProcessor,
				trace,
				null));

		assertThat(repeatedGraph.runOneShot()).isEqualTo(IngestionOneShotOutcome.UNCHANGED);

		assertThat(source.manifestRequests()).isEqualTo(manifestsBeforeRepeat + 1);
		assertThat(source.masterRequests()).isEqualTo(masterBeforeRepeat);
		assertThat(source.objectRequests()).isEqualTo(downloadsBeforeRepeat);
		assertThat(runCount()).isEqualTo(runsBeforeRepeat);
		assertThat(documentCount(GdeltIndexKind.EVENT)).isEqualTo(eventDocumentsBeforeRepeat);
		assertThat(documentCount(GdeltIndexKind.MENTION)).isEqualTo(mentionDocumentsBeforeRepeat);
	}

	@Autowired
	private ReceiptAuditService receiptAuditService;

	private GdeltPipelineService pipeline(GdeltArchiveProcessor archiveProcessor) {
		GdeltIngestionProperties properties = GdeltTestFixtures.properties(
				tempDir.resolve("staging"),
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW);
		IngestionMetrics metrics = new IngestionMetrics(meterRegistry);
		HttpRetryAfterParser retryAfterParser = new HttpRetryAfterParser(clock);
		ArchiveDownloadUriResolver resolver = archiveName -> source.rootUri()
				.resolve("/objects/" + archiveName.value());
		IngestionRunService acquisition = new IngestionRunService(
				new GdeltManifestClient(
						httpClient,
						source.rootUri().resolve("/manifest"),
						Duration.ofSeconds(5),
						65_536,
						retryAfterParser),
				new GdeltManifestParser(new GdeltManifestLineParser()),
				ingestionArchiveLedger,
				recentRecoveryPlanLedger,
				recentWindowPlanner,
				sourcePollLedger,
				new StagingLayout(properties),
				new HttpArchiveDownloader(
						httpClient,
						properties,
						metrics,
						resolver,
						retryAfterParser),
				new ZipArchiveStager(properties, metrics),
				properties,
				metrics,
				storageMonitor);
		GdeltTranslationMasterCatalogClient catalogClient =
				new GdeltTranslationMasterCatalogClient(
						httpClient,
						source.rootUri().resolve("/masterfilelist-translation.txt"),
						Duration.ofSeconds(5),
						retryAfterParser,
						new GdeltTranslationMasterCatalogParser(
								new GdeltManifestLineParser()));
		return new GdeltPipelineService(
				acquisition,
				recentRecoveryPlanLedger,
				catalogClient,
				processingLedger,
				archiveProcessor,
				fingerprintFactory,
				indexWriter,
				indexTargetResolver,
				properties,
				GdeltTestFixtures.backendDataProperties(),
				metrics,
				storageMonitor, receiptAuditService);
	}

	private void assertLatestPairWasTerminalBeforeMasterRequest() {
		int firstMaster = trace.indexOf("master:LAGGING");
		assertThat(firstMaster).isPositive();
		assertThat(trace.indexOf(terminalToken(ArchiveType.TRANSLATION_EVENTS, FRONTIER)))
				.isBetween(0, firstMaster - 1);
		assertThat(trace.indexOf(terminalToken(ArchiveType.TRANSLATION_MENTIONS, FRONTIER)))
				.isBetween(0, firstMaster - 1);
		assertThat(source.masterLatestTerminalObservations()).containsExactly(true);
	}

	private boolean latestPairIsTerminal() {
		return jdbcClient.sql("""
				select count(*) = 2
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.source_update_time = :frontier
				  and processing.status = 'INDEXED'
				""")
				.param("frontier", Timestamp.from(FRONTIER))
				.query(Boolean.class)
				.single();
	}

	private long runCount() {
		return jdbcClient.sql("select count(*) from ingestion_runs")
				.query(Long.class)
				.single();
	}

	private long registeredHistoricalRunCount() {
		return jdbcClient.sql("""
				select count(*)
				from ingestion_runs
				where source_update_time < :frontier
				""")
				.param("frontier", Timestamp.from(FRONTIER))
				.query(Long.class)
				.single();
	}

	private long indexedArchiveCount(ArchiveType archiveType) {
		return jdbcClient.sql("""
				select count(*)
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.archive_type = :archiveType
				  and processing.status = 'INDEXED'
				""")
				.param("archiveType", archiveType.name())
				.query(Long.class)
				.single();
	}

	private long retryableFailedArchiveCount(ArchiveType archiveType) {
		return jdbcClient.sql("""
				select count(*)
				from ingestion_archives
				where archive_type = :archiveType
				  and status = 'FAILED'
				  and last_error_retryable
				""")
				.param("archiveType", archiveType.name())
				.query(Long.class)
				.single();
	}

	private String processingStatus(Instant updateTime, ArchiveType archiveType) {
		return jdbcClient.sql("""
				select processing.status
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.source_update_time = :updateTime
				  and archive.archive_type = :archiveType
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.param("archiveType", archiveType.name())
				.query(String.class)
				.single();
	}

	private long processingStateCount(Instant updateTime, ArchiveType archiveType) {
		return jdbcClient.sql("""
				select count(*)
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.source_update_time = :updateTime
				  and archive.archive_type = :archiveType
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.param("archiveType", archiveType.name())
				.query(Long.class)
				.single();
	}

	private int processingAttemptCount(Instant updateTime, ArchiveType archiveType) {
		return jdbcClient.sql("""
				select processing.total_attempt_count
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.source_update_time = :updateTime
				  and archive.archive_type = :archiveType
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.param("archiveType", archiveType.name())
				.query(Integer.class)
				.single();
	}

	private long documentCount(GdeltIndexKind kind) throws IOException {
		return elasticsearchClient.count(request -> request.index(kind.readAlias())).count();
	}

	private List<Instant> processTrace(ArchiveType archiveType) {
		String prefix = "process:" + archiveType + ":";
		return trace.stream()
				.filter(entry -> entry.startsWith(prefix))
				.map(entry -> Instant.parse(entry.substring(prefix.length())))
				.toList();
	}

	private static List<Instant> windowSlots() {
		List<Instant> result = new ArrayList<>(EXPECTED_WINDOW_SLOTS);
		for (Instant slot = WINDOW_FROM; slot.isBefore(WINDOW_TO); slot = slot.plus(SLOT)) {
			result.add(slot);
		}
		return List.copyOf(result);
	}

	private static String processToken(ArchiveType type, Instant updateTime) {
		return "process:" + type + ":" + updateTime;
	}

	private static String terminalToken(ArchiveType type, Instant updateTime) {
		return "terminal:" + type + ":" + updateTime;
	}

	private static final class TracingArchiveProcessor implements GdeltArchiveProcessor {

		private final GdeltArchiveProcessor delegate;
		private final List<String> trace;
		private final Instant interruptAt;
		private final AtomicBoolean interruptionPending = new AtomicBoolean(true);

		private TracingArchiveProcessor(
				GdeltArchiveProcessor delegate,
				List<String> trace,
				Instant interruptAt
		) {
			this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
			this.trace = Objects.requireNonNull(trace, "trace must not be null");
			this.interruptAt = interruptAt;
		}

		@Override
		public ArchiveProcessingResult process(
				ArchiveProcessingRequest request,
				ArchiveProcessingProgressListener progressListener,
				ArchiveProcessingDiagnosticListener diagnosticListener
		) {
			ArchiveType type = request.kind() == GdeltArchiveKind.TRANSLATION_EVENTS
					? ArchiveType.TRANSLATION_EVENTS
					: ArchiveType.TRANSLATION_MENTIONS;
			trace.add(processToken(type, request.sourceUpdateTime()));
			if (request.sourceUpdateTime().equals(interruptAt)
					&& interruptionPending.compareAndSet(true, false)) {
				return ArchiveProcessingResult.failed(
						request.kind(),
						ArchiveProcessingProgress.empty(),
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED,
								true,
								null));
			}
			ArchiveProcessingResult result = delegate.process(
					request,
					progressListener,
					diagnosticListener);
			if (result.outcome() == ArchiveProcessingOutcome.COMPLETED) {
				trace.add(terminalToken(type, request.sourceUpdateTime()));
			}
			return result;
		}
	}

	private enum MasterPhase {
		LAGGING,
		COHERENT
	}

	private static final class FakeGdeltSource implements AutoCloseable {

		private final AtomicReference<Instant> frontier = new AtomicReference<>(FRONTIER);
		private volatile Runnable beforeManifest = () -> {};
		private volatile java.util.function.Consumer<String> beforeArchive = name -> {};

		private final HttpServer server;
		private final Map<String, ArchiveArtifact> artifacts;
		private final Map<Instant, EnumMap<ArchiveType, ArchiveArtifact>> artifactsByTime;
		private final List<Instant> slots;
		private final Set<Long> globalEventIds;
		private final List<String> trace;
		private final AtomicReference<MasterPhase> masterPhase =
				new AtomicReference<>(MasterPhase.LAGGING);
		private final AtomicReference<List<Instant>> laggingSlots =
				new AtomicReference<>(List.of());
		private final AtomicReference<BooleanSupplier> latestTerminalProbe =
				new AtomicReference<>(() -> false);
		private final AtomicReference<String> failedMentionArchive = new AtomicReference<>();
		private final AtomicBoolean failedMentionPending = new AtomicBoolean();
		private final AtomicInteger failedMentionRequests = new AtomicInteger();
		private final AtomicInteger manifestRequests = new AtomicInteger();
		private final AtomicBoolean manifestUnavailable = new AtomicBoolean();
		private final AtomicReference<String> unavailableArchive = new AtomicReference<>();
		private final AtomicInteger masterRequests = new AtomicInteger();
		private final AtomicInteger objectRequests = new AtomicInteger();
		private final List<Boolean> masterLatestTerminalObservations =
				new CopyOnWriteArrayList<>();
		private final List<String> masterRanges = new CopyOnWriteArrayList<>();

		private FakeGdeltSource(List<Instant> slots, List<String> trace) throws IOException {
			this.slots = List.copyOf(slots);
			this.trace = Objects.requireNonNull(trace, "trace must not be null");
			Artifacts prepared = prepareArtifacts(slots);
			this.artifacts = prepared.byName();
			this.artifactsByTime = prepared.byTime();
			this.globalEventIds = prepared.globalEventIds();
			server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
			server.createContext("/manifest", this::serveManifest);
			server.createContext("/masterfilelist-translation.txt", this::serveMaster);
			server.createContext("/objects/", this::serveArchive);
			server.start();
		}

		private URI rootUri() {
			return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
		}

		private Set<Long> globalEventIds() {
			return globalEventIds;
		}

		private void setLatestTerminalProbe(BooleanSupplier probe) {
			latestTerminalProbe.set(Objects.requireNonNull(probe, "probe must not be null"));
		}

		private void useLaggingMaster(List<Instant> includedSlots) {
			laggingSlots.set(List.copyOf(includedSlots));
			masterPhase.set(MasterPhase.LAGGING);
		}

		private void useCoherentMaster() {
			masterPhase.set(MasterPhase.COHERENT);
		}

		private void failMentionOnce(Instant updateTime) {
			failedMentionArchive.set(artifact(
					updateTime,
					ArchiveType.TRANSLATION_MENTIONS).archiveName());
			failedMentionPending.set(true);
		}

		private int manifestRequests() {
			return manifestRequests.get();
		}

		private int masterRequests() {
			return masterRequests.get();
		}

		private int objectRequests() {
			return objectRequests.get();
		}

		private int failedMentionRequests() {
			return failedMentionRequests.get();
		}

		private List<Boolean> masterLatestTerminalObservations() {
			return List.copyOf(masterLatestTerminalObservations);
		}

		private List<String> masterRanges() {
			return List.copyOf(masterRanges);
		}

		private void serveManifest(HttpExchange exchange) throws IOException {
			manifestRequests.incrementAndGet();
			beforeManifest.run();
			trace.add("manifest");
			if (manifestUnavailable.get()) {
				exchange.sendResponseHeaders(503, -1);
				exchange.close();
				return;
			}
			byte[] body = manifestBody().getBytes(StandardCharsets.UTF_8);
			respond(exchange, 200, body);
		}

		private void serveMaster(HttpExchange exchange) throws IOException {
			MasterPhase phase = masterPhase.get();
			masterRequests.incrementAndGet();
			trace.add("master:" + phase);
			masterLatestTerminalObservations.add(latestTerminalProbe.get().getAsBoolean());
			masterRanges.add(exchange.getRequestHeaders().getFirst("Range"));
			byte[] body = masterBody(phase).getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add(
					"Content-Range",
					"bytes 0-" + (body.length - 1) + "/" + body.length);
			exchange.getResponseHeaders().add(
					"x-goog-generation",
					phase == MasterPhase.LAGGING ? "101" : "102");
			exchange.getResponseHeaders().add(
					"ETag",
					phase == MasterPhase.LAGGING ? "\"lagging\"" : "\"coherent\"");
			respond(exchange, 206, body);
		}

		private void serveArchive(HttpExchange exchange) throws IOException {
			String path = exchange.getRequestURI().getPath();
			String archiveName = path.substring("/objects/".length());
			ArchiveArtifact artifact = artifacts.get(archiveName);
			if (artifact == null) {
				exchange.sendResponseHeaders(404, -1);
				exchange.close();
				return;
			}
			objectRequests.incrementAndGet();
			beforeArchive.accept(archiveName);
			if (archiveName.equals(unavailableArchive.get())) {
				exchange.sendResponseHeaders(503, -1);
				exchange.close();
				return;
			}
			trace.add("download:" + artifact.archiveType() + ":" + artifact.updateTime());
			if (archiveName.equals(failedMentionArchive.get())
					&& failedMentionPending.compareAndSet(true, false)) {
				failedMentionRequests.incrementAndGet();
				exchange.sendResponseHeaders(503, -1);
				exchange.close();
				return;
			}
			respond(exchange, 200, artifact.zipBytes());
		}

		private String manifestBody() {
			Instant capturedFrontier = frontier.get();
			return metadataLine(artifact(capturedFrontier, ArchiveType.TRANSLATION_EVENTS))
					+ metadataLine(artifact(capturedFrontier, ArchiveType.TRANSLATION_MENTIONS));
		}

		private String masterBody(MasterPhase phase) {
			List<Instant> included = phase == MasterPhase.COHERENT
					? slots
					: laggingSlots.get();
			StringBuilder body = new StringBuilder(included.size() * 320);
			for (Instant updateTime : included) {
				body.append(metadataLine(artifact(
						updateTime,
						ArchiveType.TRANSLATION_EVENTS)));
				body.append(metadataLine(artifact(
						updateTime,
						ArchiveType.TRANSLATION_MENTIONS)));
			}
			return body.toString();
		}

		private ArchiveArtifact artifact(Instant updateTime, ArchiveType archiveType) {
			return artifactsByTime.get(updateTime).get(archiveType);
		}

		@Override
		public void close() {
			server.stop(0);
		}

		private static Artifacts prepareArtifacts(List<Instant> slots) throws IOException {
			String eventTemplate = firstResourceLine("/gdelt/csv/event-valid.csv");
			String mentionTemplate = firstResourceLine("/gdelt/csv/mention-valid.csv");
			Map<String, ArchiveArtifact> byName = new LinkedHashMap<>();
			Map<Instant, EnumMap<ArchiveType, ArchiveArtifact>> byTime =
					new LinkedHashMap<>();
			var eventIds = new java.util.LinkedHashSet<Long>();
			for (int index = 0; index < slots.size(); index++) {
				Instant updateTime = slots.get(index);
				long globalEventId = 7_000_000_000L + index;
				eventIds.add(globalEventId);
				EnumMap<ArchiveType, ArchiveArtifact> pair =
						new EnumMap<>(ArchiveType.class);
				ArchiveArtifact event = artifact(
						updateTime,
						ArchiveType.TRANSLATION_EVENTS,
						eventCsv(eventTemplate, globalEventId, updateTime));
				ArchiveArtifact mention = artifact(
						updateTime,
						ArchiveType.TRANSLATION_MENTIONS,
						mentionCsv(mentionTemplate, globalEventId, updateTime));
				pair.put(ArchiveType.TRANSLATION_EVENTS, event);
				pair.put(ArchiveType.TRANSLATION_MENTIONS, mention);
				byTime.put(updateTime, pair);
				byName.put(event.archiveName(), event);
				byName.put(mention.archiveName(), mention);
			}
			return new Artifacts(
					Map.copyOf(byName),
					Map.copyOf(byTime),
					Set.copyOf(eventIds));
		}

		private static ArchiveArtifact artifact(
				Instant updateTime,
				ArchiveType archiveType,
				byte[] csv
		) throws IOException {
			String archiveName = GdeltTestFixtures.archiveName(updateTime, archiveType);
			byte[] zip = zip(archiveName.replaceFirst("\\.zip$", ""), csv);
			return new ArchiveArtifact(
					updateTime,
					archiveType,
					archiveName,
					zip,
					md5(zip));
		}

		private static byte[] eventCsv(
				String template,
				long globalEventId,
				Instant updateTime
		) {
			String[] fields = template.split("\\t", -1);
			fields[0] = Long.toString(globalEventId);
			fields[59] = GdeltSourceContract.formatUpdateTimestamp(updateTime);
			fields[60] = "https://example.test/events/" + globalEventId;
			return (String.join("\t", fields) + "\n").getBytes(StandardCharsets.UTF_8);
		}

		private static byte[] mentionCsv(
				String template,
				long globalEventId,
				Instant updateTime
		) {
			String[] fields = template.split("\\t", -1);
			String timestamp = GdeltSourceContract.formatUpdateTimestamp(updateTime);
			fields[0] = Long.toString(globalEventId);
			fields[1] = timestamp;
			fields[2] = timestamp;
			fields[4] = "example.test";
			fields[5] = "https://example.test/mentions/" + globalEventId;
			return (String.join("\t", fields) + "\n").getBytes(StandardCharsets.UTF_8);
		}

		private static String firstResourceLine(String resource) throws IOException {
			try (var input = GdeltLiveRecentWindowAcceptanceIntegrationTest.class
					.getResourceAsStream(resource)) {
				String content = new String(
						Objects.requireNonNull(input, "Missing test resource " + resource)
								.readAllBytes(),
						StandardCharsets.UTF_8);
				return content.lines().findFirst().orElseThrow();
			}
		}

		private static String metadataLine(ArchiveArtifact artifact) {
			return artifact.zipBytes().length + " " + artifact.md5()
					+ " http://data.gdeltproject.org/gdeltv2/"
					+ artifact.archiveName()
					+ "\n";
		}

		private static byte[] zip(String entryName, byte[] content) throws IOException {
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
				var entry = new ZipEntry(entryName);
				entry.setTime(INITIAL_NOW.toEpochMilli());
				zip.putNextEntry(entry);
				zip.write(content);
				zip.closeEntry();
			}
			return bytes.toByteArray();
		}

		private static String md5(byte[] content) {
			try {
				return HexFormat.of().formatHex(
						MessageDigest.getInstance("MD5").digest(content));
			}
			catch (NoSuchAlgorithmException exception) {
				throw new IllegalStateException(exception);
			}
		}

		private static void respond(
				HttpExchange exchange,
				int status,
				byte[] body
		) throws IOException {
			exchange.sendResponseHeaders(status, body.length);
			try (var output = exchange.getResponseBody()) {
				output.write(body);
			}
			finally {
				exchange.close();
			}
		}
	}

	private record ArchiveArtifact(
			Instant updateTime,
			ArchiveType archiveType,
			String archiveName,
			byte[] zipBytes,
			String md5
	) {

		private ArchiveArtifact {
			zipBytes = zipBytes.clone();
		}

		@Override
		public byte[] zipBytes() {
			return zipBytes.clone();
		}
	}

	private record Artifacts(
			Map<String, ArchiveArtifact> byName,
			Map<Instant, EnumMap<ArchiveType, ArchiveArtifact>> byTime,
			Set<Long> globalEventIds
	) {
	}

	static final class AcceptanceClock extends Clock {

		private final AtomicReference<Instant> now = new AtomicReference<>(INITIAL_NOW);

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			Objects.requireNonNull(zone, "zone must not be null");
			return ZoneOffset.UTC.equals(zone) ? this : Clock.fixed(instant(), zone);
		}

		@Override
		public Instant instant() {
			return now.get();
		}

		private void reset() {
			now.set(INITIAL_NOW);
		}

		private void advance(Duration duration) {
			now.updateAndGet(current -> current.plus(duration));
		}
	}

	@TestConfiguration(proxyBeanMethods = false)
	static class AcceptanceTimeConfiguration {

		@Bean
		@Primary
		AcceptanceClock acceptanceClock() {
			return new AcceptanceClock();
		}

		@Bean
		@Primary
		RetryJitterSource acceptanceRetryJitter() {
			return () -> 0.5;
		}
	}
}
