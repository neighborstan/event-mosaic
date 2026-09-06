package com.neighbor.eventmosaic.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolution;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolutionStatus;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolver;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimResult;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.observability.StoragePressureState;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.ingestion.source.GdeltMasterCatalogValidationException;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalog;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalogClient;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalogStatus;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

@ExtendWith(OutputCaptureExtension.class)
@DisplayName("Сквозная orchestration staged архивов")
class GdeltPipelineServiceTest {

	private static final String EVENT_FINGERPRINT = "a".repeat(64);
	private static final String MENTION_FINGERPRINT = "b".repeat(64);
	private static final long INDEXED_STATE_VERSION = 7;
	private static final Instant NOW = Instant.parse("2026-07-30T12:00:00Z");
	private static final ArchiveIdentityDigest RECEIPT_DIGEST = digest("1");
	private static final ArchiveIdentityDigest OTHER_DIGEST = digest("2");
	private static final ArchiveIdentityDigest EMPTY_DIGEST = digest();
	private static final ActiveIndexTargets ACTIVE_TARGETS = new ActiveIndexTargets(
			"p20260727",
			3,
			17,
			UUID.fromString("11111111-1111-1111-1111-111111111111"),
			new ExactIndexTarget(
					"gdelt-events-v1-p20260727-g0001",
					"event-index-uuid"),
			new ExactIndexTarget(
					"gdelt-mentions-v1-p20260727-g0001",
					"mention-index-uuid"));

	@TempDir
	Path tempDir;

	private final IngestionRunService ingestionRunService = mock(IngestionRunService.class);
	private final RecentRecoveryPlanLedger recentRecoveryPlanLedger =
			mock(RecentRecoveryPlanLedger.class);
	private final GdeltTranslationMasterCatalogClient masterCatalogClient =
			mock(GdeltTranslationMasterCatalogClient.class);
	private final ArchiveProcessingLedger processingLedger =
			mock(ArchiveProcessingLedger.class);
	private final GdeltArchiveProcessor archiveProcessor =
			mock(GdeltArchiveProcessor.class);
	private final ProcessingFingerprintFactory fingerprintFactory =
			mock(ProcessingFingerprintFactory.class);
	private final GdeltIndexWriter indexWriter = mock(GdeltIndexWriter.class);
	private final IndexTargetResolver indexTargetResolver = mock(IndexTargetResolver.class);
	private final IngestionMetrics metrics = mock(IngestionMetrics.class);
	private final BackendDataStorageMonitor storageMonitor =
			mock(BackendDataStorageMonitor.class);

	private GdeltPipelineService service;

	@BeforeEach
	void setUp() {
		service = newService(GdeltTestFixtures.properties(tempDir, 1024 * 1024));
		when(indexTargetResolver.resolve(any(), any(OperationBudget.class))).thenReturn(
				IndexTargetResolution.ready(ACTIVE_TARGETS));
		when(fingerprintFactory.create(
				anyString(),
				any(GdeltArchiveKind.class),
				anyString(),
				anyString()))
				.thenAnswer(invocation -> invocation.getArgument(1)
						== GdeltArchiveKind.TRANSLATION_EVENTS
								? EVENT_FINGERPRINT
								: MENTION_FINGERPRINT);
		when(processingLedger.checkpoint(any(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.markIndexed(any(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.markFailed(any(), any(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ingestionRunService.acquireArchive(
				any(IngestionArchiveState.class),
				any(OperationBudget.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));
		when(ingestionRunService.completeOneShot(
				any(IngestionRunState.class),
				any(OperationBudget.class)))
				.thenAnswer(invocation -> invocation.getArgument(0));
	}

	private GdeltPipelineService newService(GdeltIngestionProperties properties) {
		return new GdeltPipelineService(
				ingestionRunService,
				recentRecoveryPlanLedger,
				masterCatalogClient,
				processingLedger,
				archiveProcessor,
				fingerprintFactory,
				indexWriter,
				indexTargetResolver,
				properties,
				GdeltTestFixtures.backendDataProperties(),
				metrics,
				storageMonitor);
	}

	@Test
	@DisplayName("Deferred source poll завершает one-shot без downstream I/O")
	void deferredSourcePollFinishesWithoutDownstreamIo() {
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.empty(), true));

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.RETRY_DEFERRED);

		verifyNoInteractions(processingLedger, archiveProcessor, indexWriter);
	}

	@Test
	@DisplayName("Потеря source poll ownership завершает pipeline без downstream I/O")
	void sourcePollOwnershipLossFinishesWithoutDownstreamIo() {
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.empty(), false, true));

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.OWNERSHIP_LOST);

		verifyNoInteractions(
				processingLedger,
				archiveProcessor,
				indexWriter,
				indexTargetResolver);
		verify(metrics).cycleDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.OWNERSHIP_LOST));
	}

	@Test
	@DisplayName("Автоматический цикл полностью обрабатывает Event до начала Mention")
	void automaticCycleProcessesEventBeforeMention() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);

		var order = org.mockito.Mockito.inOrder(ingestionRunService, archiveProcessor);
		order.verify(ingestionRunService).prepareOneShot(any(OperationBudget.class));
		order.verify(ingestionRunService).acquireArchive(
				org.mockito.ArgumentMatchers.argThat(state ->
						state.archive().archiveType()
								== com.neighbor.eventmosaic.ingestion.api.ArchiveType
										.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				org.mockito.ArgumentMatchers.argThat(request ->
						request.kind() == GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		order.verify(ingestionRunService).acquireArchive(
				org.mockito.ArgumentMatchers.argThat(state ->
						state.archive().archiveType()
								== com.neighbor.eventmosaic.ingestion.api.ArchiveType
										.TRANSLATION_MENTIONS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				org.mockito.ArgumentMatchers.argThat(request ->
						request.kind() == GdeltArchiveKind.TRANSLATION_MENTIONS),
				any(),
				any());
	}

	@Test
	@DisplayName("Недавнее окно обрабатывает свежую пару до каталога, затем все Event до Mention")
	void recentWindowProcessesLatestThenCatalogThenRecentEventsBeforeMentions() {
		Instant frontier = GdeltTestFixtures.UPDATE_TIME;
		Instant recentNewest = frontier.minus(Duration.ofMinutes(15));
		Instant recentOlder = frontier.minus(Duration.ofMinutes(30));
		IngestionRunState latest = runState(frontier, true);
		IngestionArchiveState recentNewestEvent = archiveState(
				recentNewest,
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_EVENTS);
		IngestionArchiveState recentOlderEvent = archiveState(
				recentOlder,
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_EVENTS);
		IngestionArchiveState recentNewestMention = archiveState(
				recentNewest,
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_MENTIONS);
		IngestionArchiveState recentOlderMention = archiveState(
				recentOlder,
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_MENTIONS);
		RecentRecoveryPlanLedger.PlanState pendingPlan = recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.PENDING);
		GdeltTranslationMasterCatalog laggingCatalog = new GdeltTranslationMasterCatalog(
				List.of(
						GdeltTestFixtures.update(recentOlder),
						GdeltTestFixtures.update(recentNewest)),
				java.util.Set.of(),
				GdeltTranslationMasterCatalogStatus.PENDING,
				pendingPlan.revision().windowFrom(),
				"42",
				"master-etag",
				List.of(new DiscoveryDiagnostic(IngestionEventCode.MASTER_AHEAD_OF_LATEST, 7)));
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(pendingPlan));
		when(masterCatalogClient.fetchCatalog(any(), any(), any()))
				.thenReturn(laggingCatalog);
		when(recentRecoveryPlanLedger.registerCatalog(any())).thenReturn(
				new RecentRecoveryPlanLedger.CatalogRegistrationResult(
						RecentRecoveryPlanLedger.CatalogRegistrationOutcome.APPLIED,
						pendingPlan));
		when(recentRecoveryPlanLedger.archivesNewestFirst(
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_EVENTS))
				.thenReturn(List.of(recentNewestEvent, recentOlderEvent));
		when(recentRecoveryPlanLedger.archivesNewestFirst(
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_MENTIONS))
				.thenReturn(List.of(recentNewestMention, recentOlderMention));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			if (request.kind() == GdeltArchiveKind.TRANSLATION_MENTIONS) {
				return ArchiveProcessingResult.failed(
						request.kind(),
						ArchiveProcessingProgress.empty(),
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.INDEXING_OPERATION_FAILURE,
								true,
								null));
			}
			return completed(request.kind(), successfulProgress());
		});

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);

		var order = org.mockito.Mockito.inOrder(
				ingestionRunService,
				archiveProcessor,
				masterCatalogClient,
				recentRecoveryPlanLedger);
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(frontier, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(frontier, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(frontier, ArchiveType.TRANSLATION_MENTIONS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(frontier, GdeltArchiveKind.TRANSLATION_MENTIONS),
				any(),
				any());
		order.verify(masterCatalogClient).fetchCatalog(any(), any(), any());
		order.verify(recentRecoveryPlanLedger).registerCatalog(
				org.mockito.ArgumentMatchers.argThat(registration ->
						!registration.catalogComplete()
								&& registration.updates().size() == 2));
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(recentNewest, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(recentNewest, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(recentOlder, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(recentOlder, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(recentNewest, ArchiveType.TRANSLATION_MENTIONS),
				any(OperationBudget.class));
		verify(metrics).event(IngestionEventCode.MASTER_AHEAD_OF_LATEST);
	}

	@Test
	@DisplayName("Ошибка master не мешает обработать уже зарегистрированный recent архив")
	void pendingCatalogFailureStillProcessesRegisteredRecentWork() {
		Instant frontier = GdeltTestFixtures.UPDATE_TIME;
		Instant recentTime = frontier.minus(Duration.ofMinutes(15));
		IngestionRunState latest = runState(frontier, true);
		IngestionArchiveState recentEvent = archiveState(
				recentTime,
				ArchiveType.TRANSLATION_EVENTS);
		RecentRecoveryPlanLedger.PlanState pendingPlan = recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.PENDING);
		RemoteSourceAccessException masterFailure = new RemoteSourceAccessException(
				IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR);
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(pendingPlan));
		when(masterCatalogClient.fetchCatalog(any(), any(), any()))
				.thenThrow(masterFailure);
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_EVENTS))
				.thenReturn(List.of(recentEvent));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_MENTIONS))
				.thenReturn(List.of());
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});

		assertThatThrownBy(service::runOneShot).isSameAs(masterFailure);

		var order = org.mockito.Mockito.inOrder(
				masterCatalogClient,
				ingestionRunService,
				archiveProcessor);
		order.verify(masterCatalogClient).fetchCatalog(any(), any(), any());
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(recentTime, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(recentTime, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		assertThat(pendingPlan.catalogStatus())
				.isEqualTo(RecentRecoveryPlanLedger.CatalogStatus.PENDING);
		verify(recentRecoveryPlanLedger, never()).registerCatalog(any());
	}

	@Test
	@DisplayName("Постоянный отказ master логируется и возвращается после известного recent архива")
	void permanentCatalogFailureStillProcessesRegisteredRecentWork() {
		Instant frontier = GdeltTestFixtures.UPDATE_TIME;
		Instant recentTime = frontier.minus(Duration.ofMinutes(15));
		IngestionRunState latest = runState(frontier, true);
		IngestionArchiveState recentEvent = archiveState(
				recentTime,
				ArchiveType.TRANSLATION_EVENTS);
		RemoteResponseRejectedException masterFailure =
				new RemoteResponseRejectedException(
						IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR,
						IngestionErrorContext.forHttpStatus(403));
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.PENDING)));
		when(masterCatalogClient.fetchCatalog(any(), any(), any()))
				.thenThrow(masterFailure);
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_EVENTS))
				.thenReturn(List.of(recentEvent));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_MENTIONS))
				.thenReturn(List.of());
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});

		Logger logger = (Logger) LoggerFactory.getLogger(GdeltPipelineService.class);
		ListAppender<ILoggingEvent> appender = new ListAppender<>();
		appender.start();
		logger.addAppender(appender);
		try {
			assertThatThrownBy(service::runOneShot).isSameAs(masterFailure);
		}
		finally {
			logger.detachAppender(appender);
			appender.stop();
		}

		var order = org.mockito.Mockito.inOrder(
				masterCatalogClient,
				ingestionRunService,
				archiveProcessor);
		order.verify(masterCatalogClient).fetchCatalog(any(), any(), any());
		order.verify(ingestionRunService).acquireArchive(
				archiveAt(recentTime, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		order.verify(archiveProcessor).process(
				requestAt(recentTime, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		verify(metrics).error(IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR);
		verify(recentRecoveryPlanLedger, never()).registerCatalog(any());
		assertThat(appender.list)
				.filteredOn(event -> "GDELT master catalog failed".equals(
						event.getFormattedMessage()))
				.singleElement()
				.satisfies(event -> assertThat(event.getKeyValuePairs())
						.anySatisfy(pair -> {
							assertThat(pair.key).isEqualTo("http_status");
							assertThat(pair.value).isEqualTo(403);
						})
						.anySatisfy(pair -> {
							assertThat(pair.key).isEqualTo("retryable");
							assertThat(pair.value).isEqualTo(false);
						}));
	}

	@Test
	@DisplayName("Завершенный каталог не читает master и не захватывает уже проиндексированные recent архивы")
	void completeCatalogSkipsMasterAndIndexedRecentArchives() {
		Instant frontier = GdeltTestFixtures.UPDATE_TIME;
		Instant recentTime = frontier.minus(Duration.ofMinutes(15));
		IngestionRunState latest = runState(frontier, true);
		IngestionArchiveState recentEvent = archiveState(
				recentTime,
				ArchiveType.TRANSLATION_EVENTS);
		IngestionArchiveState recentMention = archiveState(
				recentTime,
				ArchiveType.TRANSLATION_MENTIONS);
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.CATALOG_COMPLETE)));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_EVENTS))
				.thenReturn(List.of(recentEvent));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_MENTIONS))
				.thenReturn(List.of(recentMention));
		stubIndexedRegistrations(latest);
		when(processingLedger.findByArchiveIdempotencyKey(
				recentEvent.archive().idempotencyKey()))
				.thenReturn(Optional.of(indexedState(recentEvent, eventFingerprint())));
		when(processingLedger.findByArchiveIdempotencyKey(
				recentMention.archive().idempotencyKey()))
				.thenReturn(Optional.of(indexedState(recentMention, mentionFingerprint())));

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);

		verifyNoInteractions(masterCatalogClient);
		verify(ingestionRunService, never()).acquireArchive(
				org.mockito.ArgumentMatchers.argThat(state ->
						state.archive().sourceUpdateTime().equals(recentTime)),
				any(OperationBudget.class));
		verify(indexWriter, never()).verifyReceipt(any(), any(OperationBudget.class));
	}

	@Test
	@DisplayName("Повторный recent-pass пропускает завершенный newest Event и продолжает со следующего")
	void recentPassRestartSkipsTerminalEventAndProcessesNextNewestEligible() {
		Instant frontier = GdeltTestFixtures.UPDATE_TIME;
		Instant recentNewest = frontier.minus(Duration.ofMinutes(15));
		Instant recentOlder = frontier.minus(Duration.ofMinutes(30));
		IngestionRunState latest = runState(frontier, true);
		IngestionArchiveState recentNewestEvent = archiveState(
				recentNewest,
				ArchiveType.TRANSLATION_EVENTS);
		IngestionArchiveState recentOlderEvent = archiveState(
				recentOlder,
				ArchiveType.TRANSLATION_EVENTS);
		AtomicBoolean newestIndexed = new AtomicBoolean();
		AtomicLong nanoTime = new AtomicLong();
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.CATALOG_COMPLETE)));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_EVENTS))
				.thenReturn(List.of(recentNewestEvent, recentOlderEvent));
		when(recentRecoveryPlanLedger.archivesNewestFirst(ArchiveType.TRANSLATION_MENTIONS))
				.thenReturn(List.of());
		when(processingLedger.findByArchiveIdempotencyKey(
				recentNewestEvent.archive().idempotencyKey()))
				.thenAnswer(_ -> newestIndexed.get()
						? Optional.of(indexedState(recentNewestEvent, eventFingerprint()))
						: Optional.empty());
		when(processingLedger.findByArchiveIdempotencyKey(
				recentOlderEvent.archive().idempotencyKey()))
				.thenReturn(Optional.empty());
		when(processingLedger.register(anyString(), any())).thenAnswer(invocation -> {
			String archiveKey = invocation.getArgument(0);
			ArchiveProcessingFingerprint fingerprint = invocation.getArgument(1);
			Optional<IngestionArchiveState> latestArchive = latest.archives().stream()
					.filter(state -> state.archive().idempotencyKey().equals(archiveKey))
					.findFirst();
			return latestArchive
					.map(state -> indexedState(state, fingerprint))
					.orElseGet(() -> pendingState(archiveKey, fingerprint));
		});
		when(processingLedger.claim(anyString(), any(), any())).thenAnswer(invocation ->
				claimedAttempt(
						invocation.getArgument(0),
						eventFingerprint(),
						invocation.getArgument(1)));
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});
		when(processingLedger.markIndexed(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingAttempt attempt = invocation.getArgument(0);
			if (attempt.archiveIdempotencyKey().equals(
					recentNewestEvent.archive().idempotencyKey())) {
				newestIndexed.set(true);
				nanoTime.set(Duration.ofSeconds(1).toNanos());
			}
			return AttemptTransitionResult.APPLIED;
		});

		OperationBudget firstBudget = OperationBudget.start(
				Duration.ofSeconds(1),
				nanoTime::get);
		assertThat(service.runOneShot(firstBudget))
				.isEqualTo(IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED);
		assertThat(newestIndexed).isTrue();

		nanoTime.set(0);
		org.mockito.Mockito.clearInvocations(
				ingestionRunService,
				archiveProcessor,
				processingLedger);
		OperationBudget restartBudget = OperationBudget.start(
				Duration.ofSeconds(1),
				nanoTime::get);

		assertThat(service.runOneShot(restartBudget))
				.isEqualTo(IngestionOneShotOutcome.COMPLETED);

		verify(ingestionRunService, never()).acquireArchive(
				archiveAt(recentNewest, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		verify(archiveProcessor, never()).process(
				requestAt(recentNewest, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
		verify(processingLedger, never()).register(
				eq(recentNewestEvent.archive().idempotencyKey()),
				any());
		verify(ingestionRunService).acquireArchive(
				archiveAt(recentOlder, ArchiveType.TRANSLATION_EVENTS),
				any(OperationBudget.class));
		verify(archiveProcessor).process(
				requestAt(recentOlder, GdeltArchiveKind.TRANSLATION_EVENTS),
				any(),
				any());
	}

	@Test
	@DisplayName("Поврежденная строка master остается повторяемой ошибкой с исходным кодом")
	void malformedMasterEntryMapsToRetryableSourceFailure() {
		assertCatalogValidationMapping(
				IngestionErrorCode.MASTER_CATALOG_MALFORMED_LINE,
				RemoteSourceAccessException.class,
				true);
	}

	@Test
	@DisplayName("Запрещенный адрес master становится постоянной ошибкой с исходным кодом")
	void rejectedMasterUriMapsToPermanentSourceFailure() {
		assertCatalogValidationMapping(
				IngestionErrorCode.MASTER_CATALOG_SOURCE_URI_REJECTED,
				SourceDataViolationException.class,
				false);
	}

	@Test
	@DisplayName("Автоматический цикл пропускает уже проиндексированный архив без проверки Elasticsearch")
	void automaticCycleSkipsIndexedArchiveWithoutReceiptVerification() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		ArchiveProcessingFingerprint fingerprint = eventFingerprint();
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		when(processingLedger.register(archive.archive().idempotencyKey(), fingerprint))
				.thenReturn(indexedState(archive, fingerprint));

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);

		verify(indexWriter, never()).verifyReceipt(any(), any(OperationBudget.class));
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Elasticsearch pressure завершает one-shot до resolver, claim и bulk")
	void elasticsearchPressureStopsBeforeResolverClaimAndBulk() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		doThrow(new StoragePressureException(
				StorageResource.ELASTICSEARCH,
				StoragePressureState.PRESSURE))
				.when(storageMonitor)
				.requireCapacity(StorageResource.ELASTICSEARCH);

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.STORAGE_PRESSURE);

		verifyNoInteractions(processingLedger, archiveProcessor, indexWriter, indexTargetResolver);
		verify(metrics).cycleDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.STORAGE_PRESSURE));
	}

	@Test
	@DisplayName("Deadline после внешнего результата сохраняет retryable отказ и не начинает Mention")
	void deadlineAfterCompletedEventDoesNotStartMention() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			nanoTime.set(Duration.ofSeconds(1).toNanos());
			return completed(request.kind(), successfulProgress());
		});

		assertThat(service.runOneShot(budget))
				.isEqualTo(IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED);

		verify(archiveProcessor).process(any(), any(), any());
		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger).markFailed(
				any(),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED.code(),
						true)),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
						1, 0, 0, 1, 1, 0, 1, null)),
				eq(null));
		verify(processingLedger).register(
				runState.archives().getFirst().archive().idempotencyKey(),
				new ArchiveProcessingFingerprint(
						GdeltTestFixtures.EVENT_MD5,
						"gdelt-processing-v1",
						EVENT_FINGERPRINT));
	}

	@Test
	@DisplayName("Потеря global ownership после успешного результата не подтверждает индекс")
	void globalOwnershipLossAfterCompletedResultSkipsIndexedTransition() {
		IngestionRunState runState = runState(true);
		AtomicBoolean current = new AtomicBoolean(true);
		OperationBudget budget = guardedBudget(current);
		when(ingestionRunService.prepareOneShot(budget))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			current.set(false);
			return completed(request.kind(), successfulProgress());
		});

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
		verify(processingLedger, never()).register(
				runState.archives().getLast().archive().idempotencyKey(),
				mentionFingerprint());
	}

	@Test
	@DisplayName("Потеря global ownership после failure результата не записывает отказ")
	void globalOwnershipLossAfterFailedResultSkipsFailureTransition() {
		IngestionRunState runState = runState(false);
		AtomicBoolean current = new AtomicBoolean(true);
		OperationBudget budget = guardedBudget(current);
		when(ingestionRunService.prepareOneShot(budget))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(_ -> {
			current.set(false);
			return ArchiveProcessingResult.failed(
					GdeltArchiveKind.TRANSLATION_EVENTS,
					successfulProgress(),
					new ArchiveProcessingFailure(
							ArchiveProcessingErrorCode.BULK_PARTIAL_FAILURE,
							true,
							1L));
		});

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
	}

	@Test
	@DisplayName("Сигнал потери global ownership из processor не становится generic failure")
	void processorOwnershipSignalSkipsGenericFailureTransition() {
		IngestionRunState runState = runState(false);
		OperationOwnershipLostException ownershipLost =
				new OperationOwnershipLostException();
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenThrow(ownershipLost);

		assertThatThrownBy(() -> service.runOneShot()).isSameAs(ownershipLost);

		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
	}

	@Test
	@DisplayName("Deadline active processing сохраняется как retryable durable failure")
	void activeProcessingDeadlineIsPersistedAsRetryableFailure() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		ArchiveProcessingProgress partial = new ArchiveProcessingProgress(
				1, 0, 0, 0, 0, 0, 0, null);
		when(archiveProcessor.process(any(), any(), any())).thenReturn(
				ArchiveProcessingResult.failed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						partial,
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED,
								true,
								null)));

		assertThat(service.runOneShot())
				.isEqualTo(IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED);
		verify(processingLedger).markFailed(
				any(),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED.code(),
						true)),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
						1, 0, 0, 0, 0, 0, 0, null)),
				eq(null));
	}

	@Test
	@DisplayName("Истекшая deadline во время progress сохраняет partial counters и закрывает attempt")
	void expiredDeadlineDuringProgressPersistsPartialCounters() {
		IngestionRunState runState = runState(false);
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		when(ingestionRunService.prepareOneShot(budget))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		ArchiveProcessingProgress partial = new ArchiveProcessingProgress(
				2, 0, 1, 1, 1, 0, 0, null);
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingProgressListener listener = invocation.getArgument(1);
			nanoTime.set(Duration.ofSeconds(1).toNanos());
			listener.onProgress(partial);
			throw new AssertionError("Deadline signal must stop the processor callback");
		});

		assertThat(service.runOneShot(budget))
				.isEqualTo(IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED);

		verify(processingLedger).markFailed(
				any(),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED.code(),
						true)),
				eq(new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
						2, 0, 1, 1, 1, 0, 0, null)),
				eq(null));
		verify(processingLedger, never()).markIndexed(any(), any(), any());
	}

	@Test
	@DisplayName("Регистрирует, claim-ит и завершает оба staged архива")
	void processesBothStagedArchives() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			assertThat(request.indexTargets()).isEqualTo(ACTIVE_TARGETS);
			ArchiveProcessingProgressListener listener = invocation.getArgument(1);
			ArchiveProcessingProgress progress = successfulProgress();
			assertThat(listener.onProgress(progress)).isTrue();
			return completed(request.kind(), progress);
		});

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		verify(archiveProcessor, org.mockito.Mockito.times(2)).process(any(), any(), any());
		verify(processingLedger, org.mockito.Mockito.times(2))
				.markIndexed(any(), any(), any());
		verifyNoInteractions(indexWriter);
	}

	@Test
	@DisplayName("Откладывает processing до завершения maintenance")
	void defersProcessingWhilePartitionIsUnderMaintenance() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(indexTargetResolver.resolve(any(), any(OperationBudget.class))).thenReturn(
				IndexTargetResolution.outcome(
						IndexTargetResolutionStatus.MAINTENANCE_DEFERRED));

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		verifyNoInteractions(processingLedger, archiveProcessor, indexWriter);
	}

	@Test
	@DisplayName("Логирует redacted cleanup diagnostic при потере ownership")
	void logsCleanupDiagnosticAfterOwnershipLoss(CapturedOutput output) {
		IngestionRunState runState = runState(false);
		RuntimeException diagnostic = new RuntimeException("unsafe control signal");
		diagnostic.addSuppressed(new java.io.IOException("unsafe close details"));
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			ArchiveProcessingDiagnosticListener diagnosticListener =
					invocation.getArgument(2);
			diagnosticListener.onFailure(diagnostic);
			return ArchiveProcessingResult.ownershipLost(
					request.kind(),
					ArchiveProcessingProgress.empty());
		});

		assertThatThrownBy(service::runLatestUpdate)
				.isInstanceOf(OperationOwnershipLostException.class);

		assertThat(output.getAll())
				.contains("GDELT archive processing lost ownership with cleanup failure")
				.doesNotContain("unsafe control signal")
				.doesNotContain("unsafe close details");
		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
	}

	@Test
	@DisplayName("Write block после claim остается typed deferred outcome")
	void preservesWriteBlockedOutcomeAfterClaim() {
		assertPostClaimTargetOutcome(
				IndexTargetUnavailableReason.WRITE_BLOCKED,
				"DEFERRED_WRITE_BLOCKED",
				"INDEX_TARGET_WRITE_BLOCKED");
	}

	@Test
	@DisplayName("Missing target после claim остается typed ownership outcome")
	void preservesMissingTargetOutcomeAfterClaim() {
		assertPostClaimTargetOutcome(
				IndexTargetUnavailableReason.MISSING,
				"OWNERSHIP_LOST",
				"INDEX_TARGET_MISSING");
	}

	@Test
	@DisplayName("Замена target после claim завершает цикл потерей владения")
	void preservesReplacedTargetOutcomeAfterClaim() {
		assertPostClaimTargetOutcome(
				IndexTargetUnavailableReason.REPLACED,
				"OWNERSHIP_LOST",
				"INDEX_TARGET_REPLACED");
	}

	@Test
	@DisplayName("После смены generation обновляет matched receipt и пропускает INDEXED архив")
	void recordsMatchedReceiptAgainstCurrentGenerationBeforeSkip() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		ArchiveProcessingFingerprint fingerprint = eventFingerprint();
		ArchiveProcessingTargetBinding storedBinding = previousTargetBinding(
				GdeltIndexKind.EVENT);
		ArchiveProcessingTargetBinding currentBinding = targetBinding(GdeltIndexKind.EVENT);
		ArchiveReceiptVerification matched = verification(
				GdeltIndexKind.EVENT,
				1,
				1,
				ArchiveReceiptStatus.MATCHED);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(archive.archive().idempotencyKey(), fingerprint))
				.thenReturn(indexedState(archive, fingerprint, storedBinding));
		when(indexWriter.verifyReceipt(any(), any(OperationBudget.class))).thenReturn(matched);
		when(processingLedger.recordReceiptMatch(
				eq(archive.archive().idempotencyKey()),
				eq(fingerprint.processingFingerprint()),
				eq(1),
				eq(INDEXED_STATE_VERSION),
				eq(storedBinding),
				eq(currentBinding),
				same(matched)))
				.thenReturn(AttemptTransitionResult.APPLIED);

		service.runLatestUpdate();

		verify(processingLedger).recordReceiptMatch(
				archive.archive().idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				INDEXED_STATE_VERSION,
				storedBinding,
				currentBinding,
				matched);
		verify(indexWriter).verifyReceipt(
				org.mockito.ArgumentMatchers.argThat(query ->
						query.target().equals(ACTIVE_TARGETS.event())),
				any(OperationBudget.class));
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("После смены generation receipt mismatch передает stored и current bindings")
	void recordsReceiptMismatchAgainstChangedGenerationBindings() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		ArchiveProcessingFingerprint fingerprint = eventFingerprint();
		ArchiveProcessingTargetBinding storedBinding = previousTargetBinding(
				GdeltIndexKind.EVENT);
		ArchiveProcessingTargetBinding currentBinding = targetBinding(GdeltIndexKind.EVENT);
		ArchiveReceiptVerification mismatch = verification(
				GdeltIndexKind.EVENT,
				1,
				1,
				ArchiveReceiptStatus.IDENTITY_MISMATCH);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(archive.archive().idempotencyKey(), fingerprint))
				.thenReturn(indexedState(archive, fingerprint, storedBinding));
		when(indexWriter.verifyReceipt(any(), any(OperationBudget.class))).thenReturn(mismatch);
		when(processingLedger.recordReceiptMismatch(
				eq(archive.archive().idempotencyKey()),
				eq(fingerprint.processingFingerprint()),
				eq(1),
				eq(INDEXED_STATE_VERSION),
				eq(storedBinding),
				eq(currentBinding),
				same(mismatch),
				any()))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(service::runLatestUpdate)
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(processingLedger).recordReceiptMismatch(
				eq(archive.archive().idempotencyKey()),
				eq(fingerprint.processingFingerprint()),
				eq(1),
				eq(INDEXED_STATE_VERSION),
				eq(storedBinding),
				eq(currentBinding),
				same(mismatch),
				org.mockito.ArgumentMatchers.argThat(failure ->
						"INDEX_RECEIPT_MISMATCH".equals(failure.errorCode())
								&& failure.retryable()));
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Не запускает archive worker для отсутствующей ACTIVE generation")
	void defersMissingActiveGenerationWithoutAutoCreate() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(indexTargetResolver.resolve(any(), any(OperationBudget.class))).thenReturn(
				IndexTargetResolution.outcome(IndexTargetResolutionStatus.MISSING));

		service.runLatestUpdate();

		verifyNoInteractions(processingLedger, indexWriter, archiveProcessor);
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("ownershipStatusPaths")
	@DisplayName("Статус потери владения останавливает цикл до следующего архива")
	void ownershipStatusStopsBeforeNextArchive(OwnershipStatusPath path) {
		IngestionRunState runState = runState(true);
		IngestionArchiveState event = runState.archives().getFirst();
		IngestionArchiveState mention = runState.archives().getLast();
		String eventKey = event.archive().idempotencyKey();
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));

		switch (path) {
			case INDEX_TARGET_RESOLUTION -> when(indexTargetResolver.resolve(
					any(),
					any(OperationBudget.class))).thenReturn(
					IndexTargetResolution.outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST));
			case PROCESSING_CLAIM -> {
				when(processingLedger.register(eventKey, eventFingerprint()))
						.thenReturn(pendingState(eventKey, eventFingerprint()));
				when(processingLedger.claim(eq(eventKey), any(), any())).thenReturn(
						ArchiveProcessingClaimResult.outcome(
								ArchiveProcessingClaimStatus.OWNERSHIP_LOST));
			}
			case PROCESSOR_RESULT -> {
				stubPendingRegistrationAndClaims();
				when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
					ArchiveProcessingRequest request = invocation.getArgument(0);
					return ArchiveProcessingResult.ownershipLost(
							request.kind(),
							ArchiveProcessingProgress.empty());
				});
			}
			case INDEXED_TRANSITION -> {
				stubPendingRegistrationAndClaims();
				when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
					ArchiveProcessingRequest request = invocation.getArgument(0);
					return completed(request.kind(), successfulProgress());
				});
				when(processingLedger.markIndexed(any(), any(), any()))
						.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);
			}
		}

		assertThatThrownBy(service::runOneShot)
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(processingLedger, never()).register(
				mention.archive().idempotencyKey(),
				mentionFingerprint());
		verify(metrics).cycleDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.OWNERSHIP_LOST));
	}

	@Test
	@DisplayName("Лишний receipt фиксируется как non-retryable без нового attempt")
	void recordsReceiptSurplusWithoutClaim() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(any(), any(OperationBudget.class))).thenReturn(
				verification(
						GdeltIndexKind.EVENT,
						1,
						2,
						ArchiveReceiptStatus.SURPLUS));
		when(processingLedger.recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				anyLong(),
				any(),
				any(),
				any(),
				any()))
				.thenReturn(AttemptTransitionResult.APPLIED);

		service.runLatestUpdate();

		verify(processingLedger).recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				anyLong(),
				any(),
				any(),
				any(),
				org.mockito.ArgumentMatchers.argThat(failure ->
						"INDEX_RECEIPT_SURPLUS".equals(failure.errorCode())
								&& !failure.retryable()));
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Shortage receipt переоткрывает whole-archive replay в той же generation")
	void replaysWholeArchiveAfterReceiptShortage() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		String archiveKey = archive.archive().idempotencyKey();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(archiveKey, eventFingerprint()))
				.thenReturn(indexedState(archive, eventFingerprint()));
		when(indexWriter.verifyReceipt(any(), any(OperationBudget.class))).thenReturn(
				verification(
						GdeltIndexKind.EVENT,
						1,
						0,
						ArchiveReceiptStatus.SHORTAGE));
		when(processingLedger.recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				anyLong(),
				any(),
				any(),
				any(),
				any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.claim(
				org.mockito.ArgumentMatchers.eq(archiveKey),
				any(),
				any()))
				.thenAnswer(invocation -> claimedAttempt(
						archiveKey,
						eventFingerprint(),
						invocation.getArgument(1)));
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		verify(processingLedger).recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				anyLong(),
				any(),
				any(),
				any(),
				org.mockito.ArgumentMatchers.argThat(failure ->
						"INDEX_RECEIPT_MISMATCH".equals(failure.errorCode())
								&& failure.retryable()));
		verify(processingLedger).claim(
				org.mockito.ArgumentMatchers.eq(archiveKey),
				any(),
				any());
		verify(archiveProcessor).process(
				org.mockito.ArgumentMatchers.argThat(request ->
						request.sourceArchiveKey().equals(archiveKey)
								&& request.indexTargets().equals(ACTIVE_TARGETS)),
				any(),
				any());
		verify(processingLedger).markIndexed(any(), any(), any());
	}

	@Test
	@DisplayName("Ошибка receipt access не изменяет processing ledger")
	void propagatesReceiptAccessFailureWithoutLedgerMutation() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		IndexingAccessException failure =
				new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(
				any(ArchiveReceiptQuery.class),
				any(OperationBudget.class))).thenThrow(failure);

		assertThatThrownBy(service::runLatestUpdate).isSameAs(failure);

		verify(processingLedger, never())
				.recordReceiptMismatch(
						anyString(), anyString(), anyInt(), anyLong(), any(), any(), any(), any());
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Исчезновение target во время receipt дает ownership outcome без replay")
	void treatsMissingTargetDuringReceiptAsOwnershipOutcome() {
		IngestionRunState runState = runState(true);
		IngestionArchiveState event = runState.archives().getFirst();
		IngestionArchiveState mention = runState.archives().getLast();
		IndexTargetUnavailableException failure =
				new IndexTargetUnavailableException(IndexTargetUnavailableReason.MISSING);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(event.archive().idempotencyKey(), eventFingerprint()))
				.thenReturn(indexedState(event, eventFingerprint()));
		when(indexWriter.verifyReceipt(
				any(ArchiveReceiptQuery.class),
				any(OperationBudget.class))).thenThrow(failure);

		assertThatThrownBy(service::runLatestUpdate)
				.isInstanceOfSatisfying(
						OperationOwnershipLostException.class,
						ownershipLost -> assertThat(ownershipLost.getSuppressed())
								.containsExactly(failure));

		verify(processingLedger, never())
				.recordReceiptMismatch(
						anyString(), anyString(), anyInt(), anyLong(), any(), any(), any(), any());
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verify(processingLedger, never()).register(
				mention.archive().idempotencyKey(),
				mentionFingerprint());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Ошибка receipt Event не блокирует обработку Mention")
	void continuesWithMentionAfterEventReceiptAccessFailure() {
		IngestionRunState runState = runState(true);
		IngestionArchiveState eventArchive = runState.archives().get(0);
		IngestionArchiveState mentionArchive = runState.archives().get(1);
		String eventArchiveKey = eventArchive.archive().idempotencyKey();
		String mentionArchiveKey = mentionArchive.archive().idempotencyKey();
		IndexingAccessException failure =
				new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(eventArchiveKey, eventFingerprint()))
				.thenReturn(indexedState(eventArchive, eventFingerprint()));
		when(processingLedger.register(mentionArchiveKey, mentionFingerprint()))
				.thenReturn(pendingState(mentionArchiveKey, mentionFingerprint()));
		when(processingLedger.claim(
				org.mockito.ArgumentMatchers.eq(mentionArchiveKey),
				any(),
				any()))
				.thenAnswer(invocation -> claimedAttempt(
						mentionArchiveKey,
						mentionFingerprint(),
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(
				any(ArchiveReceiptQuery.class),
				any(OperationBudget.class))).thenThrow(failure);
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return completed(request.kind(), successfulProgress());
		});

		assertThatThrownBy(service::runLatestUpdate).isSameAs(failure);

		verify(archiveProcessor).process(
				org.mockito.ArgumentMatchers.argThat(request ->
						request.kind() == GdeltArchiveKind.TRANSLATION_MENTIONS),
				any(),
				any());
		verify(processingLedger).markIndexed(
				org.mockito.ArgumentMatchers.argThat(attempt ->
						mentionArchiveKey.equals(attempt.archiveIdempotencyKey())),
				any(),
				any());
		verify(processingLedger, never())
				.recordReceiptMismatch(
						anyString(), anyString(), anyInt(), anyLong(), any(), any(), any(), any());
	}

	@Test
	@DisplayName("Несколько ошибок receipt возвращаются первой с остальными suppressed")
	void aggregatesDeferredReceiptFailures() {
		IngestionRunState runState = runState(true);
		IngestionArchiveState eventArchive = runState.archives().get(0);
		IngestionArchiveState mentionArchive = runState.archives().get(1);
		IndexingAccessException firstFailure =
				new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		IndexingProtocolException secondFailure =
				new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(
				eventArchive.archive().idempotencyKey(),
				eventFingerprint()))
				.thenReturn(indexedState(eventArchive, eventFingerprint()));
		when(processingLedger.register(
				mentionArchive.archive().idempotencyKey(),
				mentionFingerprint()))
				.thenReturn(indexedState(mentionArchive, mentionFingerprint()));
		when(indexWriter.verifyReceipt(
				any(ArchiveReceiptQuery.class),
				any(OperationBudget.class)))
				.thenThrow(firstFailure, secondFailure);

		assertThatThrownBy(service::runLatestUpdate)
				.isSameAs(firstFailure)
				.satisfies(exception -> assertThat(exception.getSuppressed())
						.containsExactly(secondFailure));

		verify(indexWriter, org.mockito.Mockito.times(2))
				.verifyReceipt(
						any(ArchiveReceiptQuery.class),
						any(OperationBudget.class));
		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Failure Event не блокирует успешную обработку Mention")
	void processesMentionAfterEventFailure() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			if (request.kind() == GdeltArchiveKind.TRANSLATION_EVENTS) {
				ArchiveProcessingProgress failedProgress =
						new ArchiveProcessingProgress(1, 0, 0, 1, 0, 1, 0, 1L);
				return ArchiveProcessingResult.failed(
						request.kind(),
						failedProgress,
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.BULK_PARTIAL_FAILURE,
								true,
								1L));
			}
			return completed(request.kind(), successfulProgress());
		});

		service.runLatestUpdate();

		verify(processingLedger).markFailed(
				org.mockito.ArgumentMatchers.argThat(attempt ->
						attempt.archiveIdempotencyKey().contains(".export.")),
				any(),
				any(),
				any());
		verify(processingLedger).markIndexed(
				org.mockito.ArgumentMatchers.argThat(attempt ->
						attempt.archiveIdempotencyKey().contains(".mentions.")),
				any(),
				any());
	}

	@Test
	@DisplayName("Прерывание CSV durable фиксируется и немедленно останавливает cycle")
	void propagatesCooperativeInterruptionAfterDurableFailure() {
		IngestionRunState runState = runState(true);
		GdeltCsvInterruptedException diagnostic = new GdeltCsvInterruptedException(
				new java.io.IOException("local read was interrupted"));
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingDiagnosticListener diagnosticListener =
					invocation.getArgument(2);
			diagnosticListener.onFailure(diagnostic);
			return ArchiveProcessingResult.failed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						ArchiveProcessingProgress.empty(),
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED,
								true,
								null));
		});

		try {
			assertThatThrownBy(service::runLatestUpdate)
					.isInstanceOfSatisfying(
							IngestionInterruptedException.class,
							exception -> assertThat(exception.getCause())
									.isSameAs(diagnostic));

			assertThat(Thread.currentThread().isInterrupted()).isTrue();
			verify(processingLedger).markFailed(any(), any(), any(), any());
			verify(archiveProcessor).process(any(), any(), any());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Ошибка фиксации не подменяет cooperative interruption")
	void preservesInterruptionWhenFailurePersistenceFails() {
		IngestionRunState runState = runState(true);
		IllegalStateException persistenceFailure =
				new IllegalStateException("database unavailable");
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenReturn(
				ArchiveProcessingResult.failed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						ArchiveProcessingProgress.empty(),
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED,
								true,
								null)));
		when(processingLedger.markFailed(any(), any(), any(), any()))
				.thenThrow(persistenceFailure);

		try {
			assertThatThrownBy(service::runLatestUpdate)
					.isInstanceOfSatisfying(
							IngestionInterruptedException.class,
							exception -> assertThat(exception.getSuppressed())
									.containsExactly(persistenceFailure));

			assertThat(Thread.currentThread().isInterrupted()).isTrue();
			verify(archiveProcessor).process(any(), any(), any());
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Unexpected runtime durable фиксируется и не запускает второй архив")
	void abortsCycleImmediatelyAfterUnexpectedRuntime() {
		IngestionRunState runState = runState(true);
		IllegalStateException defect = new IllegalStateException("programming defect");
		ArchiveProcessingProgress lastProgress =
				new ArchiveProcessingProgress(2, 0, 1, 1, 1, 0, 0, null);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingProgressListener listener = invocation.getArgument(1);
			assertThat(listener.onProgress(lastProgress)).isTrue();
			throw defect;
		});

		assertThatThrownBy(service::runLatestUpdate).isSameAs(defect);

		verify(processingLedger).markFailed(
				any(),
				org.mockito.ArgumentMatchers.argThat(failure ->
						IngestionErrorCode.INTERNAL_ERROR.code()
								.equals(failure.errorCode())
								&& !failure.retryable()),
				org.mockito.ArgumentMatchers.argThat(progress ->
						progress.deliveredRecords() == 2
								&& progress.mappingRejectedRecords() == 1
								&& progress.succeededOperations() == 1),
				any());
		verify(archiveProcessor).process(any(), any(), any());
	}

	@Test
	@DisplayName("Потеря права на обработку при фиксации сбоя не запускает следующий архив")
	void unexpectedFailureTransitionOwnershipLossStopsBeforeNextArchive() {
		IngestionRunState runState = runState(true);
		IngestionArchiveState mention = runState.archives().getLast();
		IllegalStateException defect = new IllegalStateException("programming defect");
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenThrow(defect);
		when(processingLedger.markFailed(any(), any(), any(), any()))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(service::runOneShot)
				.isInstanceOf(OperationOwnershipLostException.class)
				.isNotSameAs(defect);

		verify(processingLedger, never()).register(
				mention.archive().idempotencyKey(),
				mentionFingerprint());
		verify(metrics).cycleDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.OWNERSHIP_LOST));
		verify(metrics, never()).cycleDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.INTERNAL_FAILURE));
	}

	@Test
	@DisplayName("Отказ диагностической записи не подменяет unexpected runtime")
	void preservesUnexpectedRuntimeWhenDiagnosticPersistenceFails() {
		IngestionRunState runState = runState(true);
		IllegalStateException defect = new IllegalStateException("programming defect");
		IllegalStateException persistenceFailure =
				new IllegalStateException("database unavailable");
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenThrow(defect);
		when(processingLedger.markFailed(any(), any(), any(), any()))
				.thenThrow(persistenceFailure);

		assertThatThrownBy(service::runLatestUpdate)
				.isSameAs(defect)
				.satisfies(exception -> assertThat(exception.getSuppressed())
						.containsExactly(persistenceFailure));

		verify(archiveProcessor).process(any(), any(), any());
	}

	private static Stream<OwnershipStatusPath> ownershipStatusPaths() {
		return Stream.of(OwnershipStatusPath.values());
	}

	private void assertCatalogValidationMapping(
			IngestionErrorCode errorCode,
			Class<? extends RuntimeException> expectedType,
			boolean retryable
	) {
		IngestionRunState latest = runState(true);
		service = newService(GdeltTestFixtures.properties(
				tempDir,
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(latest), false));
		when(recentRecoveryPlanLedger.currentPlan()).thenReturn(Optional.of(recentPlan(
				RecentRecoveryPlanLedger.CatalogStatus.PENDING)));
		stubIndexedRegistrations(latest);
		when(masterCatalogClient.fetchCatalog(any(), any(), any()))
				.thenThrow(new GdeltMasterCatalogValidationException(errorCode));

		assertThatThrownBy(service::runOneShot)
				.isInstanceOf(expectedType)
				.satisfies(exception -> {
					IngestionFailureContract contract = (IngestionFailureContract) exception;
					assertThat(contract.failure().code()).isEqualTo(errorCode);
					assertThat(contract.failure().retryable()).isEqualTo(retryable);
				});
	}

	private RecentRecoveryPlanLedger.PlanState recentPlan(
			RecentRecoveryPlanLedger.CatalogStatus status
	) {
		RecentWindowPlan plan = RecentWindowPlan.fromBoundaries(
				GdeltTestFixtures.UPDATE_TIME.minus(Duration.ofHours(24)),
				GdeltTestFixtures.UPDATE_TIME,
				GdeltTestFixtures.UPDATE_TIME);
		var revision = new RecentRecoveryPlanLedger.Revision(
				1,
				plan.windowFrom(),
				plan.windowTo(),
				plan.sourceFrontier());
		if (status == RecentRecoveryPlanLedger.CatalogStatus.PENDING) {
			return new RecentRecoveryPlanLedger.PlanState(
					revision,
					status,
					null,
					null,
					null,
					null,
					0);
		}
		return new RecentRecoveryPlanLedger.PlanState(
				revision,
				status,
				"42",
				"master-etag",
				plan.windowFrom(),
				NOW,
				1);
	}

	private IngestionArchiveState archiveState(
			Instant updateTime,
			ArchiveType archiveType
	) {
		return runState(updateTime, true).archives().stream()
				.filter(state -> state.archive().archiveType() == archiveType)
				.findFirst()
				.orElseThrow();
	}

	private void stubIndexedRegistrations(IngestionRunState runState) {
		when(processingLedger.register(anyString(), any())).thenAnswer(invocation -> {
			String archiveKey = invocation.getArgument(0);
			IngestionArchiveState archiveState = runState.archives().stream()
					.filter(state -> state.archive().idempotencyKey().equals(archiveKey))
					.findFirst()
					.orElseThrow();
			return indexedState(archiveState, invocation.getArgument(1));
		});
	}

	private static IngestionArchiveState archiveAt(
			Instant updateTime,
			ArchiveType archiveType
	) {
		return org.mockito.ArgumentMatchers.argThat(state ->
				state.archive().sourceUpdateTime().equals(updateTime)
						&& state.archive().archiveType() == archiveType);
	}

	private static ArchiveProcessingRequest requestAt(
			Instant updateTime,
			GdeltArchiveKind archiveKind
	) {
		return org.mockito.ArgumentMatchers.argThat(request ->
				request.sourceUpdateTime().equals(updateTime)
						&& request.kind() == archiveKind);
	}

	private void stubPendingRegistrationAndClaims() {
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> pendingState(
						invocation.getArgument(0),
						invocation.getArgument(1)));
		when(processingLedger.claim(anyString(), any(), any())).thenAnswer(invocation -> {
			String archiveKey = invocation.getArgument(0);
			boolean event = archiveKey.contains(".export.");
			return claimedAttempt(
					archiveKey,
					event ? eventFingerprint() : mentionFingerprint(),
					invocation.getArgument(1));
		});
	}

	private static OperationBudget guardedBudget(AtomicBoolean current) {
		return OperationBudget
				.start(Duration.ofMinutes(1))
				.withLeaseGuard(
						() -> current.get()
								? OperationLeaseSnapshot.current(Duration.ofMinutes(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);
	}

	private void assertPostClaimTargetOutcome(
			IndexTargetUnavailableReason reason,
			String expectedOutcome,
			String expectedErrorCode
	) {
		IngestionRunState runState = runState(false);
		IndexTargetUnavailableException failure = new IndexTargetUnavailableException(reason);
		when(ingestionRunService.prepareOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.of(runState), false));
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenThrow(failure);

		if ("OWNERSHIP_LOST".equals(expectedOutcome)) {
			assertThatThrownBy(service::runOneShot)
					.isInstanceOfSatisfying(
							OperationOwnershipLostException.class,
							ownershipLost -> assertThat(ownershipLost.getSuppressed())
									.containsExactly(failure));
			verify(metrics).cycleDuration(
					anyLong(),
					eq(IngestionOperationMetricOutcome.OWNERSHIP_LOST));
		}
		else {
			assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.COMPLETED);
		}

		assertThat(GdeltPipelineService.targetUnavailableOutcome(reason))
				.isEqualTo(expectedOutcome);
		assertThat(reason.errorCode().code()).isEqualTo(expectedErrorCode);
		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
	}

	private IngestionRunState runState(boolean includeMention) {
		return runState(GdeltTestFixtures.UPDATE_TIME, includeMention);
	}

	private IngestionRunState runState(Instant updateTime, boolean includeMention) {
		var update = GdeltTestFixtures.update(updateTime);
		List<IngestionArchiveState> archives = update.archives().stream()
				.limit(includeMention ? 2 : 1)
				.map(archive -> new IngestionArchiveState(
						1,
						archive,
						IngestionArchiveStatus.STAGED,
						new ArchiveAttemptState(
								1,
								null,
								NOW,
								null,
								AutomaticRetryState.initial(3)),
						new StagedArchive(
								tempDir.resolve(archive.archiveType() + ".zip"),
								tempDir.resolve(archive.archiveType() + ".csv"),
								archive.expectedSizeBytes(),
								archive.expectedMd5()),
						null,
						NOW,
						NOW))
				.toList();
		return new IngestionRunState(
				1,
				update.sourceUpdateTime(),
				IngestionRunStatus.STAGED,
				NOW,
				NOW,
				null,
				archives);
	}

	private static ArchiveProcessingState pendingState(
			String archiveKey,
			ArchiveProcessingFingerprint fingerprint
	) {
		return new ArchiveProcessingState(
				archiveKey,
				fingerprint,
				ArchiveProcessingStatus.PENDING,
				0,
				new ArchiveProcessingAttemptState(
						0,
						null,
						null,
						null,
						AutomaticRetryState.initial(3)),
				null,
				com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress.empty(),
				null,
				null,
				NOW,
				null);
	}

	private static ArchiveProcessingState indexedState(
			IngestionArchiveState archive,
			ArchiveProcessingFingerprint fingerprint
	) {
		GdeltIndexKind kind = archive.archive().archiveType() ==
				com.neighbor.eventmosaic.ingestion.api.ArchiveType.TRANSLATION_EVENTS
				? GdeltIndexKind.EVENT
				: GdeltIndexKind.MENTION;
		return indexedState(archive, fingerprint, targetBinding(kind));
	}

	private static ArchiveProcessingState indexedState(
			IngestionArchiveState archive,
			ArchiveProcessingFingerprint fingerprint,
			ArchiveProcessingTargetBinding binding
	) {
		return new ArchiveProcessingState(
				archive.archive().idempotencyKey(),
				fingerprint,
				ArchiveProcessingStatus.INDEXED,
				INDEXED_STATE_VERSION,
				new ArchiveProcessingAttemptState(
						1,
						null,
						NOW,
						null,
						AutomaticRetryState.initial(3)),
				binding,
				new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
						1, 0, 0, 1, 1, 0, 1, null),
				new ArchiveProcessingReceipt(
						1,
						1,
						ArchiveIdentityDigest.ALGORITHM,
						RECEIPT_DIGEST.value(),
						RECEIPT_DIGEST.value(),
						binding.generationId(),
						binding.indexUuid(),
						NOW),
				null,
				NOW,
				NOW);
	}

	private static ArchiveProcessingFingerprint eventFingerprint() {
		return new ArchiveProcessingFingerprint(
				GdeltTestFixtures.EVENT_MD5,
				"gdelt-processing-v1",
				EVENT_FINGERPRINT);
	}

	private static ArchiveProcessingFingerprint mentionFingerprint() {
		return new ArchiveProcessingFingerprint(
				GdeltTestFixtures.MENTION_MD5,
				"gdelt-processing-v1",
				MENTION_FINGERPRINT);
	}

	private static ArchiveProcessingProgress successfulProgress() {
		return new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 1, null);
	}

	private static ArchiveProcessingResult completed(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress
	) {
		return ArchiveProcessingResult.completed(
				kind,
				progress,
				verification(
						kind == GdeltArchiveKind.TRANSLATION_EVENTS
								? GdeltIndexKind.EVENT
								: GdeltIndexKind.MENTION,
						1,
						1,
						ArchiveReceiptStatus.MATCHED));
	}

	private static ArchiveReceiptVerification verification(
			GdeltIndexKind kind,
			long expectedCount,
			long actualCount,
			ArchiveReceiptStatus status
	) {
		ArchiveIdentityDigest actualDigest = switch (status) {
			case MATCHED -> RECEIPT_DIGEST;
			case SHORTAGE -> EMPTY_DIGEST;
			case SURPLUS, IDENTITY_MISMATCH -> OTHER_DIGEST;
		};
		return new ArchiveReceiptVerification(
				kind,
				expectedCount,
				actualCount,
				RECEIPT_DIGEST,
				actualDigest,
				status);
	}

	private static ArchiveIdentityDigest digest(String... identities) {
		ArchiveIdentityDigest.Accumulator accumulator = ArchiveIdentityDigest.accumulator();
		for (String identity : identities) {
			accumulator.addIdentity(identity);
		}
		return accumulator.finish();
	}

	private static ArchiveProcessingClaimResult claimedAttempt(
			String archiveKey,
			ArchiveProcessingFingerprint fingerprint,
			ArchiveProcessingTargetBinding targetBinding
	) {
		return ArchiveProcessingClaimResult.claimed(new ArchiveProcessingAttempt(
				archiveKey,
				fingerprint,
				targetBinding,
				UUID.randomUUID(),
				NOW.plusSeconds(900),
				1,
				false));
	}

	private static ArchiveProcessingTargetBinding targetBinding(GdeltIndexKind kind) {
		var target = ACTIVE_TARGETS.target(kind);
		return new ArchiveProcessingTargetBinding(
				kind,
				ACTIVE_TARGETS.partitionKey(),
				ACTIVE_TARGETS.partitionStateVersion(),
				ACTIVE_TARGETS.generationId(),
				ACTIVE_TARGETS.generationUuid(),
				target.indexName(),
				target.indexUuid());
	}

	private static ArchiveProcessingTargetBinding previousTargetBinding(
			GdeltIndexKind kind
	) {
		String indexName = switch (kind) {
			case EVENT -> "gdelt-events-v1-p20260727-g0000";
			case MENTION -> "gdelt-mentions-v1-p20260727-g0000";
		};
		String indexUuid = kind == GdeltIndexKind.EVENT
				? "previous-event-index-uuid"
				: "previous-mention-index-uuid";
		return new ArchiveProcessingTargetBinding(
				kind,
				ACTIVE_TARGETS.partitionKey(),
				ACTIVE_TARGETS.partitionStateVersion() - 1,
				ACTIVE_TARGETS.generationId() - 1,
				UUID.fromString("22222222-2222-2222-2222-222222222222"),
				indexName,
				indexUuid);
	}

	private enum OwnershipStatusPath {
		INDEX_TARGET_RESOLUTION("разрешение индекса потеряло владение"),
		PROCESSING_CLAIM("захват обработки потерял владение"),
		PROCESSOR_RESULT("обработчик сообщил о потере владения"),
		INDEXED_TRANSITION("фиксация индекса потеряла владение");

		private final String description;

		OwnershipStatusPath(String description) {
			this.description = description;
		}

		@Override
		public String toString() {
			return description;
		}
	}
}
