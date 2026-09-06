package com.neighbor.eventmosaic.ingestion;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.archive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttempt;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.TransferredArtifactIntegrityException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.observability.StoragePressureState;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestClient;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestParser;
import com.neighbor.eventmosaic.ingestion.staging.DownloadedArchive;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.StagingPaths;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Оркестрация цикла загрузки")
class IngestionRunServiceTest {

	private GdeltManifestClient manifestClient;
	private GdeltManifestParser manifestParser;
	private IngestionArchiveLedger ledger;
	private RecentRecoveryPlanLedger recentRecoveryPlanLedger;
	private SourcePollLedger sourcePollLedger;
	private StagingLayout layout;
	private HttpArchiveDownloader downloader;
	private ZipArchiveStager stager;
	private IngestionMetrics metrics;
	private BackendDataStorageMonitor storageMonitor;
	private IngestionRunService service;

	@BeforeEach
	void setUp() {
		manifestClient = mock(GdeltManifestClient.class);
		manifestParser = mock(GdeltManifestParser.class);
		ledger = mock(IngestionArchiveLedger.class);
		recentRecoveryPlanLedger = mock(RecentRecoveryPlanLedger.class);
		sourcePollLedger = mock(SourcePollLedger.class);
		layout = mock(StagingLayout.class);
		downloader = mock(HttpArchiveDownloader.class);
		stager = mock(ZipArchiveStager.class);
		metrics = mock(IngestionMetrics.class);
		storageMonitor = mock(BackendDataStorageMonitor.class);
		service = newService(properties());
	}

	private IngestionRunService newService(GdeltIngestionProperties properties) {
		return new IngestionRunService(
				manifestClient,
				manifestParser,
				ledger,
				recentRecoveryPlanLedger,
				GdeltTestFixtures.recentWindowPlanner(),
				sourcePollLedger,
				layout,
				downloader,
				stager,
				properties,
				metrics,
				storageMonitor
		);
	}

	@Test
	@DisplayName("Остановленный worker не получает новую попытку и не обращается к источнику")
	void interruptedWorkerDoesNotStartSourceClaim() {
		Thread.currentThread().interrupt();
		try {
			assertThatThrownBy(() -> service.prepareOneShot(OperationBudget.start(Duration.ofMinutes(1))))
					.isInstanceOf(IngestionInterruptedException.class);
			verifyNoInteractions(sourcePollLedger, ledger, manifestClient, downloader, stager);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Ошибка discovery останавливает цикл до регистрации обновления")
	void discoveryFailureStopsBeforeRegistration() {
		RemoteSourceAccessException failure = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR,
				new IOException("manifest unavailable"));
		when(manifestClient.fetchLatestManifest()).thenThrow(failure);

		assertThatThrownBy(service::runLatestUpdate).isSameAs(failure);

		verify(metrics).runStarted();
		verify(metrics).error(IngestionErrorCode.MANIFEST_HTTP_ERROR);
		verifyNoInteractions(manifestParser, ledger, downloader, stager);
	}

	@Test
	@DisplayName("Storage pressure останавливает загрузку до claim и source I/O")
	void storagePressureStopsBeforeClaimAndSourceIo() {
		DiscoveredUpdate update = update();
		StoragePressureException pressure = new StoragePressureException(
				StorageResource.STAGING,
				StoragePressureState.PRESSURE);
		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		doThrow(pressure)
				.when(storageMonitor)
				.requireCapacity(StorageResource.STAGING);

		assertThatThrownBy(service::runLatestUpdate).isSameAs(pressure);

		verify(ledger, never()).claimArchive(any(), any());
		verifyNoInteractions(downloader, stager);
		verify(metrics, never()).error(IngestionErrorCode.STORAGE_PRESSURE);
	}

	@Test
	@DisplayName("Ранний one-shot возвращает deferred без ожидания и source I/O")
	void earlyOneShotReturnsDeferredWithoutSourceIo() {
		when(sourcePollLedger.claim(
				GdeltSourceContract.SOURCE_NAME,
				properties().continuity().recoveryLease()))
				.thenReturn(Optional.empty());
		when(ledger.findLatestRun()).thenReturn(Optional.empty());

		AcquisitionCycleResult result = service.runOneShot(
				OperationBudget.start(Duration.ofMinutes(1)));

		assertThat(result.sourcePollDeferred()).isTrue();
		assertThat(result.runState()).isEmpty();
		verifyNoInteractions(manifestClient, manifestParser, downloader, stager);
	}

	@Test
	@DisplayName("Подготовка one-shot регистрирует обновление без загрузки архивов")
	void preparedOneShotStopsBeforeArchiveAcquisition() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		DiscoveredUpdate update = update();
		IngestionRunState registeredRun = runState(IngestionRunStatus.PARTIAL);
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(1));
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, token))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.findRunByUpdateTime(update.sourceUpdateTime()))
				.thenReturn(Optional.of(registeredRun));

		AcquisitionCycleResult result = service.prepareOneShot(budget);

		assertThat(result.runState()).containsSame(registeredRun);
		assertThat(result.sourcePollDeferred()).isFalse();
		verify(ledger, never()).claimArchive(any(), any());
		verifyNoInteractions(layout, downloader, stager);
	}

	@Test
	@DisplayName("Политика недавнего окна атомарно активирует план вместе со свежей парой")
	void recentWindowPolicyActivatesPlanInsteadOfLegacyContinuityRegistration() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		DiscoveredUpdate update = update();
		RecentWindowPlan expectedPlan = GdeltTestFixtures.recentWindowPlanner()
				.plan(update.sourceUpdateTime());
		IngestionRunState registeredRun = runState(IngestionRunStatus.PARTIAL);
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(1));
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(recentRecoveryPlanLedger.activate(update, expectedPlan)).thenReturn(
				new RecentRecoveryPlanLedger.ActivationResult(
						RecentRecoveryPlanLedger.ActivationOutcome.ACTIVATED,
						Optional.empty(),
						2));
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, token))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.findRunByUpdateTime(update.sourceUpdateTime()))
				.thenReturn(Optional.of(registeredRun));
		service = newService(GdeltTestFixtures.properties(
				Path.of("build", "recent-window-test"),
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));

		AcquisitionCycleResult result = service.prepareOneShot(budget);

		assertThat(result.runState()).containsSame(registeredRun);
		verify(recentRecoveryPlanLedger).activate(update, expectedPlan);
		verify(ledger, never()).registerDiscoveredUpdate(any(), any(), any());
		verify(metrics).gapsCreated(2);
	}

	@Test
	@DisplayName("Устаревший latest manifest продолжает более новый durable запуск без отката состояния")
	void staleRecentManifestUsesStrictlyNewerDurableRun() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:16:00Z"),
				1,
				false);
		DiscoveredUpdate observed = update();
		RecentWindowPlan observedPlan = GdeltTestFixtures.recentWindowPlanner()
				.plan(observed.sourceUpdateTime());
		IngestionRunState durableLatest = new IngestionRunState(
				2,
				Instant.parse("2026-07-20T12:15:00Z"),
				IngestionRunStatus.STAGED,
				Instant.parse("2026-07-20T12:15:30Z"),
				Instant.parse("2026-07-20T12:16:00Z"),
				null,
				List.of());
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(1));
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(observed);
		when(recentRecoveryPlanLedger.activate(observed, observedPlan)).thenReturn(
				new RecentRecoveryPlanLedger.ActivationResult(
						RecentRecoveryPlanLedger.ActivationOutcome.STALE_FRONTIER,
						Optional.empty(),
						0));
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, token))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.findRunByUpdateTime(observed.sourceUpdateTime()))
				.thenReturn(Optional.empty());
		when(ledger.findLatestRun()).thenReturn(Optional.of(durableLatest));
		service = newService(GdeltTestFixtures.properties(
				Path.of("build", "stale-recent-test"),
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));

		AcquisitionCycleResult result = service.prepareOneShot(budget);

		assertThat(result.runState()).containsSame(durableLatest);
		verify(recentRecoveryPlanLedger).activate(observed, observedPlan);
		verify(ledger, never()).registerDiscoveredUpdate(any(), any(), any());
		verifyNoInteractions(layout, downloader, stager);
	}

	@Test
	@DisplayName("Устаревший latest manifest не выбирает старый exact run вместо нового durable запуска")
	void staleRecentManifestPrefersNewerDurableRunOverExistingExactRun() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:16:00Z"),
				1,
				false);
		DiscoveredUpdate observed = update();
		RecentWindowPlan observedPlan = GdeltTestFixtures.recentWindowPlanner()
				.plan(observed.sourceUpdateTime());
		IngestionRunState exactOld = runState(IngestionRunStatus.STAGED);
		IngestionRunState durableLatest = new IngestionRunState(
				2,
				Instant.parse("2026-07-20T12:15:00Z"),
				IngestionRunStatus.PARTIAL,
				Instant.parse("2026-07-20T12:15:30Z"),
				null,
				null,
				List.of());
		OperationBudget budget = OperationBudget.start(Duration.ofMinutes(1));
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(observed);
		when(recentRecoveryPlanLedger.activate(observed, observedPlan)).thenReturn(
				new RecentRecoveryPlanLedger.ActivationResult(
						RecentRecoveryPlanLedger.ActivationOutcome.STALE_FRONTIER,
						Optional.empty(),
						0));
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, token))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.findRunByUpdateTime(observed.sourceUpdateTime()))
				.thenReturn(Optional.of(exactOld));
		when(ledger.findLatestRun()).thenReturn(Optional.of(durableLatest));
		service = newService(GdeltTestFixtures.properties(
				Path.of("build", "stale-recent-exact-test"),
				1024 * 1024,
				FirstRunPolicy.RECENT_WINDOW));

		AcquisitionCycleResult result = service.prepareOneShot(budget);

		assertThat(result.runState()).containsSame(durableLatest);
		verify(recentRecoveryPlanLedger).activate(observed, observedPlan);
		verify(ledger, never()).registerDiscoveredUpdate(any(), any(), any());
		verifyNoInteractions(layout, downloader, stager);
	}

	@Test
	@DisplayName("Source poll failure передает Retry-After в durable retry schedule")
	void sourcePollFailurePersistsRetryAfter() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		RemoteSourceAccessException failure = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR,
				com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext.forHttpStatus(429),
				Duration.ofMinutes(5));
		when(manifestClient.fetchLatestManifest(any(OperationBudget.class)))
				.thenThrow(failure);

		assertThatThrownBy(() -> service.runOneShot(
				OperationBudget.start(Duration.ofMinutes(1))))
				.isSameAs(failure);
		verify(sourcePollLedger).markFailed(
				GdeltSourceContract.SOURCE_NAME,
				token,
				failure.failure(),
				Duration.ofMinutes(5));
	}

	@Test
	@DisplayName("Потеря права на опрос источника при фиксации отказа останавливает цикл")
	void sourcePollFailureTransitionOwnershipLossReplacesExpectedFailure() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		RemoteSourceAccessException sourceFailure = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR);
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(any(OperationBudget.class)))
				.thenThrow(sourceFailure);
		when(sourcePollLedger.markFailed(
				GdeltSourceContract.SOURCE_NAME,
				token,
				sourceFailure.failure()))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(() -> service.runOneShot(
				OperationBudget.start(Duration.ofMinutes(1))))
				.isInstanceOf(OperationOwnershipLostException.class)
				.isNotSameAs(sourceFailure);

		verifyNoInteractions(manifestParser, ledger, layout, downloader, stager);
		verify(metrics, never()).error(IngestionErrorCode.MANIFEST_HTTP_ERROR);
	}

	@Test
	@DisplayName("Истекшая deadline source poll условно закрывает активную попытку")
	void expiredSourcePollDeadlinePersistsRetryableFailure() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		OperationDeadlineExceededException deadline = new OperationDeadlineExceededException();
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenAnswer(_ -> {
			nanoTime.set(Duration.ofSeconds(1).toNanos());
			throw deadline;
		});

		assertThatThrownBy(() -> service.runOneShot(budget)).isSameAs(deadline);

		verify(sourcePollLedger).markFailed(
				GdeltSourceContract.SOURCE_NAME,
				token,
				deadline.failure());
		verifyNoInteractions(manifestParser, ledger, downloader, stager);
	}

	@Test
	@DisplayName("Потеря global ownership после отказа source не записывает failure")
	void globalOwnershipLossAfterSourceFailureSkipsFailureTransition() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		AtomicBoolean current = new AtomicBoolean(true);
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(1))
				.withLeaseGuard(
						() -> current.get()
								? OperationLeaseSnapshot.current(Duration.ofMinutes(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);
		RemoteSourceAccessException sourceFailure = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR);
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(budget)).thenAnswer(_ -> {
			current.set(false);
			throw sourceFailure;
		});

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(sourcePollLedger, never()).markFailed(any(), any(), any(), any());
		verifyNoInteractions(manifestParser, ledger, layout, downloader, stager);
		verify(metrics, never()).error(IngestionErrorCode.INTERNAL_ERROR);
	}

	@Test
	@DisplayName("Потеря source poll token запрещает дальнейшую работу с архивами")
	void sourcePollOwnershipLossStopsBeforeArchiveIo() {
		UUID token = UUID.randomUUID();
		SourcePollAttempt attempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				token,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		DiscoveredUpdate update = update();
		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(attempt));
		when(manifestClient.fetchLatestManifest(any(OperationBudget.class)))
				.thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, token))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		AcquisitionCycleResult result = service.runOneShot(
				OperationBudget.start(Duration.ofMinutes(1)));

		assertThat(result.runState()).isEmpty();
		assertThat(result.sourcePollDeferred()).isFalse();
		assertThat(result.sourcePollOwnershipLost()).isTrue();
		verify(metrics).acquisitionDuration(
				anyLong(),
				eq(IngestionOperationMetricOutcome.OWNERSHIP_LOST));
		verify(ledger, never()).claimArchive(any(), any());
		verify(ledger, never()).findRunByUpdateTime(any());
		verifyNoInteractions(layout, downloader, stager);
	}

	@Test
	@DisplayName("Потеря global ownership после staging запрещает durable переход и следующий архив")
	void globalOwnershipLossAfterStagingStopsBeforeDurableTransition() {
		UUID pollToken = UUID.randomUUID();
		SourcePollAttempt pollAttempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				pollToken,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		DiscoveredArchive mentions = update.archives().getLast();
		ArchiveAttempt archiveAttempt = attempt(events, 1, false);
		StagingPaths paths = paths("events-global-ownership");
		DownloadedArchive downloaded = new DownloadedArchive(
				paths.archivePath(),
				events.expectedSizeBytes(),
				events.expectedMd5(),
				false);
		StagedArchive staged = new StagedArchive(
				paths.archivePath(),
				paths.csvPath(),
				events.expectedSizeBytes(),
				events.expectedMd5());
		AtomicBoolean current = new AtomicBoolean(true);
		OperationBudget budget = OperationBudget
				.start(Duration.ofMinutes(1))
				.withLeaseGuard(
						() -> current.get()
								? OperationLeaseSnapshot.current(Duration.ofMinutes(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);

		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(pollAttempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, pollToken))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)))
				.thenReturn(Optional.of(archiveAttempt));
		when(layout.pathsFor(archiveAttempt)).thenReturn(paths);
		when(downloader.download(archiveAttempt, paths, budget)).thenReturn(downloaded);
		when(stager.stage(archiveAttempt, downloaded, paths, budget)).thenAnswer(_ -> {
			current.set(false);
			return staged;
		});

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(ledger, never()).markStaged(events.idempotencyKey(), archiveAttempt.token(), staged);
		verify(ledger, never()).markFailed(
				events.idempotencyKey(),
				archiveAttempt.token(),
				IngestionFailure.internalError());
		verify(ledger, never()).claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15));
	}

	@Test
	@DisplayName("Истекшая deadline после скачивания условно закрывает попытку архива")
	void expiredArchiveDeadlinePersistsRetryableFailure() {
		UUID pollToken = UUID.randomUUID();
		SourcePollAttempt pollAttempt = new SourcePollAttempt(
				GdeltSourceContract.SOURCE_NAME,
				pollToken,
				Instant.parse("2026-07-20T12:01:00Z"),
				1,
				false);
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		DiscoveredArchive mentions = update.archives().getLast();
		ArchiveAttempt archiveAttempt = attempt(events, 1, false);
		StagingPaths paths = paths("events-deadline");
		DownloadedArchive downloaded = new DownloadedArchive(
				paths.archivePath(),
				events.expectedSizeBytes(),
				events.expectedMd5(),
				false);
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);

		when(sourcePollLedger.claim(any(), any())).thenReturn(Optional.of(pollAttempt));
		when(manifestClient.fetchLatestManifest(budget)).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(sourcePollLedger.markSucceeded(GdeltSourceContract.SOURCE_NAME, pollToken))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)))
				.thenReturn(Optional.of(archiveAttempt));
		when(layout.pathsFor(archiveAttempt)).thenReturn(paths);
		when(downloader.download(archiveAttempt, paths, budget)).thenAnswer(_ -> {
			nanoTime.set(Duration.ofSeconds(1).toNanos());
			return downloaded;
		});
		when(ledger.markFailed(
				events.idempotencyKey(),
				archiveAttempt.token(),
				new OperationDeadlineExceededException().failure()))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationDeadlineExceededException.class);

		verify(ledger).markFailed(
				events.idempotencyKey(),
				archiveAttempt.token(),
				new OperationDeadlineExceededException().failure());
		verifyNoInteractions(stager);
		verify(ledger, never()).claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15));
	}

	@Test
	@DisplayName("Исчерпанная one-shot deadline не создает source poll state или token")
	void expiredOneShotDeadlinePreventsPollIo() {
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		nanoTime.set(Duration.ofSeconds(1).toNanos());

		assertThatThrownBy(() -> service.runOneShot(budget))
				.isInstanceOf(OperationDeadlineExceededException.class);
		verifyNoInteractions(sourcePollLedger, manifestClient, manifestParser, ledger, downloader, stager);
	}

	@Test
	@DisplayName("Архив событий подготавливается, а ошибка mentions оставляет запуск частичным")
	void stagesEventAndKeepsMentionFailureAsPartialRun() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		DiscoveredArchive mentions = update.archives().getLast();
		ArchiveAttempt eventAttempt = attempt(events, 1, false);
		ArchiveAttempt mentionAttempt = attempt(mentions, 1, false);
		StagingPaths eventPaths = paths("events");
		StagingPaths mentionPaths = paths("mentions");
		DownloadedArchive downloadedEvent = new DownloadedArchive(
				eventPaths.archivePath(), events.expectedSizeBytes(), events.expectedMd5(), false);
		StagedArchive stagedEvent = new StagedArchive(
				eventPaths.archivePath(), eventPaths.csvPath(), events.expectedSizeBytes(), events.expectedMd5());
		IngestionRunState finalRun = runState(IngestionRunStatus.PARTIAL);

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(1);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(eventAttempt));
		when(ledger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(mentionAttempt));
		when(layout.pathsFor(eventAttempt)).thenReturn(eventPaths);
		when(layout.pathsFor(mentionAttempt)).thenReturn(mentionPaths);
		when(downloader.download(eventAttempt, eventPaths)).thenReturn(downloadedEvent);
		TransferredArtifactIntegrityException mentionException = new TransferredArtifactIntegrityException(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
		when(downloader.download(mentionAttempt, mentionPaths)).thenThrow(mentionException);
		when(stager.stage(eventAttempt, downloadedEvent, eventPaths)).thenReturn(stagedEvent);
		when(ledger.markStaged(events.idempotencyKey(), eventAttempt.token(), stagedEvent))
				.thenReturn(AttemptTransitionResult.APPLIED);
		IngestionFailure mentionFailure = mentionException.failure();
		when(ledger.markFailed(
				mentions.idempotencyKey(),
				mentionAttempt.token(),
				mentionFailure)).thenReturn(AttemptTransitionResult.APPLIED);
		when(ledger.findRunByUpdateTime(update.sourceUpdateTime())).thenReturn(Optional.of(finalRun));

		assertThat(service.runLatestUpdate()).isSameAs(finalRun);

		verify(ledger).markStaged(events.idempotencyKey(), eventAttempt.token(), stagedEvent);
		verify(ledger).markFailed(
				mentions.idempotencyKey(),
				mentionAttempt.token(),
				mentionFailure);
		verify(metrics).gapsCreated(1);
		verify(metrics).event(IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE);
		verify(metrics).runCompleted(IngestionRunStatus.PARTIAL);
	}

	@Test
	@DisplayName("Потеря ownership прекращает устаревшую попытку без ложного failure")
	void ownershipLossStopsStaleAttemptWithoutMarkingFailure() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		ArchiveAttempt attempt = attempt(events, 2, true);
		StagingPaths eventPaths = paths("events");
		DownloadedArchive downloaded = new DownloadedArchive(
				eventPaths.archivePath(), events.expectedSizeBytes(), events.expectedMd5(), false);
		StagedArchive staged = new StagedArchive(
				eventPaths.archivePath(), eventPaths.csvPath(), events.expectedSizeBytes(), events.expectedMd5());
		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenReturn(eventPaths);
		when(downloader.download(attempt, eventPaths)).thenReturn(downloaded);
		when(stager.stage(attempt, downloaded, eventPaths)).thenReturn(staged);
		when(ledger.markStaged(events.idempotencyKey(), attempt.token(), staged))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(service::runLatestUpdate)
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(ledger).markStaged(events.idempotencyKey(), attempt.token(), staged);
		verify(ledger, never()).markFailed(events.idempotencyKey(), attempt.token(),
				IngestionFailure.internalError());
		verify(ledger, never()).claimArchive(
				update.archives().getLast().idempotencyKey(),
				Duration.ofMinutes(15));
		verify(ledger, never()).findRunByUpdateTime(update.sourceUpdateTime());
		verify(metrics).archiveOutcome(ArchiveType.TRANSLATION_EVENTS, ArchiveOutcome.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Потеря права на архив при фиксации отказа не начинает следующий архив")
	void archiveFailureTransitionOwnershipLossStopsBeforeNextArchive() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		DiscoveredArchive mentions = update.archives().getLast();
		ArchiveAttempt attempt = attempt(events, 1, false);
		StagingPaths eventPaths = paths("events-failure-ownership");
		TransferredArtifactIntegrityException failure = new TransferredArtifactIntegrityException(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)))
				.thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenReturn(eventPaths);
		when(downloader.download(attempt, eventPaths)).thenThrow(failure);
		when(ledger.markFailed(events.idempotencyKey(), attempt.token(), failure.failure()))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(service::runLatestUpdate)
				.isInstanceOf(OperationOwnershipLostException.class)
				.isNotSameAs(failure);

		verify(ledger, never()).claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15));
		verify(metrics).archiveOutcome(ArchiveType.TRANSLATION_EVENTS, ArchiveOutcome.OWNERSHIP_LOST);
		verify(metrics, never()).archiveOutcome(ArchiveType.TRANSLATION_EVENTS, ArchiveOutcome.FAILED);
		verify(metrics, never()).error(IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
	}

	@Test
	@DisplayName("Неожиданная runtime-ошибка сохраняется best-effort и пробрасывается")
	void unexpectedRuntimeFailureIsPersistedAndRethrown() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		ArchiveAttempt attempt = attempt(events, 1, false);
		IllegalStateException failure = new IllegalStateException("unsafe implementation detail");
		IngestionFailure projected = IngestionFailure.internalError();

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenThrow(failure);
		when(ledger.markFailed(events.idempotencyKey(), attempt.token(), projected))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(service::runLatestUpdate).isSameAs(failure);

		verify(ledger).markFailed(events.idempotencyKey(), attempt.token(), projected);
		verify(metrics).error(IngestionErrorCode.INTERNAL_ERROR);
	}

	@Test
	@DisplayName("Ошибка сохранения неожиданного отказа подавляется исходной runtime-ошибкой")
	void unexpectedRuntimeFailureRemainsPrimaryWhenFailurePersistenceFails() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		ArchiveAttempt attempt = attempt(events, 1, false);
		IllegalStateException originalFailure = new IllegalStateException("unsafe implementation detail");
		IllegalStateException persistenceFailure = new IllegalStateException("database unavailable");
		IngestionFailure projected = IngestionFailure.internalError();

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenThrow(originalFailure);
		when(ledger.markFailed(events.idempotencyKey(), attempt.token(), projected))
				.thenThrow(persistenceFailure);

		assertThatThrownBy(service::runLatestUpdate)
				.isSameAs(originalFailure)
				.satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(persistenceFailure));

		verify(ledger).markFailed(events.idempotencyKey(), attempt.token(), projected);
	}

	@Test
	@DisplayName("Ошибка сохранения ожидаемого отказа становится основной и сохраняет исходную в suppressed")
	void expectedFailurePersistenceFailureBecomesPrimary() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		ArchiveAttempt attempt = attempt(events, 1, false);
		StagingPaths eventPaths = paths("events");
		TransferredArtifactIntegrityException expectedFailure = new TransferredArtifactIntegrityException(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH);
		IllegalStateException persistenceFailure = new IllegalStateException("database unavailable");
		IngestionFailure projected = expectedFailure.failure();

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenReturn(eventPaths);
		when(downloader.download(attempt, eventPaths)).thenThrow(expectedFailure);
		when(ledger.markFailed(events.idempotencyKey(), attempt.token(), projected))
				.thenThrow(persistenceFailure);

		assertThatThrownBy(service::runLatestUpdate)
				.isSameAs(persistenceFailure)
				.satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(expectedFailure));

		verify(ledger).markFailed(events.idempotencyKey(), attempt.token(), projected);
	}

	@Test
	@DisplayName("Прерывание сохраняется для текущего архива и останавливает весь цикл")
	void interruptionStopsCycleBeforeNextArchive() {
		DiscoveredUpdate update = update();
		DiscoveredArchive events = update.archives().getFirst();
		DiscoveredArchive mentions = update.archives().getLast();
		ArchiveAttempt attempt = attempt(events, 1, false);
		StagingPaths eventPaths = paths("events");
		IngestionInterruptedException interruption = new IngestionInterruptedException(
				new InterruptedException("stop"));
		IngestionFailure projected = interruption.failure();

		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		when(ledger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.of(attempt));
		when(layout.pathsFor(attempt)).thenReturn(eventPaths);
		when(downloader.download(attempt, eventPaths)).thenThrow(interruption);
		when(ledger.markFailed(events.idempotencyKey(), attempt.token(), projected))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(service::runLatestUpdate).isSameAs(interruption);

		verify(ledger).markFailed(events.idempotencyKey(), attempt.token(), projected);
		verify(ledger, never()).claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15));
		verify(metrics).error(IngestionErrorCode.OPERATION_INTERRUPTED);
	}

	@Test
	@DisplayName("Повторный цикл пропускает уже захваченные или подготовленные архивы")
	void idempotentRepeatSkipsAlreadyClaimedOrStagedArchives() {
		DiscoveredUpdate update = update();
		IngestionRunState stagedRun = runState(IngestionRunStatus.STAGED);
		when(manifestClient.fetchLatestManifest()).thenReturn("manifest");
		when(manifestParser.parse("manifest")).thenReturn(update);
		when(ledger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null))
				.thenReturn(0);
		for (DiscoveredArchive archive : update.archives()) {
			when(ledger.claimArchive(archive.idempotencyKey(), Duration.ofMinutes(15))).thenReturn(Optional.empty());
		}
		when(ledger.findRunByUpdateTime(update.sourceUpdateTime())).thenReturn(Optional.of(stagedRun));

		assertThat(service.runLatestUpdate().status()).isEqualTo(IngestionRunStatus.STAGED);

		verifyNoInteractions(layout, downloader, stager);
	}

	private static GdeltIngestionProperties properties() {
		return GdeltTestFixtures.properties(Path.of("staging"), 1024);
	}

	private static DiscoveredUpdate update() {
		Instant time = Instant.parse("2026-07-20T12:00:00Z");
		return new DiscoveredUpdate(
				time,
				List.of(
						archive(time, ArchiveType.TRANSLATION_EVENTS,
								"0123456789abcdef0123456789abcdef", 100),
						archive(time, ArchiveType.TRANSLATION_MENTIONS,
								"fedcba9876543210fedcba9876543210", 100)
				),
				List.of(new DiscoveryDiagnostic(IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE, 3))
		);
	}

	private static ArchiveAttempt attempt(DiscoveredArchive archive, int count, boolean recovered) {
		return new ArchiveAttempt(
				archive,
				UUID.randomUUID(),
				Instant.parse("2026-07-20T12:01:00Z"),
				count,
				recovered);
	}

	private static StagingPaths paths(String name) {
		Path root = Path.of(name).toAbsolutePath().normalize();
		return new StagingPaths(
				root,
				root.resolve("archive.zip"),
				root.resolve("archive.part"),
				root.resolve("data.csv"),
				root.resolve("data.part")
		);
	}

	private static IngestionRunState runState(IngestionRunStatus status) {
		return new IngestionRunState(
				1,
				Instant.parse("2026-07-20T12:00:00Z"),
				status,
				Instant.parse("2026-07-20T12:01:00Z"),
				status == IngestionRunStatus.STAGED ? Instant.parse("2026-07-20T12:02:00Z") : null,
				null,
				List.of()
		);
	}
}
