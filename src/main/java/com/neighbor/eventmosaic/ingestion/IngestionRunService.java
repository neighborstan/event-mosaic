package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttempt;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.ArchiveContentViolationException;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.error.TransferredArtifactIntegrityException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlanner;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestClient;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestParser;
import com.neighbor.eventmosaic.ingestion.staging.DownloadedArchive;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.StagingPaths;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.stereotype.Service;

/**
 * Загружает архивы GDELT для одного цикла, начиная с самого нового обновления. Обнаруживает архивы, сохраняет их состояние, скачивает
 * и безопасно подготавливает их для дальнейшей обработки.
 */
@Service
public class IngestionRunService {

	private static final Logger LOGGER = LoggerFactory.getLogger(IngestionRunService.class);
	private static final String EVENT_FIELD = "event";
	private static final String SOURCE_UPDATE_TIME_FIELD = "source_update_time";
	private static final String ARCHIVE_TYPE_FIELD = "archive_type";
	private static final String ATTEMPT_COUNT_FIELD = "attempt_count";
	private static final String ERROR_CODE_FIELD = "error_code";
	private static final String RETRYABLE_FIELD = "retryable";

	private final GdeltManifestClient manifestClient;
	private final GdeltManifestParser manifestParser;
	private final IngestionArchiveLedger archiveLedger;
	private final RecentRecoveryPlanLedger recentRecoveryPlanLedger;
	private final RecentWindowPlanner recentWindowPlanner;
	private final SourcePollLedger sourcePollLedger;
	private final StagingLayout stagingLayout;
	private final HttpArchiveDownloader archiveDownloader;
	private final ZipArchiveStager archiveStager;
	private final GdeltIngestionProperties properties;
	private final IngestionMetrics metrics;
	private final BackendDataStorageMonitor storageMonitor;

	/**
	 * Создает службу загрузки из клиента GDELT, хранилищ состояния и компонентов безопасной подготовки файлов.
	 *
	 * @param manifestClient клиент файла со списком архивов последнего обновления
	 * @param manifestParser разборщик и проверка пары архивов одного обновления GDELT
	 * @param archiveLedger хранилище состояния запусков и архивов
	 * @param recentRecoveryPlanLedger хранилище плана восстановления последнего суточного окна
	 * @param recentWindowPlanner планировщик окна относительно общих серверных часов
	 * @param sourcePollLedger хранилище состояния попыток получить список последнего обновления
	 * @param stagingLayout построитель безопасных путей к временным файлам
	 * @param archiveDownloader загрузчик и проверка ZIP-архивов
	 * @param archiveStager безопасная распаковка и публикация подготовленного файла
	 * @param properties настройки загрузки GDELT
	 * @param metrics сбор метрик о времени и исходах загрузки
	 * @param storageMonitor проверка свободного места перед скачиванием архива
	 */
	public IngestionRunService(
			GdeltManifestClient manifestClient,
			GdeltManifestParser manifestParser,
			IngestionArchiveLedger archiveLedger,
			RecentRecoveryPlanLedger recentRecoveryPlanLedger,
			RecentWindowPlanner recentWindowPlanner,
			SourcePollLedger sourcePollLedger,
			StagingLayout stagingLayout,
			HttpArchiveDownloader archiveDownloader,
			ZipArchiveStager archiveStager,
			GdeltIngestionProperties properties,
			IngestionMetrics metrics,
			BackendDataStorageMonitor storageMonitor
	) {
		this.manifestClient = manifestClient;
		this.manifestParser = manifestParser;
		this.archiveLedger = archiveLedger;
		this.recentRecoveryPlanLedger = recentRecoveryPlanLedger;
		this.recentWindowPlanner = recentWindowPlanner;
		this.sourcePollLedger = sourcePollLedger;
		this.stagingLayout = stagingLayout;
		this.archiveDownloader = archiveDownloader;
		this.archiveStager = archiveStager;
		this.properties = properties;
		this.metrics = metrics;
		this.storageMonitor = storageMonitor;
	}

	/**
	 * Находит последнее обновление GDELT, сохраняет его состояние и независимо подготавливает архивы Event и Mention.
	 *
	 * @return итоговое сохраненное состояние запуска
	 */
	public IngestionRunState runLatestUpdate() {
		OperationBudget budget = OperationBudget.start(Duration.ofDays(1));
		return observeAcquisition(
				() -> executeLatestUpdate(budget, false),
				_ -> IngestionOperationMetricOutcome.COMPLETED);
	}

	/** Загружает последнее обновление в пределах переданного ограничения времени и срока владения циклом. */
	public IngestionRunState runLatestUpdate(OperationBudget budget) {
		return observeAcquisition(
				() -> executeLatestUpdate(budget, true),
				_ -> IngestionOperationMetricOutcome.COMPLETED);
	}

	/**
	 * Запрашивает список архивов последнего обновления. Если попытку нужно отложить, продолжает работу с самым новым ранее сохраненным запуском.
	 */
	AcquisitionCycleResult runOneShot(OperationBudget budget) {
		return observeAcquisition(
				() -> executeOneShot(budget),
				IngestionRunService::acquisitionOutcome);
	}

	/**
	 * Только находит и регистрирует последнее обновление, но не скачивает его архивы. Возвращенное состояние позволяет обработать архивы по очереди.
	 */
	AcquisitionCycleResult prepareOneShot(OperationBudget budget) {
		return observeAcquisition(
				() -> executePreparedOneShot(budget),
				IngestionRunService::acquisitionOutcome);
	}

	/** Скачивает и безопасно подготавливает ровно один архив, после чего возвращает его актуальное сохраненное состояние. */
	IngestionArchiveState acquireArchive(
			IngestionArchiveState archiveState,
			OperationBudget budget
	) {
		return translateCycleFailures(() -> {
			requireRemaining(budget);
			processArchive(archiveState.archive(), budget, true);
			requireRemaining(budget);
			return archiveLedger.findByIdempotencyKey(
					archiveState.archive().idempotencyKey())
					.orElseThrow(() -> new IllegalStateException(
							"Registered ingestion archive disappeared"));
		});
	}

	/** После поархивной обработки пересчитывает и публикует итоговое состояние запуска. */
	IngestionRunState completeOneShot(
			IngestionRunState runState,
			OperationBudget budget
	) {
		return translateCycleFailures(() -> {
			requireRemaining(budget);
			return completeRun(runState.sourceUpdateTime());
		});
	}

	private static IngestionOperationMetricOutcome acquisitionOutcome(
			AcquisitionCycleResult result
	) {
		if (result.sourcePollOwnershipLost()) {
			return IngestionOperationMetricOutcome.OWNERSHIP_LOST;
		}
		return result.sourcePollDeferred()
				? IngestionOperationMetricOutcome.RETRY_DEFERRED
				: IngestionOperationMetricOutcome.COMPLETED;
	}

	private <T> T observeAcquisition(
			Supplier<T> operation,
			Function<T, IngestionOperationMetricOutcome> successOutcome
	) {
		long startedAt = System.nanoTime();
		IngestionOperationMetricOutcome outcome =
				IngestionOperationMetricOutcome.INTERNAL_FAILURE;
		try {
			T result = observeCycle(operation);
			outcome = successOutcome.apply(result);
			return result;
		}
		catch (StoragePressureException exception) {
			outcome = IngestionOperationMetricOutcome.STORAGE_PRESSURE;
			throw exception;
		}
		catch (OperationDeadlineExceededException exception) {
			outcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
			throw exception;
		}
		catch (OperationOwnershipLostException exception) {
			outcome = IngestionOperationMetricOutcome.OWNERSHIP_LOST;
			throw exception;
		}
		catch (IngestionInterruptedException exception) {
			outcome = IngestionOperationMetricOutcome.INTERRUPTED;
			throw exception;
		}
		catch (ApplicationException exception) {
			outcome = IngestionOperationMetricOutcome.EXPECTED_FAILURE;
			throw exception;
		}
		finally {
			metrics.acquisitionDuration(System.nanoTime() - startedAt, outcome);
		}
	}

	private <T> T observeCycle(Supplier<T> operation) {
		metrics.runStarted();
		return translateCycleFailures(operation);
	}

	private <T> T translateCycleFailures(Supplier<T> operation) {
		try {
			return operation.get();
		} catch (UnexpectedArchiveFailure failure) {
			metrics.error(IngestionErrorCode.INTERNAL_ERROR);
			logUnexpectedArchiveFailure(failure);
			throw failure.original();
		} catch (StoragePressureException exception) {
			throw exception;
		} catch (RemoteSourceAccessException
				| TransferredArtifactIntegrityException
				| RemoteResponseRejectedException
				| SourceDataViolationException
				| ArchiveContentViolationException
				| StagingStorageException
				| OperationDeadlineExceededException
				| IngestionInterruptedException exception) {
			logExpectedCycleFailure(exception);
			throw exception;
		} catch (OperationOwnershipLostException exception) {
			throw exception;
		} catch (RuntimeException exception) {
			metrics.error(IngestionErrorCode.INTERNAL_ERROR);
			LOGGER.atError()
					.setCause(SafeExceptionProjection.from(
							exception,
							IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
					.addKeyValue(EVENT_FIELD, "gdelt.cycle.internal_error")
					.addKeyValue(ERROR_CODE_FIELD, IngestionErrorCode.INTERNAL_ERROR)
					.addKeyValue(RETRYABLE_FIELD, false)
					.log("Unexpected GDELT ingestion cycle failure");
			throw exception;
		}
	}

	private IngestionRunState executeLatestUpdate(
			OperationBudget budget,
			boolean boundedAdapters
	) {
		DiscoveredUpdate update = discoverAndRegister(budget, boundedAdapters);
		if (usesRecentWindow()) {
			return processKnownRun(
					resolveRegisteredRun(update, budget),
					budget,
					boundedAdapters);
		}
		return processRegisteredUpdate(update, budget, boundedAdapters);
	}

	private AcquisitionCycleResult executeOneShot(OperationBudget budget) {
		OneShotDiscovery discovery = discoverOneShot(budget);
		if (discovery.sourcePollOwnershipLost()) {
			return new AcquisitionCycleResult(Optional.empty(), false, true);
		}
		if (discovery.sourcePollDeferred()) {
			return new AcquisitionCycleResult(
					discovery.knownRun().map(run -> processKnownRun(run, budget, true)),
					true);
		}
		DiscoveredUpdate update = discovery.update().orElseThrow();
		IngestionRunState processed = usesRecentWindow()
				? processKnownRun(resolveRegisteredRun(update, budget), budget, true)
				: processRegisteredUpdate(update, budget, true);
		return new AcquisitionCycleResult(
				Optional.of(processed),
				false,
				false);
	}

	private AcquisitionCycleResult executePreparedOneShot(OperationBudget budget) {
		OneShotDiscovery discovery = discoverOneShot(budget);
		if (discovery.sourcePollOwnershipLost()) {
			return new AcquisitionCycleResult(Optional.empty(), false, true);
		}
		if (discovery.sourcePollDeferred()) {
			return new AcquisitionCycleResult(discovery.knownRun(), true);
		}
		DiscoveredUpdate update = discovery.update().orElseThrow();
		IngestionRunState runState = resolveRegisteredRun(update, budget);
		return new AcquisitionCycleResult(Optional.of(runState), false, false);
	}

	private OneShotDiscovery discoverOneShot(OperationBudget budget) {
		requireRemaining(budget);
		sourcePollLedger.register(GdeltSourceContract.SOURCE_NAME);
		requireRemaining(budget);
		Optional<SourcePollAttempt> claimed = sourcePollLedger.claim(
				GdeltSourceContract.SOURCE_NAME,
				properties.continuity().recoveryLease());
		if (claimed.isEmpty()) {
			requireRemaining(budget);
			return OneShotDiscovery.deferred(archiveLedger.findLatestRun());
		}

		SourcePollAttempt pollAttempt = claimed.orElseThrow();
		DiscoveredUpdate update;
		try {
			update = discoverAndRegister(budget, true);
			requireRemaining(budget);
			AttemptTransitionResult sourcePollTransition = sourcePollLedger.markSucceeded(
					GdeltSourceContract.SOURCE_NAME,
					pollAttempt.token());
			if (sourcePollTransition != AttemptTransitionResult.APPLIED) {
				return OneShotDiscovery.ownershipLost();
			}
			requireRemaining(budget);
		}
		catch (ApplicationException exception) {
			if (exception instanceof IngestionFailureContract failure) {
				persistSourcePollFailure(pollAttempt, failure, exception, budget);
			}
			throw exception;
		}
		catch (OperationOwnershipLostException exception) {
			throw exception;
		}
		catch (RuntimeException exception) {
			persistSourcePollInternalFailure(pollAttempt, exception, budget);
			throw exception;
		}
		return OneShotDiscovery.discovered(update);
	}

	private DiscoveredUpdate discoverAndRegister(
			OperationBudget budget,
			boolean boundedAdapter
	) {
		requireRemaining(budget);
		String manifest = boundedAdapter
				? manifestClient.fetchLatestManifest(budget)
				: manifestClient.fetchLatestManifest();
		DiscoveredUpdate update = manifestParser.parse(manifest);

		for (DiscoveryDiagnostic diagnostic : update.diagnostics()) {
			metrics.event(diagnostic.code());
			LOGGER.atInfo()
					.addKeyValue(EVENT_FIELD, "gdelt.manifest.entry_ignored")
					.addKeyValue(SOURCE_UPDATE_TIME_FIELD, update.sourceUpdateTime())
					.addKeyValue("event_code", diagnostic.code())
					.addKeyValue("line_number", diagnostic.lineNumber())
					.log("GDELT manifest entry ignored");
		}

		requireRemaining(budget);
		int gapsCreated;
		if (usesRecentWindow()) {
			var plan = recentWindowPlanner.plan(update.sourceUpdateTime());
			gapsCreated = recentRecoveryPlanLedger.activate(update, plan).auditGapsCreated();
		}
		else {
			gapsCreated = archiveLedger.registerDiscoveredUpdate(
					update,
					properties.continuity().firstRunPolicy(),
					properties.continuity().firstRunStartAt());
		}
		metrics.gapsCreated(gapsCreated);
		return update;
	}

	private IngestionRunState processRegisteredUpdate(
			DiscoveredUpdate update,
			OperationBudget budget,
			boolean boundedAdapters
	) {
		for (DiscoveredArchive archive : update.archives()) {
			requireRemaining(budget);
			processArchive(archive, budget, boundedAdapters);
		}
		requireRemaining(budget);
		return completeRun(update.sourceUpdateTime());
	}

	private IngestionRunState processKnownRun(
			IngestionRunState known,
			OperationBudget budget,
			boolean boundedAdapters
	) {
		for (var archiveState : known.archives()) {
			requireRemaining(budget);
			processArchive(archiveState.archive(), budget, boundedAdapters);
		}
		requireRemaining(budget);
		return completeRun(known.sourceUpdateTime());
	}

	private IngestionRunState resolveRegisteredRun(
			DiscoveredUpdate observed,
			OperationBudget budget
	) {
		requireRemaining(budget);
		Optional<IngestionRunState> exact = archiveLedger.findRunByUpdateTime(
				observed.sourceUpdateTime());
		requireRemaining(budget);
		if (usesRecentWindow()) {
			Optional<IngestionRunState> latest = archiveLedger.findLatestRun();
			requireRemaining(budget);
			if (latest.isPresent()
					&& latest.orElseThrow().sourceUpdateTime().isAfter(observed.sourceUpdateTime())) {
				return latest.orElseThrow();
			}
		}
		if (exact.isPresent()) {
			return exact.orElseThrow();
		}
		throw new IllegalStateException("Registered ingestion run disappeared");
	}

	private boolean usesRecentWindow() {
		return properties.continuity().firstRunPolicy()
				== com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy.RECENT_WINDOW;
	}

	private IngestionRunState completeRun(java.time.Instant sourceUpdateTime) {
		IngestionRunState result = archiveLedger.findRunByUpdateTime(sourceUpdateTime)
				.orElseThrow(() -> new IllegalStateException("Registered ingestion run disappeared"));
		metrics.runCompleted(result.status());
		LOGGER.atInfo()
				.addKeyValue(EVENT_FIELD, "gdelt.cycle.completed")
				.addKeyValue(SOURCE_UPDATE_TIME_FIELD, result.sourceUpdateTime())
				.addKeyValue("run_status", result.status())
				.log("GDELT ingestion cycle finished");
		return result;
	}

	private void processArchive(
			DiscoveredArchive archive,
			OperationBudget budget,
			boolean boundedAdapters
	) {
		requireRemaining(budget);
		Optional<IngestionArchiveState> existing = archiveLedger.findByIdempotencyKey(
				archive.idempotencyKey());
		requireRemaining(budget);
		if (existing.isPresent()
				&& existing.orElseThrow().status() == IngestionArchiveStatus.STAGED) {
			metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.SKIPPED);
			return;
		}
		storageMonitor.requireCapacity(StorageResource.STAGING);
		requireRemaining(budget);
		Optional<ArchiveAttempt> claimed = archiveLedger.claimArchive(
				archive.idempotencyKey(),
				properties.continuity().recoveryLease());
		if (claimed.isEmpty()) {
			metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.SKIPPED);
			return;
		}
		ArchiveAttempt attempt = claimed.orElseThrow();
		if (attempt.attemptCount() > 1) {
			metrics.retry(archive.archiveType(), attempt.recovered());
		}

		boolean downloadCompleted = false;
		try {
			requireRemaining(budget);
			StagingPaths paths = stagingLayout.pathsFor(attempt);
			DownloadedArchive downloaded = boundedAdapters
					? archiveDownloader.download(attempt, paths, budget)
					: archiveDownloader.download(attempt, paths);
			requireRemaining(budget);
			downloadCompleted = true;
			metrics.downloadOutcome(
					archive.archiveType(),
					downloaded.reused() ? DownloadOutcome.REUSED : DownloadOutcome.DOWNLOADED
			);
			StagedArchive staged = boundedAdapters
					? archiveStager.stage(attempt, downloaded, paths, budget)
					: archiveStager.stage(attempt, downloaded, paths);
			requireRemaining(budget);
			if (ownershipLost(
					archive,
					attempt,
					archiveLedger.markStaged(archive.idempotencyKey(), attempt.token(), staged))) {
				throw new OperationOwnershipLostException();
			}
			metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.STAGED);
			LOGGER.atInfo()
					.addKeyValue(EVENT_FIELD, "gdelt.archive.staged")
					.addKeyValue(SOURCE_UPDATE_TIME_FIELD, archive.sourceUpdateTime())
					.addKeyValue(ARCHIVE_TYPE_FIELD, archive.archiveType())
					.addKeyValue(ATTEMPT_COUNT_FIELD, attempt.attemptCount())
					.log("GDELT archive staged");
		} catch (RemoteSourceAccessException
				| TransferredArtifactIntegrityException
				| RemoteResponseRejectedException
				| ArchiveContentViolationException
				| StagingStorageException
				| OperationDeadlineExceededException
				| IngestionInterruptedException exception) {
			handleExpectedArchiveFailure(
					archive,
					attempt,
					exception,
					!downloadCompleted,
					budget);
		} catch (OperationOwnershipLostException exception) {
			throw exception;
		} catch (RuntimeException exception) {
			IngestionFailure failure = IngestionFailure.internalError();
			persistUnexpectedFailure(archive, attempt, failure, exception, budget);
			throw new UnexpectedArchiveFailure(archive, attempt, exception);
		}
	}

	private void persistSourcePollFailure(
			SourcePollAttempt attempt,
			IngestionFailureContract failure,
			ApplicationException exception,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		try {
			AttemptTransitionResult transition;
			if (failure.retryAfter().isZero()) {
				transition = sourcePollLedger.markFailed(
						GdeltSourceContract.SOURCE_NAME,
						attempt.token(),
						failure.failure());
			}
			else {
				transition = sourcePollLedger.markFailed(
						GdeltSourceContract.SOURCE_NAME,
						attempt.token(),
						failure.failure(),
						failure.retryAfter());
			}
			throwIfOwnershipLost(transition);
		}
		catch (OperationOwnershipLostException ownershipLost) {
			throw ownershipLost;
		}
		catch (RuntimeException persistenceFailure) {
			exception.addSuppressed(persistenceFailure);
		}
	}

	private void persistSourcePollInternalFailure(
			SourcePollAttempt attempt,
			RuntimeException exception,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		try {
			AttemptTransitionResult transition = sourcePollLedger.markFailed(
					GdeltSourceContract.SOURCE_NAME,
					attempt.token(),
					IngestionFailure.internalError());
			throwIfOwnershipLost(transition);
		}
		catch (OperationOwnershipLostException ownershipLost) {
			throw ownershipLost;
		}
		catch (RuntimeException persistenceFailure) {
			exception.addSuppressed(persistenceFailure);
		}
	}

	private static void requireRemaining(OperationBudget budget) {
		try {
			budget.requireAvailable();
		}
		catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void requireTerminalOwnership(OperationBudget budget) {
		budget.requireOwnershipForTerminalTransition();
	}

	private <T extends ApplicationException & IngestionFailureContract> void handleExpectedArchiveFailure(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			T exception,
			boolean downloadFailed,
			OperationBudget budget
	) {
		IngestionFailure failure = exception.failure();
		AttemptTransitionResult transition = persistExpectedFailure(
				archive,
				attempt,
				exception,
				failure,
				budget);
		if (ownershipLost(archive, attempt, transition)) {
			throw new OperationOwnershipLostException();
		}
		metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.FAILED);
		if (downloadFailed) {
			metrics.downloadOutcome(archive.archiveType(), DownloadOutcome.FAILED);
		}
		if (exception.abortsCycle()) {
			throw exception;
		}
		metrics.error(failure.code());
		LoggingEventBuilder log = LOGGER.atError()
				.setCause(SafeExceptionProjection.from(exception, failure.code().safeMessage()))
				.addKeyValue(EVENT_FIELD, "gdelt.archive.failed")
				.addKeyValue(SOURCE_UPDATE_TIME_FIELD, archive.sourceUpdateTime())
				.addKeyValue(ARCHIVE_TYPE_FIELD, archive.archiveType())
				.addKeyValue(ERROR_CODE_FIELD, failure.code())
				.addKeyValue(RETRYABLE_FIELD, failure.retryable())
				.addKeyValue(ATTEMPT_COUNT_FIELD, attempt.attemptCount());
		addErrorContext(log, exception.context()).log("GDELT archive failed");
	}

	private <T extends ApplicationException & IngestionFailureContract>
			AttemptTransitionResult persistExpectedFailure(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			T exception,
			IngestionFailure failure,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		try {
			AttemptTransitionResult transition;
			if (!exception.retryAfter().isZero()) {
				transition = archiveLedger.markFailed(
						archive.idempotencyKey(),
						attempt.token(),
						failure,
						exception.retryAfter());
			}
			else {
				transition = archiveLedger.markFailed(
						archive.idempotencyKey(),
						attempt.token(),
						failure
				);
			}
			return transition;
		} catch (RuntimeException persistenceFailure) {
			persistenceFailure.addSuppressed(exception);
			throw new UnexpectedArchiveFailure(
					archive,
					attempt,
					persistenceFailure
			);
		}
	}

	private boolean ownershipLost(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			AttemptTransitionResult transition
	) {
		if (transition == AttemptTransitionResult.APPLIED) {
			return false;
		}
		metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.OWNERSHIP_LOST);
		LOGGER.atWarn()
				.addKeyValue(EVENT_FIELD, "gdelt.archive.ownership_lost")
				.addKeyValue(SOURCE_UPDATE_TIME_FIELD, archive.sourceUpdateTime())
				.addKeyValue(ARCHIVE_TYPE_FIELD, archive.archiveType())
				.addKeyValue(ATTEMPT_COUNT_FIELD, attempt.attemptCount())
				.log("GDELT archive attempt no longer owns the transition");
		return true;
	}

	private void persistUnexpectedFailure(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			IngestionFailure failure,
			RuntimeException exception,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		try {
			AttemptTransitionResult transition = archiveLedger.markFailed(
					archive.idempotencyKey(),
					attempt.token(),
					failure
			);
			if (transition == AttemptTransitionResult.APPLIED) {
				metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.FAILED);
			} else {
				metrics.archiveOutcome(archive.archiveType(), ArchiveOutcome.OWNERSHIP_LOST);
				throw new OperationOwnershipLostException();
			}
		} catch (OperationOwnershipLostException ownershipLost) {
			throw ownershipLost;
		} catch (RuntimeException persistenceFailure) {
			exception.addSuppressed(persistenceFailure);
		}
	}

	private static void throwIfOwnershipLost(AttemptTransitionResult transition) {
		if (transition == AttemptTransitionResult.OWNERSHIP_LOST) {
			throw new OperationOwnershipLostException();
		}
	}

	private void logUnexpectedArchiveFailure(UnexpectedArchiveFailure failure) {
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						failure.original(),
						IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
				.addKeyValue(EVENT_FIELD, "gdelt.archive.internal_error")
				.addKeyValue(SOURCE_UPDATE_TIME_FIELD, failure.archive().sourceUpdateTime())
				.addKeyValue(ARCHIVE_TYPE_FIELD, failure.archive().archiveType())
				.addKeyValue(ERROR_CODE_FIELD, IngestionErrorCode.INTERNAL_ERROR)
				.addKeyValue(RETRYABLE_FIELD, false)
				.addKeyValue(ATTEMPT_COUNT_FIELD, failure.attempt().attemptCount())
				.log("Unexpected GDELT archive failure");
	}

	private <T extends ApplicationException & IngestionFailureContract> void logExpectedCycleFailure(T exception) {
		IngestionFailure failure = exception.failure();
		metrics.error(failure.code());
		LoggingEventBuilder log = LOGGER.atError()
				.setCause(SafeExceptionProjection.from(exception, failure.code().safeMessage()))
				.addKeyValue(EVENT_FIELD, "gdelt.cycle.failed")
				.addKeyValue(ERROR_CODE_FIELD, failure.code())
				.addKeyValue(RETRYABLE_FIELD, failure.retryable());
		addErrorContext(log, exception.context()).log("GDELT ingestion cycle failed");
	}

	private static LoggingEventBuilder addErrorContext(
			LoggingEventBuilder builder,
			IngestionErrorContext context
	) {
		if (context.lineNumber() != null) {
			builder = builder.addKeyValue("line_number", context.lineNumber());
		}
		if (context.httpStatus() != null) {
			builder = builder.addKeyValue("http_status", context.httpStatus());
		}
		return builder;
	}

	private static final class UnexpectedArchiveFailure extends RuntimeException {

		private final transient DiscoveredArchive archive;
		private final transient ArchiveAttempt attempt;
		private final transient RuntimeException original;

		private UnexpectedArchiveFailure(
				DiscoveredArchive archive,
				ArchiveAttempt attempt,
				RuntimeException original
		) {
			super(IngestionErrorCode.INTERNAL_ERROR.safeMessage(), null, false, false);
			this.archive = archive;
			this.attempt = attempt;
			this.original = original;
		}

		private DiscoveredArchive archive() {
			return archive;
		}

		private ArchiveAttempt attempt() {
			return attempt;
		}

		private RuntimeException original() {
			return original;
		}
	}

	private record OneShotDiscovery(
			Optional<DiscoveredUpdate> update,
			Optional<IngestionRunState> knownRun,
			boolean sourcePollDeferred,
			boolean sourcePollOwnershipLost
	) {

		private static OneShotDiscovery discovered(DiscoveredUpdate update) {
			return new OneShotDiscovery(
					Optional.of(update),
					Optional.empty(),
					false,
					false);
		}

		private static OneShotDiscovery deferred(Optional<IngestionRunState> knownRun) {
			return new OneShotDiscovery(
					Optional.empty(),
					knownRun,
					true,
					false);
		}

		private static OneShotDiscovery ownershipLost() {
			return new OneShotDiscovery(
					Optional.empty(),
					Optional.empty(),
					false,
					true);
		}
	}
}
