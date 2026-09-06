package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingFailureContract;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolution;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolutionStatus;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolver;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimResult;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.ingestion.source.GdeltMasterCatalogValidationException;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalog;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalogClient;
import com.neighbor.eventmosaic.ingestion.source.GdeltTranslationMasterCatalogStatus;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Управляет полным циклом загрузки и обработки GDELT. Сначала обрабатывает самое новое обновление, а затем заполняет
 * пропуски в суточном окне. Состояние каждого архива сохраняется, поэтому после сбоя цикл можно продолжить без повтора
 * уже завершенной работы.
 */
@Service
public class GdeltPipelineService {

	private static final Logger LOGGER = LoggerFactory.getLogger(GdeltPipelineService.class);
	private static final String PROJECTION_REVISION = "gdelt-processing-v1";
	private static final String INDEX_MAPPING_REVISION = "gdelt-indices-v1";
	private static final String LOG_KEY_EVENT = "event";
	private static final String LOG_KEY_SOURCE_UPDATE_TIME = "source_update_time";
	private static final String LOG_KEY_ARCHIVE_KIND = "archive_kind";
	private static final String LOG_KEY_ERROR_CODE = "error_code";
	private static final String LOG_KEY_RETRYABLE = "retryable";
	private static final String LOG_KEY_ATTEMPT_COUNT = "attempt_count";
	private static final String LOG_KEY_OUTCOME = "outcome";
	private static final String OUTCOME_FAILED = "FAILED";
	private static final String OUTCOME_OWNERSHIP_LOST = "OWNERSHIP_LOST";
	private static final List<ArchiveType> LATEST_ARCHIVE_ORDER = List.of(
			ArchiveType.TRANSLATION_EVENTS,
			ArchiveType.TRANSLATION_MENTIONS);

	private final IngestionRunService ingestionRunService;
	private final RecentRecoveryPlanLedger recentRecoveryPlanLedger;
	private final GdeltTranslationMasterCatalogClient masterCatalogClient;
	private final ArchiveProcessingLedger processingLedger;
	private final GdeltArchiveProcessor archiveProcessor;
	private final ProcessingFingerprintFactory fingerprintFactory;
	private final GdeltIndexWriter indexWriter;
	private final IndexTargetResolver indexTargetResolver;
	private final GdeltIngestionProperties properties;
	private final BackendDataProperties backendDataProperties;
	private final IngestionMetrics metrics;
	private final BackendDataStorageMonitor storageMonitor;

	/**
	 * Создает службу, которая связывает загрузку, обработку, индексацию и сохранение состояния.
	 *
	 * @param ingestionRunService загрузка архивов одного обновления GDELT
	 * @param recentRecoveryPlanLedger хранилище плана восстановления последнего суточного окна
	 * @param masterCatalogClient клиент полного каталога GDELT для поиска недостающих архивов
	 * @param processingLedger журнал состояния и владения попытками обработки
	 * @param archiveProcessor потоковая обработка архива и подготовка документов для поиска
	 * @param fingerprintFactory создание идентификатора версии обработки
	 * @param indexWriter запись документов в Elasticsearch и проверка подтверждения индексации
	 * @param indexTargetResolver выбор текущего физического индекса для записи
	 * @param properties настройки загрузки GDELT и сроков владения попытками
	 * @param backendDataProperties общие ограничения времени операции и проверки сохраненных данных
	 * @param metrics сбор метрик о времени и исходах цикла
	 * @param storageMonitor проверка свободного места перед ростом поискового индекса
	 */
	public GdeltPipelineService(
			IngestionRunService ingestionRunService,
			RecentRecoveryPlanLedger recentRecoveryPlanLedger,
			GdeltTranslationMasterCatalogClient masterCatalogClient,
			ArchiveProcessingLedger processingLedger,
			GdeltArchiveProcessor archiveProcessor,
			ProcessingFingerprintFactory fingerprintFactory,
			GdeltIndexWriter indexWriter,
			IndexTargetResolver indexTargetResolver,
			GdeltIngestionProperties properties,
			BackendDataProperties backendDataProperties,
			IngestionMetrics metrics,
			BackendDataStorageMonitor storageMonitor
	) {
		this.ingestionRunService = ingestionRunService;
		this.recentRecoveryPlanLedger = recentRecoveryPlanLedger;
		this.masterCatalogClient = masterCatalogClient;
		this.processingLedger = processingLedger;
		this.archiveProcessor = archiveProcessor;
		this.fingerprintFactory = fingerprintFactory;
		this.indexWriter = indexWriter;
		this.indexTargetResolver = indexTargetResolver;
		this.properties = properties;
		this.backendDataProperties = backendDataProperties;
		this.metrics = metrics;
		this.storageMonitor = storageMonitor;
	}

	/**
	 * Загружает последнее обновление GDELT и независимо обрабатывает каждый подготовленный архив.
	 *
	 * @return сохраненное состояние загрузки текущего обновления
	 */
	public IngestionRunState runLatestUpdate() {
		OperationBudget budget = OperationBudget.start(
				backendDataProperties.operationDeadline());
		IngestionRunState runState = ingestionRunService.runLatestUpdate();
		processRunState(runState, budget);
		return runState;
	}

	/**
	 * Выполняет ровно один ограниченный по времени цикл, начиная с самого нового обновления. Метод не ждет между повторами и не планирует
	 * следующий запуск.
	 *
	 * @return причина, по которой цикл завершился или был отложен
	 */
	public IngestionOneShotOutcome runOneShot() {
		return runCycle(OperationBudget.start(
				backendDataProperties.operationDeadline()));
	}

	/**
	 * Выполняет один цикл в рамках ранее начатого общего отсчета времени. Вызывающий код должен заранее получить исключительное право
	 * на выполнение этого цикла.
	 *
	 * @param budget общее ограничение времени и срока исключительного права, начатые до вызова
	 * @return причина, по которой цикл завершился или был отложен
	 */
	public IngestionOneShotOutcome runCycle(OperationBudget budget) {
		long startedAt = System.nanoTime();
		IngestionOperationMetricOutcome metricOutcome =
				IngestionOperationMetricOutcome.INTERNAL_FAILURE;
		try {
			AcquisitionCycleResult acquisition = ingestionRunService.prepareOneShot(budget);
			if (acquisition.sourcePollOwnershipLost()) {
				metricOutcome = IngestionOperationMetricOutcome.OWNERSHIP_LOST;
				return IngestionOneShotOutcome.OWNERSHIP_LOST;
			}
			if (acquisition.runState().isPresent()) {
				IngestionRunState runState = acquisition.runState().orElseThrow();
				ArchiveBatchResult latest = processArchiveBatch(
						orderedArchives(runState),
						budget,
						true,
						false);
				if (latest.deadlineReached()) {
					metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
					return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
				}
				ingestionRunService.completeOneShot(runState, budget);
				RuntimeException deferredFailure = latest.deferredFailure();
				RecentWorkPreparation recentPreparation = prepareRecentWork(
						runState,
						budget);
				deferredFailure = combineDeferredFailures(
						deferredFailure,
						recentPreparation.deferredFailure());
				if (recentPreparation.processKnownWork()) {
					ArchiveBatchResult recentEvents = processRecentArchives(
							ArchiveType.TRANSLATION_EVENTS,
							runState.sourceUpdateTime(),
							budget);
					deferredFailure = combineDeferredFailures(
							deferredFailure,
							recentEvents.deferredFailure());
					if (recentEvents.deadlineReached()) {
						metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
						return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
					}

					ArchiveBatchResult recentMentions = processRecentArchives(
							ArchiveType.TRANSLATION_MENTIONS,
							runState.sourceUpdateTime(),
							budget);
					deferredFailure = combineDeferredFailures(
							deferredFailure,
							recentMentions.deferredFailure());
					if (recentMentions.deadlineReached()) {
						metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
						return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
					}
				}
				throwDeferredFailure(deferredFailure);
			}
			IngestionOneShotOutcome outcome = acquisition.sourcePollDeferred()
					? IngestionOneShotOutcome.RETRY_DEFERRED
					: IngestionOneShotOutcome.COMPLETED;
			metricOutcome = outcome == IngestionOneShotOutcome.RETRY_DEFERRED
					? IngestionOperationMetricOutcome.RETRY_DEFERRED
					: IngestionOperationMetricOutcome.COMPLETED;
			return outcome;
		}
		catch (OperationDeadlineExceededException _) {
			metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
			return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
		}
		catch (OperationDeadlineReachedException _) {
			metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
			return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
		}
		catch (StoragePressureException _) {
			metricOutcome = IngestionOperationMetricOutcome.STORAGE_PRESSURE;
			return IngestionOneShotOutcome.STORAGE_PRESSURE;
		}
		catch (OperationOwnershipLostException exception) {
			metricOutcome = IngestionOperationMetricOutcome.OWNERSHIP_LOST;
			throw exception;
		}
		catch (IngestionInterruptedException | IndexingInterruptedException exception) {
			metricOutcome = IngestionOperationMetricOutcome.INTERRUPTED;
			throw exception;
		}
		catch (ApplicationException exception) {
			metricOutcome = IngestionOperationMetricOutcome.EXPECTED_FAILURE;
			throw exception;
		}
		catch (RuntimeException exception) {
			metricOutcome = IngestionOperationMetricOutcome.INTERNAL_FAILURE;
			throw exception;
		}
		finally {
			metrics.cycleDuration(System.nanoTime() - startedAt, metricOutcome);
		}
	}

	IngestionOneShotOutcome runOneShot(OperationBudget budget) {
		return runCycle(budget);
	}

	private boolean processRunState(
			IngestionRunState runState,
			OperationBudget budget
	) {
		return processArchives(runState, budget, false, true);
	}

	private boolean processArchives(
			IngestionRunState runState,
			OperationBudget budget,
			boolean acquireBeforeProcessing,
			boolean verifyIndexedReceipt
	) {
		ArchiveBatchResult result = processArchiveBatch(
				orderedArchives(runState),
				budget,
				acquireBeforeProcessing,
				verifyIndexedReceipt);
		if (!result.deadlineReached() && acquireBeforeProcessing) {
			ingestionRunService.completeOneShot(runState, budget);
		}
		throwDeferredFailure(result.deferredFailure());
		return result.deadlineReached();
	}

	private ArchiveBatchResult processArchiveBatch(
			List<IngestionArchiveState> archives,
			OperationBudget budget,
			boolean acquireBeforeProcessing,
			boolean verifyIndexedReceipt
	) {
		RuntimeException deferredFailure = null;
		for (IngestionArchiveState registeredState : archives) {
			IngestionArchiveState archiveState = acquireBeforeProcessing
					? ingestionRunService.acquireArchive(registeredState, budget)
					: registeredState;
			ArchiveStepResult step = processArchiveState(
					archiveState,
					budget,
					verifyIndexedReceipt);
			if (step.deadlineReached()) {
				return ArchiveBatchResult.deadline(deferredFailure);
			}
			deferredFailure = combineDeferredFailures(
					deferredFailure,
					step.deferredFailure());
		}
		return ArchiveBatchResult.completed(deferredFailure);
	}

	private RecentWorkPreparation prepareRecentWork(
			IngestionRunState latestRun,
			OperationBudget budget
	) {
		if (properties.continuity().firstRunPolicy() != FirstRunPolicy.RECENT_WINDOW) {
			return RecentWorkPreparation.skip();
		}
		requireRemaining(budget);
		var currentPlan = recentRecoveryPlanLedger.currentPlan();
		requireRemaining(budget);
		if (currentPlan.isEmpty()) {
			return RecentWorkPreparation.skip();
		}
		var planState = currentPlan.orElseThrow();
		var revision = planState.revision();
		if (!revision.sourceFrontier().equals(latestRun.sourceUpdateTime())) {
			return RecentWorkPreparation.skip();
		}
		if (planState.catalogStatus()
				== RecentRecoveryPlanLedger.CatalogStatus.CATALOG_COMPLETE) {
			return RecentWorkPreparation.ready();
		}

		DiscoveredUpdate capturedLatest = capturedUpdate(latestRun);
		RecentWindowPlan plan = RecentWindowPlan.fromBoundaries(
				revision.windowFrom(),
				revision.windowTo(),
				revision.sourceFrontier());
		GdeltTranslationMasterCatalog catalog;
		try {
			catalog = fetchRecentCatalog(
					Set.copyOf(plan.targetUpdateTimes()),
					capturedLatest,
					budget);
		}
		catch (RemoteResponseRejectedException
				| RemoteSourceAccessException
				| SourceDataViolationException exception) {
			return RecentWorkPreparation.ready(exception);
		}
		emitCatalogDiagnostics(capturedLatest, catalog.diagnostics());
		requireRemaining(budget);
		var registration = recentRecoveryPlanLedger.registerCatalog(
				new RecentRecoveryPlanLedger.CatalogRegistration(
						revision,
						catalog.updates(),
						catalog.generation(),
						catalog.etag(),
						catalog.verifiedFrom(),
						catalog.status()
								== GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE));
		requireRemaining(budget);
		return switch (registration.outcome()) {
			case APPLIED, ALREADY_COMPLETE -> RecentWorkPreparation.ready();
			case STALE_PLAN -> throw new OperationOwnershipLostException();
		};
	}

	private GdeltTranslationMasterCatalog fetchRecentCatalog(
			Set<java.time.Instant> targetTimestamps,
			DiscoveredUpdate capturedLatest,
			OperationBudget budget
	) {
		try {
			return masterCatalogClient.fetchCatalog(
					targetTimestamps,
					capturedLatest,
					budget);
		}
		catch (GdeltMasterCatalogValidationException exception) {
			ApplicationException mapped = mapCatalogValidationFailure(exception);
			logCatalogFailure((IngestionFailureContract) mapped, mapped);
			throw mapped;
		}
		catch (RemoteResponseRejectedException | RemoteSourceAccessException exception) {
			logCatalogFailure(exception, exception);
			throw exception;
		}
	}

	private static ApplicationException mapCatalogValidationFailure(
			GdeltMasterCatalogValidationException exception
	) {
		return switch (exception.errorCode()) {
			case MASTER_CATALOG_SOURCE_URI_REJECTED,
					MASTER_CATALOG_METADATA_CONFLICT ->
					new SourceDataViolationException(
							exception.errorCode(),
							exception.context());
			default -> new RemoteSourceAccessException(
					exception.errorCode(),
					exception.context());
		};
	}

	private void logCatalogFailure(
			IngestionFailureContract contract,
			RuntimeException exception
	) {
		var failure = contract.failure();
		metrics.error(failure.code());
		var log = LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						exception,
						failure.code().safeMessage()))
				.addKeyValue(LOG_KEY_EVENT, "gdelt.master_catalog.failed")
				.addKeyValue(LOG_KEY_ERROR_CODE, failure.code().code())
				.addKeyValue(LOG_KEY_RETRYABLE, failure.retryable());
		if (contract.context().lineNumber() != null) {
			log = log.addKeyValue("line_number", contract.context().lineNumber());
		}
		if (contract.context().httpStatus() != null) {
			log = log.addKeyValue("http_status", contract.context().httpStatus());
		}
		log.log("GDELT master catalog failed");
	}

	private void emitCatalogDiagnostics(
			DiscoveredUpdate capturedLatest,
			List<DiscoveryDiagnostic> diagnostics
	) {
		for (DiscoveryDiagnostic diagnostic : diagnostics) {
			metrics.event(diagnostic.code());
			LOGGER.atInfo()
					.addKeyValue(LOG_KEY_EVENT, "gdelt.master_catalog.entry_ignored")
					.addKeyValue(
							LOG_KEY_SOURCE_UPDATE_TIME,
							capturedLatest.sourceUpdateTime())
					.addKeyValue("event_code", diagnostic.code())
					.addKeyValue("line_number", diagnostic.lineNumber())
					.log("GDELT master catalog entry ignored");
		}
	}

	private ArchiveBatchResult processRecentArchives(
			ArchiveType archiveType,
			java.time.Instant sourceFrontier,
			OperationBudget budget
	) {
		requireRemaining(budget);
		List<IngestionArchiveState> registered =
				recentRecoveryPlanLedger.archivesNewestFirst(archiveType);
		requireRemaining(budget);
		var eligible = new ArrayList<IngestionArchiveState>(registered.size());
		for (IngestionArchiveState archiveState : registered) {
			requireRemaining(budget);
			if (archiveState.archive().sourceUpdateTime().equals(sourceFrontier)) {
				continue;
			}
			boolean terminalIndexed = processingLedger.findByArchiveIdempotencyKey(
					archiveState.archive().idempotencyKey())
					.map(state -> state.status() == ArchiveProcessingStatus.INDEXED)
					.orElse(false);
			requireRemaining(budget);
			if (!terminalIndexed) {
				eligible.add(archiveState);
			}
		}
		return processArchiveBatch(List.copyOf(eligible), budget, true, false);
	}

	private static DiscoveredUpdate capturedUpdate(IngestionRunState runState) {
		return new DiscoveredUpdate(
				runState.sourceUpdateTime(),
				runState.archives().stream()
						.map(IngestionArchiveState::archive)
						.toList(),
				List.of());
	}

	private static RuntimeException combineDeferredFailures(
			RuntimeException first,
			RuntimeException next
	) {
		if (first == null) {
			return next;
		}
		if (next != null && next != first) {
			first.addSuppressed(next);
		}
		return first;
	}

	private static void throwDeferredFailure(RuntimeException failure) {
		if (failure != null) {
			throw failure;
		}
	}

	private ArchiveStepResult processArchiveState(
			IngestionArchiveState archiveState,
			OperationBudget budget,
			boolean verifyIndexedReceipt
	) {
		if (!isStaged(archiveState)) {
			return ArchiveStepResult.completed();
		}
		try {
			requireRemaining(budget);
			return processStagedArchive(
					archiveState,
					budget,
					verifyIndexedReceipt)
					? ArchiveStepResult.deadline()
					: ArchiveStepResult.completed();
		} catch (OperationDeadlineExceededException _) {
			return ArchiveStepResult.deadline();
		} catch (OperationOwnershipLostException exception) {
			throw exception;
		} catch (IngestionInterruptedException exception) {
			throw exception;
		} catch (StoragePressureException exception) {
			throw exception;
		} catch (IndexingInterruptedException exception) {
			logExpectedArchiveFailure(archiveState, exception, exception);
			throw exception;
		} catch (IndexTargetUnavailableException exception) {
			String outcome = targetUnavailableOutcome(exception.reason());
			logOutcome(
					archiveState,
					toGdeltKind(archiveState.archive().archiveType()),
					outcome,
					exception.reason().errorCode().code());
			if (OUTCOME_OWNERSHIP_LOST.equals(outcome)) {
				OperationOwnershipLostException ownershipLost =
						new OperationOwnershipLostException();
				ownershipLost.addSuppressed(exception);
				throw ownershipLost;
			}
			return ArchiveStepResult.completed();
		} catch (IndexingAccessException | IndexingProtocolException exception) {
			logExpectedArchiveFailure(archiveState, exception, exception);
			return ArchiveStepResult.deferred(exception);
		} catch (RuntimeException exception) {
			logUnexpectedArchiveFailure(archiveState, exception);
			throw exception;
		}
	}

	private boolean processStagedArchive(
			IngestionArchiveState archiveState,
			OperationBudget budget,
			boolean verifyIndexedReceipt
	) {
		requireRemaining(budget);
		GdeltArchiveKind kind = toGdeltKind(archiveState.archive().archiveType());
		GdeltIndexKind indexKind = toIndexKind(kind);
		storageMonitor.requireCapacity(StorageResource.ELASTICSEARCH);
		requireRemaining(budget);
		IndexTargetResolution targetResolution = indexTargetResolver.resolve(
				archiveState.archive().sourceUpdateTime(),
				budget);
		requireRemaining(budget);
		if (targetResolution.status() != IndexTargetResolutionStatus.READY) {
			logOutcome(
					archiveState,
					kind,
					resolutionOutcome(targetResolution),
					null);
			if (targetResolution.status() == IndexTargetResolutionStatus.OWNERSHIP_LOST) {
				throw new OperationOwnershipLostException();
			}
			return false;
		}
		ActiveIndexTargets indexTargets = targetResolution.targets();
		ArchiveProcessingTargetBinding targetBinding = toTargetBinding(
				indexTargets,
				indexKind);
		String archiveKey = archiveState.archive().idempotencyKey();
		String processingFingerprint = fingerprintFactory.create(
				archiveKey,
				kind,
				PROJECTION_REVISION,
				INDEX_MAPPING_REVISION);
		ArchiveProcessingFingerprint fingerprint = new ArchiveProcessingFingerprint(
				archiveState.stagedArchive().actualMd5(),
				PROJECTION_REVISION,
				processingFingerprint);
		requireRemaining(budget);
		ArchiveProcessingState processingState =
				processingLedger.register(archiveKey, fingerprint);
		requireRemaining(budget);
		if (processingState.status() == ArchiveProcessingStatus.INDEXED) {
			if (!verifyIndexedReceipt) {
				logOutcome(archiveState, kind, "SKIPPED_INDEXED", null);
				return false;
			}
			ReceiptDecision receiptDecision =
					reconcileIndexedReceipt(
							archiveState,
							kind,
							indexTargets,
							processingState,
							budget);
			if (receiptDecision != ReceiptDecision.REINDEX) {
				logOutcome(
						archiveState,
						kind,
						receiptDecision.outcome(),
						receiptDecision.errorCode());
				if (receiptDecision == ReceiptDecision.OWNERSHIP_LOST) {
					throw new OperationOwnershipLostException();
				}
				return false;
			}
		}

		storageMonitor.requireCapacity(StorageResource.ELASTICSEARCH);
		requireRemaining(budget);
		ArchiveProcessingClaimResult claimResult = processingLedger.claim(
				archiveKey,
				targetBinding,
				properties.continuity().recoveryLease());
		if (claimResult.status() != ArchiveProcessingClaimStatus.CLAIMED) {
			requireRemaining(budget);
			logOutcome(archiveState, kind, claimOutcome(claimResult.status()), null);
			if (claimResult.status() == ArchiveProcessingClaimStatus.OWNERSHIP_LOST) {
				throw new OperationOwnershipLostException();
			}
			return false;
		}
		ArchiveProcessingAttempt attempt = claimResult.attempt();
		if (!attempt.targetBinding().equals(targetBinding)) {
			throw new IllegalStateException("Processing claim returned another target binding");
		}
		try {
			requireRemaining(budget);
			return processClaimedArchive(
					archiveState,
					kind,
					processingFingerprint,
					indexTargets,
					attempt,
					budget);
		}
		catch (OperationDeadlineExceededException _) {
			recordDeadlineFailure(
					archiveState,
					kind,
					attempt,
					ArchiveProcessingProgress.empty(),
					budget);
			return true;
		}
	}

	private ReceiptDecision reconcileIndexedReceipt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ActiveIndexTargets indexTargets,
			ArchiveProcessingState processingState,
			OperationBudget budget
	) {
		requireRemaining(budget);
		var persistedReceipt = processingState.receipt();
		var query = new ArchiveReceiptQuery(
				toIndexKind(kind),
				indexTargets.target(toIndexKind(kind)),
				archiveState.archive().idempotencyKey(),
				processingState.fingerprint().processingFingerprint(),
				persistedReceipt.expectedDocumentCount(),
				new ArchiveIdentityDigest(persistedReceipt.expectedIdentityDigest()),
				backendDataProperties.receiptPageSize());
		var verification = indexWriter.verifyReceipt(query, budget);
		requireRemaining(budget);
		ArchiveProcessingTargetBinding currentTargetBinding = toTargetBinding(
				indexTargets,
				toIndexKind(kind));
		if (verification.matched()) {
			AttemptTransitionResult transition = processingLedger.recordReceiptMatch(
					archiveState.archive().idempotencyKey(),
					processingState.fingerprint().processingFingerprint(),
					processingState.attempt().count(),
					processingState.stateVersion(),
					processingState.targetBinding(),
					currentTargetBinding,
					verification);
			return transition == AttemptTransitionResult.APPLIED
					? ReceiptDecision.MATCHED
					: ReceiptDecision.OWNERSHIP_LOST;
		}
		boolean surplus = verification.status() == ArchiveReceiptStatus.SURPLUS;
		ArchiveProcessingErrorCode receiptCode = surplus
				? ArchiveProcessingErrorCode.INDEX_RECEIPT_SURPLUS
				: ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH;
		AttemptTransitionResult transition = processingLedger.recordReceiptMismatch(
				archiveState.archive().idempotencyKey(),
				processingState.fingerprint().processingFingerprint(),
				processingState.attempt().count(),
				processingState.stateVersion(),
				processingState.targetBinding(),
				currentTargetBinding,
				verification,
				new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						receiptCode.code(),
						!surplus));
		if (transition == AttemptTransitionResult.OWNERSHIP_LOST) {
			return ReceiptDecision.OWNERSHIP_LOST;
		}
		return surplus ? ReceiptDecision.SURPLUS : ReceiptDecision.REINDEX;
	}

	private boolean processClaimedArchive(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			String processingFingerprint,
			ActiveIndexTargets indexTargets,
			ArchiveProcessingAttempt attempt,
			OperationBudget budget
	) {
		AtomicReference<ArchiveProcessingProgress> latestProgress =
				new AtomicReference<>(ArchiveProcessingProgress.empty());
		AtomicReference<RuntimeException> diagnosticFailure = new AtomicReference<>();
		ArchiveProcessingRequest request = new ArchiveProcessingRequest(
				kind,
				archiveState.archive().sourceUpdateTime(),
				archiveState.archive().idempotencyKey(),
				processingFingerprint,
				indexTargets,
				archiveState.stagedArchive().csvPath(),
				backendDataProperties.receiptPageSize(),
				budget);
		ArchiveProcessingResult result;
		try {
			result = archiveProcessor.process(
					request,
					progress -> {
						latestProgress.set(progress);
						requireRemaining(budget);
						return processingLedger.checkpoint(
								attempt,
								toLedgerProgress(progress),
								properties.continuity().recoveryLease())
								== AttemptTransitionResult.APPLIED;
					},
					diagnosticFailure::set);
		} catch (IndexTargetUnavailableException exception) {
			throw exception;
		} catch (OperationOwnershipLostException exception) {
			throw exception;
		} catch (OperationDeadlineExceededException _) {
			recordDeadlineFailure(
					archiveState,
					kind,
					attempt,
					latestProgress.get(),
					budget);
			return true;
		} catch (RuntimeException exception) {
			recordUnexpectedFailure(
					attempt,
					latestProgress.get(),
					exception,
					budget);
			throw exception;
		}

		switch (result.outcome()) {
			case COMPLETED -> {
				try {
					completeAttempt(
							archiveState,
							kind,
							attempt,
							result,
							budget);
				}
				catch (OperationDeadlineExceededException _) {
					recordDeadlineFailure(
							archiveState,
							kind,
							attempt,
							result.progress(),
							budget);
					return true;
				}
			}
			case FAILED -> failAttempt(
					archiveState,
					kind,
					attempt,
					result,
					diagnosticFailure.get(),
					budget);
			case OWNERSHIP_LOST -> {
				logOwnershipLost(
						archiveState,
						kind,
						attempt,
						diagnosticFailure.get());
				throw new OperationOwnershipLostException();
			}
		}
		return result.outcome() == com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome.FAILED
				&& result.failure().code()
				== ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED;
	}

	private void completeAttempt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			OperationBudget budget
	) {
		requireRemaining(budget);
		AttemptTransitionResult transition = processingLedger.markIndexed(
				attempt,
				toLedgerProgress(result.progress()),
				result.receipt());
		logOutcome(
				archiveState,
				kind,
				transition == AttemptTransitionResult.APPLIED
						? "INDEXED"
						: OUTCOME_OWNERSHIP_LOST,
				null);
		throwIfOwnershipLost(transition);
	}

	private void failAttempt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			RuntimeException diagnosticFailure,
			OperationBudget budget
	) {
		var failure = result.failure();
		if (isInterruption(failure.code())) {
			throw interruptedAttemptFailure(
					archiveState,
					kind,
					attempt,
					result,
					diagnosticFailure,
					budget);
		}
		requireTerminalOwnership(budget);
		AttemptTransitionResult transition;
		try {
			transition = processingLedger.markFailed(
					attempt,
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							failure.code().code(),
							failure.retryable()),
					toLedgerProgress(result.progress()),
					result.receipt());
		} catch (RuntimeException persistenceFailure) {
			if (diagnosticFailure != null) {
				persistenceFailure.addSuppressed(diagnosticFailure);
			}
			throw persistenceFailure;
		}
		String outcome = transition == AttemptTransitionResult.APPLIED
				? OUTCOME_FAILED
				: OUTCOME_OWNERSHIP_LOST;
		if (diagnosticFailure == null) {
			logOutcome(
					archiveState,
					kind,
					outcome,
					failure.code().code());
		}
		else {
			logExpectedAttemptFailure(
					archiveState,
					kind,
					attempt,
					result,
					diagnosticFailure,
					outcome);
		}
		throwIfOwnershipLost(transition);
	}

	private IngestionInterruptedException interruptedAttemptFailure(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			RuntimeException diagnosticFailure,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		Thread.currentThread().interrupt();
		IngestionInterruptedException interruption = diagnosticFailure == null
				? new IngestionInterruptedException()
				: new IngestionInterruptedException(diagnosticFailure);
		String outcome;
		try {
			AttemptTransitionResult transition = processingLedger.markFailed(
					attempt,
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							result.failure().code().code(),
							true),
					toLedgerProgress(result.progress()),
					result.receipt());
			outcome = transition == AttemptTransitionResult.APPLIED
					? OUTCOME_FAILED
					: OUTCOME_OWNERSHIP_LOST;
			if (transition == AttemptTransitionResult.OWNERSHIP_LOST) {
				OperationOwnershipLostException ownershipLost =
						new OperationOwnershipLostException();
				ownershipLost.addSuppressed(interruption);
				throw ownershipLost;
			}
		} catch (OperationOwnershipLostException ownershipLost) {
			throw ownershipLost;
		} catch (RuntimeException persistenceFailure) {
			interruption.addSuppressed(persistenceFailure);
			outcome = "PERSISTENCE_FAILED";
			LOGGER.atError()
					.addKeyValue(
							LOG_KEY_EVENT,
							"gdelt.processing.interruption_persistence_failed")
					.addKeyValue(
							LOG_KEY_SOURCE_UPDATE_TIME,
							archiveState.archive().sourceUpdateTime())
					.addKeyValue(LOG_KEY_ARCHIVE_KIND, kind)
					.addKeyValue(LOG_KEY_ERROR_CODE, result.failure().code().code())
					.addKeyValue(LOG_KEY_ATTEMPT_COUNT, attempt.attemptCount())
					.log("Failed to persist interrupted GDELT processing attempt");
		}
		logInterruptedAttemptFailure(
				archiveState,
				kind,
				attempt,
				result,
				interruption,
				outcome);
		return interruption;
	}

	private void recordUnexpectedFailure(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			RuntimeException exception,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		try {
			AttemptTransitionResult transition = processingLedger.markFailed(
					attempt,
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							IngestionErrorCode.INTERNAL_ERROR.code(),
							false),
					toLedgerProgress(progress),
					null);
			throwIfOwnershipLost(transition);
		} catch (OperationOwnershipLostException ownershipLost) {
			throw ownershipLost;
		} catch (RuntimeException persistenceFailure) {
			exception.addSuppressed(persistenceFailure);
		}
	}

	private static void logExpectedArchiveFailure(
			IngestionArchiveState archiveState,
			RuntimeException exception,
			IndexingFailureContract failure
	) {
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						exception,
						failure.errorCode().safeMessage()))
				.addKeyValue(LOG_KEY_EVENT, "gdelt.processing.expected_failure")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(
						LOG_KEY_ARCHIVE_KIND,
						toGdeltKind(archiveState.archive().archiveType()))
				.addKeyValue(LOG_KEY_ERROR_CODE, failure.errorCode().code())
				.addKeyValue(LOG_KEY_RETRYABLE, failure.retryable())
				.addKeyValue(LOG_KEY_OUTCOME, OUTCOME_FAILED)
				.log("GDELT archive processing failed");
	}

	private static void logExpectedAttemptFailure(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			RuntimeException diagnosticFailure,
			String outcome
	) {
		var failure = result.failure();
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						diagnosticFailure,
						failure.code().safeMessage()))
				.addKeyValue(LOG_KEY_EVENT, "gdelt.processing.expected_failure")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(LOG_KEY_ARCHIVE_KIND, kind)
				.addKeyValue(LOG_KEY_ERROR_CODE, failure.code().code())
				.addKeyValue(LOG_KEY_RETRYABLE, failure.retryable())
				.addKeyValue(LOG_KEY_ATTEMPT_COUNT, attempt.attemptCount())
				.addKeyValue(LOG_KEY_OUTCOME, outcome)
				.log("GDELT archive processing failed");
	}

	private static void logInterruptedAttemptFailure(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			IngestionInterruptedException interruption,
			String outcome
	) {
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						interruption,
						IngestionErrorCode.OPERATION_INTERRUPTED.safeMessage()))
				.addKeyValue(LOG_KEY_EVENT, "gdelt.processing.interrupted")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(LOG_KEY_ARCHIVE_KIND, kind)
				.addKeyValue(LOG_KEY_ERROR_CODE, result.failure().code().code())
				.addKeyValue(LOG_KEY_RETRYABLE, true)
				.addKeyValue(LOG_KEY_ATTEMPT_COUNT, attempt.attemptCount())
				.addKeyValue(LOG_KEY_OUTCOME, outcome)
				.log("GDELT archive processing was interrupted");
	}

	private static void logOwnershipLost(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			RuntimeException diagnosticFailure
	) {
		if (diagnosticFailure == null) {
			logOutcome(archiveState, kind, OUTCOME_OWNERSHIP_LOST, null);
			return;
		}
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						diagnosticFailure,
						IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
				.addKeyValue(
						LOG_KEY_EVENT,
						"gdelt.processing.ownership_lost_cleanup_failure")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(LOG_KEY_ARCHIVE_KIND, kind)
				.addKeyValue(LOG_KEY_ERROR_CODE, IngestionErrorCode.INTERNAL_ERROR.code())
				.addKeyValue(LOG_KEY_ATTEMPT_COUNT, attempt.attemptCount())
				.addKeyValue(LOG_KEY_OUTCOME, OUTCOME_OWNERSHIP_LOST)
				.log("GDELT archive processing lost ownership with cleanup failure");
	}

	private static void logUnexpectedArchiveFailure(
			IngestionArchiveState archiveState,
			RuntimeException exception
	) {
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						exception,
						IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
				.addKeyValue(LOG_KEY_EVENT, "gdelt.processing.internal_error")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(
						LOG_KEY_ARCHIVE_KIND,
						toGdeltKind(archiveState.archive().archiveType()))
				.addKeyValue(LOG_KEY_ERROR_CODE, IngestionErrorCode.INTERNAL_ERROR.code())
				.addKeyValue(LOG_KEY_OUTCOME, "ABORTED")
				.log("Unexpected GDELT archive processing failure");
	}

	private static com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress
			toLedgerProgress(ArchiveProcessingProgress progress) {
		return new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
				progress.deliveredRecords(),
				progress.sourceInvalidRecords(),
				progress.mappingRejectedRecords(),
				progress.submittedOperations(),
				progress.succeededOperations(),
				progress.failedOperations(),
				progress.receiptDocuments(),
				progress.firstFailedLineNumber());
	}

	private static boolean isStaged(IngestionArchiveState archiveState) {
		return archiveState.status() == IngestionArchiveStatus.STAGED
				&& archiveState.stagedArchive() != null;
	}

	private static List<IngestionArchiveState> orderedArchives(
			IngestionRunState runState
	) {
		return LATEST_ARCHIVE_ORDER.stream()
				.flatMap(type -> runState.archives().stream()
						.filter(state -> state.archive().archiveType() == type)
						.findFirst()
						.stream())
				.toList();
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

	private void recordDeadlineFailure(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			OperationBudget budget
	) {
		requireTerminalOwnership(budget);
		AttemptTransitionResult transition = processingLedger.markFailed(
				attempt,
				new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED.code(),
						true),
				toLedgerProgress(progress),
				null);
		logOutcome(
				archiveState,
				kind,
				transition == AttemptTransitionResult.APPLIED
						? OUTCOME_FAILED
						: OUTCOME_OWNERSHIP_LOST,
				ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED.code());
		throwIfOwnershipLost(transition);
	}

	private static void throwIfOwnershipLost(AttemptTransitionResult transition) {
		if (transition == AttemptTransitionResult.OWNERSHIP_LOST) {
			throw new OperationOwnershipLostException();
		}
	}

	private static boolean isInterruption(ArchiveProcessingErrorCode code) {
		return code == ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED
				|| code == ArchiveProcessingErrorCode.INDEXING_INTERRUPTED;
	}

	private static GdeltArchiveKind toGdeltKind(ArchiveType archiveType) {
		return switch (archiveType) {
			case TRANSLATION_EVENTS -> GdeltArchiveKind.TRANSLATION_EVENTS;
			case TRANSLATION_MENTIONS -> GdeltArchiveKind.TRANSLATION_MENTIONS;
		};
	}

	private static GdeltIndexKind toIndexKind(GdeltArchiveKind kind) {
		return switch (kind) {
			case TRANSLATION_EVENTS -> GdeltIndexKind.EVENT;
			case TRANSLATION_MENTIONS -> GdeltIndexKind.MENTION;
		};
	}

	private static ArchiveProcessingTargetBinding toTargetBinding(
			ActiveIndexTargets targets,
			GdeltIndexKind kind
	) {
		var target = targets.target(kind);
		return new ArchiveProcessingTargetBinding(
				kind,
				targets.partitionKey(),
				targets.partitionStateVersion(),
				targets.generationId(),
				targets.generationUuid(),
				target.indexName(),
				target.indexUuid());
	}

	private static String resolutionOutcome(IndexTargetResolution resolution) {
		return switch (resolution.status()) {
			case READY -> throw new IllegalArgumentException("READY is not a deferred outcome");
			case MAINTENANCE_DEFERRED -> "DEFERRED_MAINTENANCE";
			case OWNERSHIP_LOST -> OUTCOME_OWNERSHIP_LOST;
			case MISSING -> "DEFERRED_TARGET_MISSING";
			case WRITE_BLOCKED -> "DEFERRED_WRITE_BLOCKED";
		};
	}

	private static String claimOutcome(ArchiveProcessingClaimStatus status) {
		return switch (status) {
			case CLAIMED -> throw new IllegalArgumentException("CLAIMED is not a skipped outcome");
			case NOT_CLAIMABLE -> "SKIPPED_NOT_CLAIMABLE";
			case MAINTENANCE_DEFERRED -> "DEFERRED_MAINTENANCE";
			case OWNERSHIP_LOST -> OUTCOME_OWNERSHIP_LOST;
		};
	}

	static String targetUnavailableOutcome(IndexTargetUnavailableReason reason) {
		return switch (reason) {
			case WRITE_BLOCKED -> "DEFERRED_WRITE_BLOCKED";
			case MISSING, REPLACED -> OUTCOME_OWNERSHIP_LOST;
		};
	}

	private static void logOutcome(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			String outcome,
			String errorCode
	) {
		var log = outcome.startsWith(OUTCOME_FAILED) ? LOGGER.atError() : LOGGER.atInfo();
		log.addKeyValue(LOG_KEY_EVENT, "gdelt.processing.completed")
				.addKeyValue(
						LOG_KEY_SOURCE_UPDATE_TIME,
						archiveState.archive().sourceUpdateTime())
				.addKeyValue(LOG_KEY_ARCHIVE_KIND, kind)
				.addKeyValue(LOG_KEY_OUTCOME, outcome);
		if (errorCode != null) {
			log = log.addKeyValue(LOG_KEY_ERROR_CODE, errorCode);
		}
		log.log("GDELT archive processing finished");
	}

	private enum ReceiptDecision {
		MATCHED("SKIPPED_RECEIPT_MATCHED", null),
		REINDEX("REINDEX", null),
		SURPLUS(
				"FAILED_RECEIPT_SURPLUS",
				ArchiveProcessingErrorCode.INDEX_RECEIPT_SURPLUS.code()),
		OWNERSHIP_LOST(OUTCOME_OWNERSHIP_LOST, null);

		private final String outcome;
		private final String errorCode;

		ReceiptDecision(String outcome, String errorCode) {
			this.outcome = outcome;
			this.errorCode = errorCode;
		}

		private String outcome() {
			return outcome;
		}

		private String errorCode() {
			return errorCode;
		}
	}

	private record RecentWorkPreparation(
			boolean processKnownWork,
			RuntimeException deferredFailure
	) {

		private static RecentWorkPreparation skip() {
			return new RecentWorkPreparation(false, null);
		}

		private static RecentWorkPreparation ready() {
			return new RecentWorkPreparation(true, null);
		}

		private static RecentWorkPreparation ready(RuntimeException deferredFailure) {
			return new RecentWorkPreparation(true, deferredFailure);
		}
	}

	private record ArchiveStepResult(
			boolean deadlineReached,
			RuntimeException deferredFailure
	) {

		private static ArchiveStepResult completed() {
			return new ArchiveStepResult(false, null);
		}

		private static ArchiveStepResult deadline() {
			return new ArchiveStepResult(true, null);
		}

		private static ArchiveStepResult deferred(RuntimeException failure) {
			return new ArchiveStepResult(false, failure);
		}
	}

	private record ArchiveBatchResult(
			boolean deadlineReached,
			RuntimeException deferredFailure
	) {

		private static ArchiveBatchResult completed(RuntimeException deferredFailure) {
			return new ArchiveBatchResult(false, deferredFailure);
		}

		private static ArchiveBatchResult deadline(RuntimeException deferredFailure) {
			return new ArchiveBatchResult(true, deferredFailure);
		}
	}
}
