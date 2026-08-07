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
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Координирует acquisition и durable processing последнего полного GDELT update.
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

	private final IngestionRunService ingestionRunService;
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
	 * Создает сквозной orchestration service поверх module API boundaries.
	 *
	 * @param ingestionRunService acquisition одного latest update
	 * @param processingLedger durable processing ownership
	 * @param archiveProcessor потоковый mapper и indexer
	 * @param fingerprintFactory versioned processing identity
	 * @param indexWriter проверка Elasticsearch receipts
	 * @param indexTargetResolver lifecycle exact ACTIVE generation
	 * @param properties runtime lease policy
	 * @param backendDataProperties bounded backend-data verification policy
	 * @param metrics bounded cycle metrics
	 * @param storageMonitor единая проверка storage pressure перед index growth
	 */
	public GdeltPipelineService(
			IngestionRunService ingestionRunService,
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
	 * Загружает latest update и независимо обрабатывает каждый staged archive.
	 *
	 * @return неизмененное acquisition state текущего update
	 */
	public IngestionRunState runLatestUpdate() {
		OperationBudget budget = OperationBudget.start(
				backendDataProperties.operationDeadline());
		IngestionRunState runState = ingestionRunService.runLatestUpdate();
		processRunState(runState, budget);
		return runState;
	}

	/**
	 * Выполняет ровно один bounded latest-first cycle без sleep и scheduling.
	 *
	 * @return typed terminal outcome one-shot cycle
	 */
	public IngestionOneShotOutcome runOneShot() {
		return runOneShot(OperationBudget.start(
				backendDataProperties.operationDeadline()));
	}

	IngestionOneShotOutcome runOneShot(OperationBudget budget) {
		long startedAt = System.nanoTime();
		IngestionOperationMetricOutcome metricOutcome =
				IngestionOperationMetricOutcome.INTERNAL_FAILURE;
		try {
			AcquisitionCycleResult acquisition = ingestionRunService.runOneShot(budget);
			if (acquisition.runState().isPresent()
					&& processRunState(acquisition.runState().orElseThrow(), budget)) {
				metricOutcome = IngestionOperationMetricOutcome.OPERATION_DEADLINE_EXCEEDED;
				return IngestionOneShotOutcome.OPERATION_DEADLINE_EXCEEDED;
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
		catch (StoragePressureException _) {
			metricOutcome = IngestionOperationMetricOutcome.STORAGE_PRESSURE;
			return IngestionOneShotOutcome.STORAGE_PRESSURE;
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

	private boolean processRunState(
			IngestionRunState runState,
			OperationBudget budget
	) {
		RuntimeException deferredFailure = null;
		for (IngestionArchiveState archiveState : runState.archives()) {
			if (isStaged(archiveState)) {
				if (!budget.hasRemaining()) {
					return true;
				}
				try {
					if (processStagedArchive(archiveState, budget)) {
						return true;
					}
				} catch (OperationDeadlineExceededException _) {
					return true;
				} catch (IngestionInterruptedException exception) {
					throw exception;
				} catch (StoragePressureException exception) {
					throw exception;
				} catch (IndexingInterruptedException exception) {
					logExpectedArchiveFailure(archiveState, exception, exception);
					throw exception;
				} catch (IndexTargetUnavailableException exception) {
					logOutcome(
							archiveState,
							toGdeltKind(archiveState.archive().archiveType()),
							targetUnavailableOutcome(exception.reason()),
							exception.reason().errorCode().code());
				} catch (IndexingAccessException | IndexingProtocolException exception) {
					logExpectedArchiveFailure(archiveState, exception, exception);
					if (deferredFailure == null) {
						deferredFailure = exception;
					}
					else {
						deferredFailure.addSuppressed(exception);
					}
				} catch (RuntimeException exception) {
					logUnexpectedArchiveFailure(archiveState, exception);
					throw exception;
				}
			}
		}
		if (deferredFailure != null) {
			throw deferredFailure;
		}
		return false;
	}

	private boolean processStagedArchive(
			IngestionArchiveState archiveState,
			OperationBudget budget
	) {
		requireRemaining(budget);
		GdeltArchiveKind kind = toGdeltKind(archiveState.archive().archiveType());
		GdeltIndexKind indexKind = toIndexKind(kind);
		storageMonitor.requireCapacity(StorageResource.ELASTICSEARCH);
		requireRemaining(budget);
		IndexTargetResolution targetResolution = indexTargetResolver.resolve(
				archiveState.archive().sourceUpdateTime());
		if (targetResolution.status() != IndexTargetResolutionStatus.READY) {
			logOutcome(
					archiveState,
					kind,
					resolutionOutcome(targetResolution),
					null);
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
		if (processingState.status() == ArchiveProcessingStatus.INDEXED) {
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
			logOutcome(archiveState, kind, claimOutcome(claimResult.status()), null);
			return false;
		}
		ArchiveProcessingAttempt attempt = claimResult.attempt();
		if (!attempt.targetBinding().equals(targetBinding)) {
			throw new IllegalStateException("Processing claim returned another target binding");
		}
		return processClaimedArchive(
				archiveState,
				kind,
				processingFingerprint,
				indexTargets,
				attempt,
				budget);
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
		var verification = indexWriter.verifyReceipt(query);
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
						return processingLedger.checkpoint(
								attempt,
								toLedgerProgress(progress),
								properties.continuity().recoveryLease())
								== AttemptTransitionResult.APPLIED;
					},
					diagnosticFailure::set);
		} catch (IndexTargetUnavailableException exception) {
			throw exception;
		} catch (RuntimeException exception) {
			recordUnexpectedFailure(attempt, latestProgress.get(), exception);
			throw exception;
		}

		switch (result.outcome()) {
			case COMPLETED -> completeAttempt(archiveState, kind, attempt, result);
			case FAILED -> failAttempt(
					archiveState,
					kind,
					attempt,
					result,
					diagnosticFailure.get());
			case OWNERSHIP_LOST -> logOwnershipLost(
					archiveState,
					kind,
					attempt,
					diagnosticFailure.get());
		}
		return result.outcome() == com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome.FAILED
				&& result.failure().code()
				== ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED;
	}

	private void completeAttempt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result
	) {
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
	}

	private void failAttempt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			RuntimeException diagnosticFailure
	) {
		var failure = result.failure();
		if (isInterruption(failure.code())) {
			throw interruptedAttemptFailure(
					archiveState,
					kind,
					attempt,
					result,
					diagnosticFailure);
		}
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
	}

	private IngestionInterruptedException interruptedAttemptFailure(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result,
			RuntimeException diagnosticFailure
	) {
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
			RuntimeException exception
	) {
		try {
			processingLedger.markFailed(
					attempt,
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							IngestionErrorCode.INTERNAL_ERROR.code(),
							false),
					toLedgerProgress(progress),
					null);
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

	private static void requireRemaining(OperationBudget budget) {
		if (!budget.hasRemaining()) {
			throw new OperationDeadlineExceededException();
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
}
