package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingFailureContract;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import java.util.Optional;
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
	private final GdeltIngestionProperties properties;

	/**
	 * Создает сквозной orchestration service поверх module API boundaries.
	 *
	 * @param ingestionRunService acquisition одного latest update
	 * @param processingLedger durable processing ownership
	 * @param archiveProcessor потоковый mapper и indexer
	 * @param fingerprintFactory versioned processing identity
	 * @param indexWriter проверка Elasticsearch receipts
	 * @param properties runtime lease policy
	 */
	public GdeltPipelineService(
			IngestionRunService ingestionRunService,
			ArchiveProcessingLedger processingLedger,
			GdeltArchiveProcessor archiveProcessor,
			ProcessingFingerprintFactory fingerprintFactory,
			GdeltIndexWriter indexWriter,
			GdeltIngestionProperties properties
	) {
		this.ingestionRunService = ingestionRunService;
		this.processingLedger = processingLedger;
		this.archiveProcessor = archiveProcessor;
		this.fingerprintFactory = fingerprintFactory;
		this.indexWriter = indexWriter;
		this.properties = properties;
	}

	/**
	 * Загружает latest update и независимо обрабатывает каждый staged archive.
	 *
	 * @return неизмененное acquisition state текущего update
	 */
	public IngestionRunState runLatestUpdate() {
		IngestionRunState runState = ingestionRunService.runLatestUpdate();
		RuntimeException deferredFailure = null;
		for (IngestionArchiveState archiveState : runState.archives()) {
			if (isStaged(archiveState)) {
				try {
					processStagedArchive(archiveState);
				} catch (IngestionInterruptedException exception) {
					throw exception;
				} catch (IndexingInterruptedException exception) {
					logExpectedArchiveFailure(archiveState, exception, exception);
					throw exception;
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
		return runState;
	}

	private void processStagedArchive(IngestionArchiveState archiveState) {
		GdeltArchiveKind kind = toGdeltKind(archiveState.archive().archiveType());
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
		ArchiveProcessingState processingState =
				processingLedger.register(archiveKey, fingerprint);
		if (processingState.status() == ArchiveProcessingStatus.INDEXED) {
			ReceiptDecision receiptDecision =
					reconcileIndexedReceipt(archiveState, kind, processingState);
			if (receiptDecision != ReceiptDecision.REINDEX) {
				logOutcome(
						archiveState,
						kind,
						receiptDecision.outcome(),
						receiptDecision.errorCode());
				return;
			}
		}

		Optional<ArchiveProcessingAttempt> claimed = processingLedger.claim(
				archiveKey,
				properties.continuity().recoveryLease());
		if (claimed.isEmpty()) {
			logOutcome(archiveState, kind, "SKIPPED_NOT_CLAIMABLE", null);
			return;
		}
		processClaimedArchive(
				archiveState,
				kind,
				processingFingerprint,
				claimed.orElseThrow());
	}

	private ReceiptDecision reconcileIndexedReceipt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingState processingState
	) {
		var query = new ArchiveReceiptQuery(
				toIndexKind(kind),
				archiveState.archive().idempotencyKey(),
				processingState.progress().receiptDocuments());
		var verification = indexWriter.verifyReceipt(query);
		if (verification.matched()) {
			return ReceiptDecision.MATCHED;
		}
		boolean surplus =
				verification.actualDocumentCount() > verification.expectedDocumentCount();
		ArchiveProcessingErrorCode receiptCode = surplus
				? ArchiveProcessingErrorCode.INDEX_RECEIPT_SURPLUS
				: ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH;
		AttemptTransitionResult transition = processingLedger.recordReceiptMismatch(
				archiveState.archive().idempotencyKey(),
				processingState.fingerprint().processingFingerprint(),
				processingState.attempt().count(),
				new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
						receiptCode.code(),
						!surplus));
		if (transition == AttemptTransitionResult.OWNERSHIP_LOST) {
			return ReceiptDecision.OWNERSHIP_LOST;
		}
		return surplus ? ReceiptDecision.SURPLUS : ReceiptDecision.REINDEX;
	}

	private void processClaimedArchive(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			String processingFingerprint,
			ArchiveProcessingAttempt attempt
	) {
		AtomicReference<ArchiveProcessingProgress> latestProgress =
				new AtomicReference<>(ArchiveProcessingProgress.empty());
		AtomicReference<RuntimeException> diagnosticFailure = new AtomicReference<>();
		ArchiveProcessingRequest request = new ArchiveProcessingRequest(
				kind,
				archiveState.archive().sourceUpdateTime(),
				archiveState.archive().idempotencyKey(),
				processingFingerprint,
				archiveState.stagedArchive().csvPath());
		ArchiveProcessingResult result;
		try {
			result = archiveProcessor.process(
					request,
					progress -> {
						latestProgress.set(progress);
						return processingLedger.checkpoint(
								attempt.archiveIdempotencyKey(),
								attempt.token(),
								toLedgerProgress(progress),
								properties.continuity().recoveryLease())
								== AttemptTransitionResult.APPLIED;
					},
					diagnosticFailure::set);
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
	}

	private void completeAttempt(
			IngestionArchiveState archiveState,
			GdeltArchiveKind kind,
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingResult result
	) {
		AttemptTransitionResult transition = processingLedger.markIndexed(
				attempt.archiveIdempotencyKey(),
				attempt.token(),
				toLedgerProgress(result.progress()));
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
					attempt.archiveIdempotencyKey(),
					attempt.token(),
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							failure.code().code(),
							failure.retryable()),
					toLedgerProgress(result.progress()));
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
					attempt.archiveIdempotencyKey(),
					attempt.token(),
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							result.failure().code().code(),
							true),
					toLedgerProgress(result.progress()));
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
					attempt.archiveIdempotencyKey(),
					attempt.token(),
					new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure(
							IngestionErrorCode.INTERNAL_ERROR.code(),
							false),
					toLedgerProgress(progress));
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
