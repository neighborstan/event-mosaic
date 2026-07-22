package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.ArchiveContentViolationException;
import com.neighbor.eventmosaic.ingestion.error.IngestionFailureContract;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.ingestion.error.StagingStorageException;
import com.neighbor.eventmosaic.ingestion.error.TransferredArtifactIntegrityException;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestClient;
import com.neighbor.eventmosaic.ingestion.source.GdeltManifestParser;
import com.neighbor.eventmosaic.ingestion.staging.DownloadedArchive;
import com.neighbor.eventmosaic.ingestion.staging.HttpArchiveDownloader;
import com.neighbor.eventmosaic.ingestion.staging.StagingLayout;
import com.neighbor.eventmosaic.ingestion.staging.StagingPaths;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;
import org.springframework.stereotype.Service;

/**
 * Координирует один latest-first acquisition cycle без привязки к trigger.
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
	private final StagingLayout stagingLayout;
	private final HttpArchiveDownloader archiveDownloader;
	private final ZipArchiveStager archiveStager;
	private final GdeltIngestionProperties properties;
	private final IngestionMetrics metrics;

	/**
	 * Собирает orchestration boundary из source, ledger и staging collaborators.
	 *
	 * @param manifestClient клиент latest manifest
	 * @param manifestParser parser целостного GDELT update
	 * @param archiveLedger durable state boundary
	 * @param stagingLayout построитель безопасных staging paths
	 * @param archiveDownloader downloader и verifier ZIP
	 * @param archiveStager безопасный ZIP stager
	 * @param properties runtime policy ingestion
	 * @param metrics publisher метрик с заранее ограниченными наборами значений тегов
	 */
	public IngestionRunService(
			GdeltManifestClient manifestClient,
			GdeltManifestParser manifestParser,
			IngestionArchiveLedger archiveLedger,
			StagingLayout stagingLayout,
			HttpArchiveDownloader archiveDownloader,
			ZipArchiveStager archiveStager,
			GdeltIngestionProperties properties,
			IngestionMetrics metrics
	) {
		this.manifestClient = manifestClient;
		this.manifestParser = manifestParser;
		this.archiveLedger = archiveLedger;
		this.stagingLayout = stagingLayout;
		this.archiveDownloader = archiveDownloader;
		this.archiveStager = archiveStager;
		this.properties = properties;
		this.metrics = metrics;
	}

	/**
	 * Обнаруживает latest update, регистрирует state и независимо обрабатывает
	 * Event и Mention archives.
	 *
	 * @return итоговое производное состояние run
	 */
	public IngestionRunState runLatestUpdate() {
		metrics.runStarted();
		try {
			return executeLatestUpdate();
		} catch (UnexpectedArchiveFailure failure) {
			metrics.error(IngestionErrorCode.INTERNAL_ERROR);
			logUnexpectedArchiveFailure(failure);
			throw failure.original();
		} catch (RemoteSourceAccessException
				| TransferredArtifactIntegrityException
				| RemoteResponseRejectedException
				| SourceDataViolationException
				| ArchiveContentViolationException
				| StagingStorageException
				| IngestionInterruptedException exception) {
			logExpectedCycleFailure(exception);
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

	private IngestionRunState executeLatestUpdate() {
		DiscoveredUpdate update = manifestParser.parse(manifestClient.fetchLatestManifest());

		for (DiscoveryDiagnostic diagnostic : update.diagnostics()) {
			metrics.event(diagnostic.code());
			LOGGER.atInfo()
					.addKeyValue(EVENT_FIELD, "gdelt.manifest.entry_ignored")
					.addKeyValue(SOURCE_UPDATE_TIME_FIELD, update.sourceUpdateTime())
					.addKeyValue("event_code", diagnostic.code())
					.addKeyValue("line_number", diagnostic.lineNumber())
					.log("GDELT manifest entry ignored");
		}

		int gapsCreated = archiveLedger.registerDiscoveredUpdate(
				update,
				properties.continuity().firstRunPolicy(),
				properties.continuity().firstRunStartAt()
		);
		metrics.gapsCreated(gapsCreated);
		for (DiscoveredArchive archive : update.archives()) {
			processArchive(archive);
		}

		IngestionRunState result = archiveLedger.findRunByUpdateTime(update.sourceUpdateTime())
				.orElseThrow(() -> new IllegalStateException("Registered ingestion run disappeared"));
		metrics.runCompleted(result.status());
		LOGGER.atInfo()
				.addKeyValue(EVENT_FIELD, "gdelt.cycle.completed")
				.addKeyValue(SOURCE_UPDATE_TIME_FIELD, result.sourceUpdateTime())
				.addKeyValue("run_status", result.status())
				.log("GDELT ingestion cycle finished");
		return result;
	}

	private void processArchive(DiscoveredArchive archive) {
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
			StagingPaths paths = stagingLayout.pathsFor(attempt);
			DownloadedArchive downloaded = archiveDownloader.download(attempt, paths);
			downloadCompleted = true;
			metrics.downloadOutcome(
					archive.archiveType(),
					downloaded.reused() ? DownloadOutcome.REUSED : DownloadOutcome.DOWNLOADED
			);
			StagedArchive staged = archiveStager.stage(attempt, downloaded, paths);
			if (ownershipLost(
					archive,
					attempt,
					archiveLedger.markStaged(archive.idempotencyKey(), attempt.token(), staged))) {
				return;
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
				| IngestionInterruptedException exception) {
			handleExpectedArchiveFailure(archive, attempt, exception, !downloadCompleted);
		} catch (RuntimeException exception) {
			IngestionFailure failure = IngestionFailure.internalError();
			persistUnexpectedFailure(archive, attempt, failure, exception);
			throw new UnexpectedArchiveFailure(archive, attempt, exception);
		}
	}

	private <T extends ApplicationException & IngestionFailureContract> void handleExpectedArchiveFailure(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			T exception,
			boolean downloadFailed
	) {
		IngestionFailure failure = exception.failure();
		AttemptTransitionResult transition = persistExpectedFailure(archive, attempt, exception, failure);
		if (ownershipLost(archive, attempt, transition)) {
			if (exception.abortsCycle()) {
				throw exception;
			}
			return;
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

	private AttemptTransitionResult persistExpectedFailure(
			DiscoveredArchive archive,
			ArchiveAttempt attempt,
			ApplicationException exception,
			IngestionFailure failure
	) {
		try {
			return archiveLedger.markFailed(
					archive.idempotencyKey(),
					attempt.token(),
					failure
			);
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
			RuntimeException exception
	) {
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
			}
		} catch (RuntimeException persistenceFailure) {
			exception.addSuppressed(persistenceFailure);
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
}
