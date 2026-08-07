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
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.observability.BackendDataStorageMonitor;
import com.neighbor.eventmosaic.ingestion.error.StoragePressureException;
import com.neighbor.eventmosaic.ingestion.observability.StoragePressureState;
import com.neighbor.eventmosaic.ingestion.observability.StorageResource;
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
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
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
		service = new GdeltPipelineService(
				ingestionRunService,
				processingLedger,
				archiveProcessor,
				fingerprintFactory,
				indexWriter,
				indexTargetResolver,
				GdeltTestFixtures.properties(tempDir, 1024 * 1024),
				GdeltTestFixtures.backendDataProperties(),
				metrics,
				storageMonitor);
		when(indexTargetResolver.resolve(any())).thenReturn(
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
	}

	@Test
	@DisplayName("Deferred source poll завершает one-shot без downstream I/O")
	void deferredSourcePollFinishesWithoutDownstreamIo() {
		when(ingestionRunService.runOneShot(any()))
				.thenReturn(new AcquisitionCycleResult(Optional.empty(), true));

		assertThat(service.runOneShot()).isEqualTo(IngestionOneShotOutcome.RETRY_DEFERRED);

		verifyNoInteractions(processingLedger, archiveProcessor, indexWriter);
	}

	@Test
	@DisplayName("Elasticsearch pressure завершает one-shot до resolver, claim и bulk")
	void elasticsearchPressureStopsBeforeResolverClaimAndBulk() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.runOneShot(any()))
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
	@DisplayName("Deadline после завершенного Event не начинает Mention и не откатывает Event")
	void deadlineAfterCompletedEventDoesNotStartMention() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.runOneShot(any()))
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
		verify(processingLedger).markIndexed(any(), any(), any());
		verify(processingLedger).register(
				runState.archives().getFirst().archive().idempotencyKey(),
				new ArchiveProcessingFingerprint(
						GdeltTestFixtures.EVENT_MD5,
						"gdelt-processing-v1",
						EVENT_FINGERPRINT));
	}

	@Test
	@DisplayName("Deadline active processing сохраняется как retryable durable failure")
	void activeProcessingDeadlineIsPersistedAsRetryableFailure() {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.runOneShot(any()))
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
		when(indexTargetResolver.resolve(any())).thenReturn(
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

		assertThat(service.runLatestUpdate()).isSameAs(runState);

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
		when(indexWriter.verifyReceipt(any())).thenReturn(matched);
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
		verify(indexWriter).verifyReceipt(org.mockito.ArgumentMatchers.argThat(query ->
				query.target().equals(ACTIVE_TARGETS.event())));
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
		when(indexWriter.verifyReceipt(any())).thenReturn(mismatch);
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

		service.runLatestUpdate();

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
		when(indexTargetResolver.resolve(any())).thenReturn(
				IndexTargetResolution.outcome(IndexTargetResolutionStatus.MISSING));

		service.runLatestUpdate();

		verifyNoInteractions(processingLedger, indexWriter, archiveProcessor);
	}

	@Test
	@DisplayName("Потеря ownership при receipt reopen не создаёт новый attempt")
	void skipsClaimWhenReceiptReopenLosesOwnership() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(any())).thenReturn(
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
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		service.runLatestUpdate();

		verify(processingLedger, never()).claim(anyString(), any(), any());
		verifyNoInteractions(archiveProcessor);
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
		when(indexWriter.verifyReceipt(any())).thenReturn(
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
		when(indexWriter.verifyReceipt(any())).thenReturn(
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
		when(indexWriter.verifyReceipt(any(ArchiveReceiptQuery.class))).thenThrow(failure);

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
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(any(ArchiveReceiptQuery.class))).thenThrow(
				new IndexTargetUnavailableException(IndexTargetUnavailableReason.MISSING));

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		verify(processingLedger, never())
				.recordReceiptMismatch(
						anyString(), anyString(), anyInt(), anyLong(), any(), any(), any(), any());
		verify(processingLedger, never()).claim(anyString(), any(), any());
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
		when(indexWriter.verifyReceipt(any(ArchiveReceiptQuery.class))).thenThrow(failure);
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
		when(indexWriter.verifyReceipt(any(ArchiveReceiptQuery.class)))
				.thenThrow(firstFailure, secondFailure);

		assertThatThrownBy(service::runLatestUpdate)
				.isSameAs(firstFailure)
				.satisfies(exception -> assertThat(exception.getSuppressed())
						.containsExactly(secondFailure));

		verify(indexWriter, org.mockito.Mockito.times(2))
				.verifyReceipt(any(ArchiveReceiptQuery.class));
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

	private void assertPostClaimTargetOutcome(
			IndexTargetUnavailableReason reason,
			String expectedOutcome,
			String expectedErrorCode
	) {
		IngestionRunState runState = runState(false);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenThrow(
				new IndexTargetUnavailableException(reason));

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		assertThat(GdeltPipelineService.targetUnavailableOutcome(reason))
				.isEqualTo(expectedOutcome);
		assertThat(reason.errorCode().code()).isEqualTo(expectedErrorCode);
		verify(processingLedger, never()).markIndexed(any(), any(), any());
		verify(processingLedger, never()).markFailed(any(), any(), any(), any());
	}

	private IngestionRunState runState(boolean includeMention) {
		var update = GdeltTestFixtures.update(GdeltTestFixtures.UPDATE_TIME);
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
}
