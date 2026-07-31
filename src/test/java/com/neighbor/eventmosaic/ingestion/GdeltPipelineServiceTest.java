package com.neighbor.eventmosaic.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
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
	private static final Instant NOW = Instant.parse("2026-07-30T12:00:00Z");

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

	private GdeltPipelineService service;

	@BeforeEach
	void setUp() {
		service = new GdeltPipelineService(
				ingestionRunService,
				processingLedger,
				archiveProcessor,
				fingerprintFactory,
				indexWriter,
				GdeltTestFixtures.properties(tempDir, 1024 * 1024));
		when(fingerprintFactory.create(
				anyString(),
				any(GdeltArchiveKind.class),
				anyString(),
				anyString()))
				.thenAnswer(invocation -> invocation.getArgument(1)
						== GdeltArchiveKind.TRANSLATION_EVENTS
								? EVENT_FINGERPRINT
								: MENTION_FINGERPRINT);
		when(processingLedger.checkpoint(anyString(), any(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.markIndexed(anyString(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.markFailed(anyString(), any(), any(), any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
	}

	@Test
	@DisplayName("Регистрирует, claim-ит и завершает оба staged архива")
	void processesBothStagedArchives() {
		IngestionRunState runState = runState(true);
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		stubPendingRegistrationAndClaims();
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			ArchiveProcessingProgressListener listener = invocation.getArgument(1);
			ArchiveProcessingProgress progress = successfulProgress();
			assertThat(listener.onProgress(progress)).isTrue();
			return ArchiveProcessingResult.completed(request.kind(), progress);
		});

		assertThat(service.runLatestUpdate()).isSameAs(runState);

		verify(archiveProcessor, org.mockito.Mockito.times(2)).process(any(), any(), any());
		verify(processingLedger, org.mockito.Mockito.times(2))
				.markIndexed(anyString(), any(), any());
		verifyNoInteractions(indexWriter);
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
		verify(processingLedger, never()).markIndexed(anyString(), any(), any());
		verify(processingLedger, never()).markFailed(anyString(), any(), any(), any());
	}

	@Test
	@DisplayName("Не создает новый attempt при совпавшем receipt INDEXED архива")
	void skipsIndexedArchiveWithMatchingReceipt() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(any())).thenReturn(
				new ArchiveReceiptVerification(
						GdeltIndexKind.EVENT,
						1,
						1,
						ArchiveReceiptStatus.MATCHED));

		service.runLatestUpdate();

		verify(processingLedger, never()).claim(anyString(), any());
		verifyNoInteractions(archiveProcessor);
	}

	@Test
	@DisplayName("Открывает полный reindex после удаления целевого индекса")
	void reopensIndexedArchiveAfterMissingIndex() {
		IngestionRunState runState = runState(false);
		IngestionArchiveState archive = runState.archives().getFirst();
		when(ingestionRunService.runLatestUpdate()).thenReturn(runState);
		when(processingLedger.register(anyString(), any()))
				.thenAnswer(invocation -> indexedState(
						archive,
						invocation.getArgument(1)));
		when(indexWriter.verifyReceipt(any())).thenReturn(
				new ArchiveReceiptVerification(
						GdeltIndexKind.EVENT,
						1,
						0,
						ArchiveReceiptStatus.INDEX_ABSENT));
		when(processingLedger.recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				any()))
				.thenReturn(AttemptTransitionResult.APPLIED);
		when(processingLedger.claim(anyString(), any())).thenAnswer(invocation ->
				attempt(
						archive.archive().idempotencyKey(),
						eventFingerprint()));
		when(archiveProcessor.process(any(), any(), any())).thenReturn(
				ArchiveProcessingResult.completed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						successfulProgress()));

		service.runLatestUpdate();

		verify(processingLedger).recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				any());
		verify(archiveProcessor).process(any(), any(), any());
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
				new ArchiveReceiptVerification(
						GdeltIndexKind.EVENT,
						1,
						2,
						ArchiveReceiptStatus.MISMATCHED));
		when(processingLedger.recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				any()))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		service.runLatestUpdate();

		verify(processingLedger, never()).claim(anyString(), any());
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
				new ArchiveReceiptVerification(
						GdeltIndexKind.EVENT,
						1,
						2,
						ArchiveReceiptStatus.MISMATCHED));
		when(processingLedger.recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				any()))
				.thenReturn(AttemptTransitionResult.APPLIED);

		service.runLatestUpdate();

		verify(processingLedger).recordReceiptMismatch(
				anyString(),
				anyString(),
				anyInt(),
				org.mockito.ArgumentMatchers.argThat(failure ->
						"INDEX_RECEIPT_SURPLUS".equals(failure.errorCode())
								&& !failure.retryable()));
		verify(processingLedger, never()).claim(anyString(), any());
		verifyNoInteractions(archiveProcessor);
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
				.recordReceiptMismatch(anyString(), anyString(), anyInt(), any());
		verify(processingLedger, never()).claim(anyString(), any());
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
				any()))
				.thenReturn(attempt(mentionArchiveKey, mentionFingerprint()));
		when(indexWriter.verifyReceipt(any(ArchiveReceiptQuery.class))).thenThrow(failure);
		when(archiveProcessor.process(any(), any(), any())).thenAnswer(invocation -> {
			ArchiveProcessingRequest request = invocation.getArgument(0);
			return ArchiveProcessingResult.completed(request.kind(), successfulProgress());
		});

		assertThatThrownBy(service::runLatestUpdate).isSameAs(failure);

		verify(archiveProcessor).process(
				org.mockito.ArgumentMatchers.argThat(request ->
						request.kind() == GdeltArchiveKind.TRANSLATION_MENTIONS),
				any(),
				any());
		verify(processingLedger).markIndexed(
				org.mockito.ArgumentMatchers.eq(mentionArchiveKey),
				any(),
				any());
		verify(processingLedger, never())
				.recordReceiptMismatch(anyString(), anyString(), anyInt(), any());
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
		verify(processingLedger, never()).claim(anyString(), any());
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
			return ArchiveProcessingResult.completed(request.kind(), successfulProgress());
		});

		service.runLatestUpdate();

		verify(processingLedger).markFailed(
				org.mockito.ArgumentMatchers.contains(".export."),
				any(),
				any(),
				any());
		verify(processingLedger).markIndexed(
				org.mockito.ArgumentMatchers.contains(".mentions."),
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
			verify(processingLedger).markFailed(anyString(), any(), any(), any());
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
		when(processingLedger.markFailed(anyString(), any(), any(), any()))
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
				anyString(),
				any(),
				org.mockito.ArgumentMatchers.argThat(failure ->
						IngestionErrorCode.INTERNAL_ERROR.code()
								.equals(failure.errorCode())
								&& !failure.retryable()),
				org.mockito.ArgumentMatchers.argThat(progress ->
						progress.deliveredRecords() == 2
								&& progress.mappingRejectedRecords() == 1
								&& progress.succeededOperations() == 1));
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
		when(processingLedger.markFailed(anyString(), any(), any(), any()))
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
		when(processingLedger.claim(anyString(), any())).thenAnswer(invocation -> {
			String archiveKey = invocation.getArgument(0);
			boolean event = archiveKey.contains(".export.");
			return attempt(
					archiveKey,
					event ? eventFingerprint() : mentionFingerprint());
		});
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
				new ArchiveProcessingAttemptState(
						0,
						null,
						null,
						null,
						AutomaticRetryState.initial(3)),
				com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress.empty(),
				null,
				NOW,
				null);
	}

	private static ArchiveProcessingState indexedState(
			IngestionArchiveState archive,
			ArchiveProcessingFingerprint fingerprint
	) {
		return new ArchiveProcessingState(
				archive.archive().idempotencyKey(),
				fingerprint,
				ArchiveProcessingStatus.INDEXED,
				new ArchiveProcessingAttemptState(
						1,
						null,
						NOW,
						null,
						AutomaticRetryState.initial(3)),
				new com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress(
						1, 0, 0, 1, 1, 0, 1, null),
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

	private static java.util.Optional<ArchiveProcessingAttempt> attempt(
			String archiveKey,
			ArchiveProcessingFingerprint fingerprint
	) {
		return java.util.Optional.of(new ArchiveProcessingAttempt(
				archiveKey,
				fingerprint,
				UUID.randomUUID(),
				NOW.plusSeconds(900),
				1,
				false));
	}
}
