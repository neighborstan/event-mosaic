package com.neighbor.eventmosaic.ingestion.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.CleanupCandidateSnapshot;
import com.neighbor.eventmosaic.indexing.api.CleanupClaim;
import com.neighbor.eventmosaic.indexing.api.CleanupCompletionEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupDeleteEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupOperation;
import com.neighbor.eventmosaic.indexing.api.CleanupOrphanOwner;
import com.neighbor.eventmosaic.indexing.api.CleanupOwnership;
import com.neighbor.eventmosaic.indexing.api.CleanupProtectedActive;
import com.neighbor.eventmosaic.indexing.api.CleanupTransitionResult;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexCleanupLedger;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ExactIndexAliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ObservedIndex;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.ingestion.api.ArchiveAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupErrorCode;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupRejectedException;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupCommand;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupOutcome;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationWriteOutcome;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReplaySourcePlan;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import java.net.URI;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

@DisplayName("Защищенное выполнение очистки технического поколения")
class DefaultGenerationCleanupServiceFaultTest {

	private static final String PARTITION_KEY = "p20260727";
	private static final Instant PARTITION_START = Instant.parse("2026-07-27T00:00:00Z");
	private static final Instant PARTITION_END = Instant.parse("2026-08-03T00:00:00Z");
	private static final Instant NOW = Instant.parse("2026-08-10T12:00:00Z");
	private static final UUID GENERATION_UUID =
			UUID.fromString("11111111-1111-1111-1111-111111111111");
	private static final UUID OPERATION_TOKEN =
			UUID.fromString("22222222-2222-2222-2222-222222222222");
	private static final IndexGenerationNames GENERATION_NAMES = new IndexGenerationNames(
			"gdelt-events-v1-p20260727-g0002",
			"gdelt-mentions-v1-p20260727-g0002");
	private static final String EVENT_UUID = "event-index-uuid";
	private static final String MENTION_UUID = "mention-index-uuid";
	private static final String ACTOR = "local.operator";
	private static final String REASON_CODE = "FAILED_BUILD_CLEANUP";

	private final IndexCleanupLedger cleanupLedger = mock(IndexCleanupLedger.class);
	private final IndexLifecycleLedger lifecycleLedger = mock(IndexLifecycleLedger.class);
	private final IndexMaintenanceGateway elasticsearch = mock(IndexMaintenanceGateway.class);
	private final IngestionArchiveLedger archiveLedger = mock(IngestionArchiveLedger.class);
	private final ArchiveProcessingLedger processingLedger = mock(ArchiveProcessingLedger.class);
	private final ZipArchiveStager archiveStager = mock(ZipArchiveStager.class);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
	private final DefaultGenerationCleanupService service = new DefaultGenerationCleanupService(
			cleanupLedger,
			lifecycleLedger,
			elasticsearch,
			archiveLedger,
			processingLedger,
			archiveStager,
			properties(),
			clock);

	private IngestionArchiveState replaySource;

	@BeforeEach
	void setUpReplaySource() {
		replaySource = replaySource();
		when(lifecycleLedger.findPartition(PARTITION_KEY))
				.thenReturn(Optional.of(partition(null)));
		when(archiveLedger.findStagedBetween(PARTITION_START, PARTITION_END))
				.thenReturn(List.of(replaySource));
		when(archiveLedger.findByIdempotencyKey(
				replaySource.archive().idempotencyKey()))
				.thenReturn(Optional.of(replaySource));
	}

	@Test
	@DisplayName("Явно подтвержденное пустое FAILED поколение проходит все фазы очистки")
	void failedGenerationWithNoWritesCompletesFencedCleanup() {
		CleanupCandidateSnapshot candidate = failedCandidate();
		when(cleanupLedger.findCandidates(
				PARTITION_KEY,
				Duration.ofHours(24),
				Duration.ofDays(7)))
				.thenReturn(List.of(candidate));
		stubDeleteSequence(candidate);
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);
		CleanupOperation pending = operation(
				plan,
				IndexMaintenancePhase.CLEANUP_PENDING,
				1,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation pendingTwo = operation(
				plan,
				IndexMaintenancePhase.CLEANUP_PENDING,
				2,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation pendingThree = operation(
				plan,
				IndexMaintenancePhase.CLEANUP_PENDING,
				3,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation pendingFour = operation(
				plan,
				IndexMaintenancePhase.CLEANUP_PENDING,
				4,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation requested = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation requestedSix = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				6,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation requestedSeven = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				7,
				NOW.plus(Duration.ofMinutes(15)));
		CleanupOperation requestedEight = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				8,
				NOW.plus(Duration.ofMinutes(15)));
		when(cleanupLedger.findRecoverable(PARTITION_KEY))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.of(requested));
		when(cleanupLedger.claim(any(CleanupClaim.class), any(Duration.class)))
				.thenReturn(Optional.of(pending));
		when(cleanupLedger.renew(any(CleanupOwnership.class), any(Duration.class)))
				.thenReturn(Optional.of(pendingTwo))
				.thenReturn(Optional.of(pendingThree))
				.thenReturn(Optional.of(pendingFour))
				.thenReturn(Optional.of(requestedSix))
				.thenReturn(Optional.of(requestedSeven))
				.thenReturn(Optional.of(requestedEight));
		when(cleanupLedger.requestDelete(
				any(CleanupOwnership.class),
				any(CleanupDeleteEvidence.class)))
				.thenReturn(CleanupTransitionResult.APPLIED);
		when(cleanupLedger.complete(
				any(CleanupOwnership.class),
				any(CleanupCompletionEvidence.class)))
				.thenReturn(CleanupTransitionResult.APPLIED);

		var result = service.execute(new GenerationCleanupCommand(
				plan,
				ACTOR,
				REASON_CODE));

		assertThat(result.outcome()).isEqualTo(GenerationCleanupOutcome.COMPLETED);
		assertThat(result.phase()).isEqualTo(IndexMaintenancePhase.COMPLETED);
		assertThat(result.operationToken()).isEqualTo(OPERATION_TOKEN);
		ArgumentCaptor<CleanupClaim> claim = ArgumentCaptor.forClass(CleanupClaim.class);
		verify(cleanupLedger).claim(claim.capture(), any(Duration.class));
		assertThat(claim.getValue().candidate().status())
				.isEqualTo(IndexGenerationStatus.FAILED);
		assertThat(claim.getValue().candidate().buildWriteOutcome())
				.isEqualTo(CleanupBuildWriteOutcome.NONE);
		verify(cleanupLedger).requestDelete(
				any(CleanupOwnership.class),
				any(CleanupDeleteEvidence.class));
		verify(elasticsearch).deleteExactIndex(new ExactIndexTarget(
				GENERATION_NAMES.eventIndexName(), EVENT_UUID));
		verify(elasticsearch).deleteExactIndex(new ExactIndexTarget(
				GENERATION_NAMES.mentionIndexName(), MENTION_UUID));
		verify(cleanupLedger).complete(
				any(CleanupOwnership.class),
				any(CleanupCompletionEvidence.class));
	}

	@Test
	@DisplayName("Проверка возраста создает только план и не изменяет данные")
	void inspectUsesAgeThresholdsWithoutMutation() {
		CleanupCandidateSnapshot candidate = failedCandidate();
		when(cleanupLedger.findCandidates(
				PARTITION_KEY,
				Duration.ofHours(24),
				Duration.ofDays(7)))
				.thenReturn(List.of(candidate));
		stubPresentPair(candidate, Set.of(), Set.of());

		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertThat(plan.expiresAt()).isEqualTo(NOW.plus(Duration.ofMinutes(15)));
		assertThat(plan.fingerprint()).hasSize(64);
		verify(cleanupLedger).findCandidates(
				PARTITION_KEY,
				Duration.ofHours(24),
				Duration.ofDays(7));
		verify(cleanupLedger, never()).claim(any(), any());
		verify(cleanupLedger, never()).requestDelete(any(), any());
		verify(cleanupLedger, never()).complete(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Измененный fingerprint и истекший план отклоняются до захвата очистки")
	void rejectsTamperedAndExpiredPlansBeforeClaim() {
		CleanupCandidateSnapshot candidate = failedCandidate();
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(candidate));
		stubPresentPair(candidate, Set.of(), Set.of());
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);
		clearInvocations(cleanupLedger, lifecycleLedger, elasticsearch, archiveLedger, archiveStager);

		assertCleanupError(
				withFingerprint(plan, "f".repeat(64)),
				GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		assertCleanupError(
				withExpiresAt(plan, NOW.minusSeconds(1)),
				GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);

		verify(cleanupLedger, times(1)).findRecoverable(PARTITION_KEY);
		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Другой UUID на месте удаляемого индекса останавливает восстановление")
	void differentIndexUuidStopsDeleteRecovery() {
		GenerationCleanupPlan plan = signedFailedPlan();
		stubDeleteRecovery(plan);
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ExactIndexAliasMembership(
						GENERATION_NAMES.eventIndexName(),
						"replacement-event-uuid",
						Set.of())));

		assertCleanupError(
				plan,
				GenerationCleanupErrorCode.CLEANUP_EXACT_TARGET_CONFLICT);

		verify(elasticsearch, never()).deleteExactIndex(any());
		verify(cleanupLedger, never()).complete(any(), any());
	}

	@Test
	@DisplayName("Стабильный или дополнительный alias запрещает удаление поколения")
	void anyAliasMembershipStopsCleanup() {
		CleanupCandidateSnapshot candidate = failedCandidate();
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(candidate));
		stubPresentPair(
				candidate,
				Set.of("gdelt-events-read", "operator-hold"),
				Set.of("gdelt-mentions-read"));
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(plan, GenerationCleanupErrorCode.CLEANUP_ALIAS_CONFLICT);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("planMutations")
	@DisplayName("Изменившиеся alias или UUID делают подтвержденный план устаревшим")
	void observedTargetMutationInvalidatesPlan(
			String description,
			ExactIndexAliasMembership changedEvent
	) {
		CleanupCandidateSnapshot candidate = failedCandidate();
		ExactIndexAliasMembership originalEvent = new ExactIndexAliasMembership(
				GENERATION_NAMES.eventIndexName(),
				EVENT_UUID,
				Set.of());
		ExactIndexAliasMembership originalMention = new ExactIndexAliasMembership(
				GENERATION_NAMES.mentionIndexName(),
				MENTION_UUID,
				Set.of());
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(candidate));
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.eventIndexName()))
				.thenReturn(Optional.of(originalEvent))
				.thenReturn(Optional.of(changedEvent));
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(originalMention));
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(plan, GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("protectedActiveExactFaults")
	@DisplayName("Поврежденная точная пара текущего поколения сохраняет rollback-копию")
	void protectedActiveExactPairMustRemainHealthy(
			String description,
			Optional<ObservedIndex> observedEvent,
			Optional<ObservedIndex> observedMention
	) {
		CleanupProtectedActive active = configureHealthySuperseded();
		when(elasticsearch.observeExactIndex(active.names().eventIndexName()))
				.thenReturn(observedEvent);
		when(elasticsearch.observeExactIndex(active.names().mentionIndexName()))
				.thenReturn(observedMention);

		assertThatExceptionOfType(GenerationCleanupRejectedException.class)
				.isThrownBy(() -> service.inspect(PARTITION_KEY, GENERATION_UUID))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidStableAliasMemberships")
	@DisplayName("Неожиданный target стабильного alias сохраняет rollback-копию")
	void malformedStableAliasTargetStopsSupersededCleanup(
			String description,
			AliasMembership aliases
	) {
		configureHealthySuperseded();
		when(elasticsearch.readAliases()).thenReturn(aliases);

		assertThatExceptionOfType(GenerationCleanupRejectedException.class)
				.isThrownBy(() -> service.inspect(PARTITION_KEY, GENERATION_UUID))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Неизвестный результат удаления завершается по уже отсутствующей паре с тем же token")
	void absentPairCompletesDeleteRecoveryWithSameToken() {
		GenerationCleanupPlan plan = signedFailedPlan();
		stubDeleteRecovery(plan);
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.eventIndexName()))
				.thenReturn(Optional.empty());
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.mentionIndexName()))
				.thenReturn(Optional.empty());
		when(cleanupLedger.complete(any(), any()))
				.thenReturn(CleanupTransitionResult.APPLIED);

		var result = service.execute(new GenerationCleanupCommand(
				plan,
				ACTOR,
				REASON_CODE));

		assertThat(result.outcome()).isEqualTo(GenerationCleanupOutcome.COMPLETED);
		assertThat(result.operationToken()).isEqualTo(OPERATION_TOKEN);
		verify(cleanupLedger).resume(
				PARTITION_KEY,
				plan.fingerprint(),
				Duration.ofMinutes(15));
		verify(cleanupLedger, never()).claim(any(), any());
		verify(cleanupLedger, never()).requestDelete(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
		ArgumentCaptor<CleanupOwnership> ownership =
				ArgumentCaptor.forClass(CleanupOwnership.class);
		verify(cleanupLedger).complete(ownership.capture(), any());
		assertThat(ownership.getValue().operationToken()).isEqualTo(OPERATION_TOKEN);
	}

	@Test
	@DisplayName("После частичного удаления исчезает только оставшийся индекс с тем же token")
	void partialPairDeleteResumesWithSameToken() {
		GenerationCleanupPlan plan = signedFailedPlan();
		stubDeleteRecovery(plan);
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.eventIndexName()))
				.thenReturn(Optional.empty());
		when(elasticsearch.observeAllAliasesForExactIndex(
				GENERATION_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ExactIndexAliasMembership(
						GENERATION_NAMES.mentionIndexName(),
						MENTION_UUID,
						Set.of())))
				.thenReturn(Optional.empty());
		when(cleanupLedger.complete(any(), any()))
				.thenReturn(CleanupTransitionResult.APPLIED);

		var result = service.execute(new GenerationCleanupCommand(
				plan,
				ACTOR,
				REASON_CODE));

		assertThat(result.outcome()).isEqualTo(GenerationCleanupOutcome.COMPLETED);
		assertThat(result.operationToken()).isEqualTo(OPERATION_TOKEN);
		verify(elasticsearch, never()).deleteExactIndex(new ExactIndexTarget(
				GENERATION_NAMES.eventIndexName(), EVENT_UUID));
		verify(elasticsearch).deleteExactIndex(new ExactIndexTarget(
				GENERATION_NAMES.mentionIndexName(), MENTION_UUID));
		ArgumentCaptor<CleanupOwnership> ownership =
				ArgumentCaptor.forClass(CleanupOwnership.class);
		verify(cleanupLedger).complete(ownership.capture(), any());
		assertThat(ownership.getValue().operationToken()).isEqualTo(OPERATION_TOKEN);
	}

	@Test
	@DisplayName("Потеря владения возвращается как результат без удаления индексов")
	void ownershipLossStopsBeforeDelete() {
		GenerationCleanupPlan plan = signedFailedPlan();
		CleanupOperation expired = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				4,
				NOW.minusSeconds(1));
		CleanupOperation resumed = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		when(cleanupLedger.findRecoverable(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(cleanupLedger.resume(
				PARTITION_KEY,
				plan.fingerprint(),
				Duration.ofMinutes(15)))
				.thenReturn(Optional.of(resumed));
		when(cleanupLedger.renew(any(), any())).thenReturn(Optional.empty());

		var result = service.execute(new GenerationCleanupCommand(
				plan,
				ACTOR,
				REASON_CODE));

		assertThat(result.outcome()).isEqualTo(GenerationCleanupOutcome.OWNERSHIP_LOST);
		assertThat(result.phase()).isEqualTo(IndexMaintenancePhase.DELETE_REQUESTED);
		assertThat(result.operationToken()).isEqualTo(OPERATION_TOKEN);
		verifyNoInteractions(elasticsearch);
		verify(cleanupLedger, never()).complete(any(), any());
	}

	@Test
	@DisplayName("Возобновившийся владелец осиротевшей сборки делает план устаревшим")
	void resumedOrphanOwnerInvalidatesPlan() {
		CleanupCandidateSnapshot orphan = orphanCandidate(
				CleanupBuildWriteOutcome.NONE,
				EVENT_UUID,
				MENTION_UUID);
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(orphan))
				.thenReturn(List.of());
		stubPresentPair(orphan, Set.of(), Set.of());
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(plan, GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Частичная запись осиротевшей сборки требует отдельного согласования")
	void partialOrphanWriteRequiresReconciliation() {
		CleanupCandidateSnapshot orphan = orphanCandidate(
				CleanupBuildWriteOutcome.PARTIAL,
				EVENT_UUID,
				MENTION_UUID);
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(orphan));
		stubPresentPair(orphan, Set.of(), Set.of());
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(
				plan,
				GenerationCleanupErrorCode.CLEANUP_RECONCILIATION_REQUIRED);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Alias осиротевшей сборки требует восстановления lifecycle до удаления")
	void orphanAliasRequiresReconciliation() {
		CleanupCandidateSnapshot orphan = orphanCandidate(
				CleanupBuildWriteOutcome.NONE,
				EVENT_UUID,
				MENTION_UUID);
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(orphan));
		stubPresentPair(orphan, Set.of("operator-hold"), Set.of());
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(
				plan,
				GenerationCleanupErrorCode.CLEANUP_RECONCILIATION_REQUIRED);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("orphanUnsafePhases")
	@DisplayName("Остановленная после начала работы сборка требует согласования lifecycle")
	void orphanAfterWorkStartedRequiresReconciliation(
			String description,
			IndexMaintenancePhase ownerPhase
	) {
		CleanupCandidateSnapshot orphan = orphanCandidate(
				CleanupBuildWriteOutcome.NONE,
				EVENT_UUID,
				MENTION_UUID,
				ownerPhase);
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(orphan));
		stubPresentPair(orphan, Set.of(), Set.of());
		GenerationCleanupPlan plan = service.inspect(PARTITION_KEY, GENERATION_UUID);

		assertCleanupError(
				plan,
				GenerationCleanupErrorCode.CLEANUP_RECONCILIATION_REQUIRED);

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	@Test
	@DisplayName("Старое поколение нельзя очистить без здоровых receipt текущего поколения")
	void supersededCleanupRequiresHealthyCurrentReceipts() {
		CleanupProtectedActive active = protectedActive();
		CleanupCandidateSnapshot superseded = supersededCandidate(active);
		when(lifecycleLedger.findPartition(PARTITION_KEY))
				.thenReturn(Optional.of(partition(active.generationId())));
		when(lifecycleLedger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of(activeGeneration(active)));
		when(elasticsearch.readAliases()).thenReturn(new AliasMembership(
				Set.of(active.names().eventIndexName()),
				Set.of(active.names().mentionIndexName())));
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(superseded));
		when(processingLedger.findByPartition(PARTITION_KEY)).thenReturn(List.of());
		stubPresentPair(superseded, Set.of(), Set.of());

		assertThatExceptionOfType(GenerationCleanupRejectedException.class)
				.isThrownBy(() -> service.inspect(PARTITION_KEY, GENERATION_UUID))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));

		verify(cleanupLedger, never()).claim(any(), any());
		verify(elasticsearch, never()).deleteExactIndex(any());
	}

	private static Stream<Arguments> planMutations() {
		return Stream.of(
				Arguments.of(
						"после inspect появился alias",
						new ExactIndexAliasMembership(
								GENERATION_NAMES.eventIndexName(),
								EVENT_UUID,
								Set.of("operator-hold"))),
				Arguments.of(
						"после inspect изменился UUID",
						new ExactIndexAliasMembership(
								GENERATION_NAMES.eventIndexName(),
								"replacement-event-uuid",
								Set.of())));
	}

	private static Stream<Arguments> protectedActiveExactFaults() {
		CleanupProtectedActive active = protectedActive();
		ObservedIndex event = new ObservedIndex(
				active.names().eventIndexName(), active.eventIndexUuid(), false);
		ObservedIndex mention = new ObservedIndex(
				active.names().mentionIndexName(), active.mentionIndexUuid(), false);
		return Stream.of(
				Arguments.of(
						"отсутствует Event index",
						Optional.<ObservedIndex>empty(),
						Optional.of(mention)),
				Arguments.of(
						"отсутствует Mention index",
						Optional.of(event),
						Optional.<ObservedIndex>empty()),
				Arguments.of(
						"заменен UUID Event index",
						Optional.of(new ObservedIndex(
								active.names().eventIndexName(),
								"replacement-event-uuid",
								false)),
						Optional.of(mention)),
				Arguments.of(
						"заменен UUID Mention index",
						Optional.of(event),
						Optional.of(new ObservedIndex(
								active.names().mentionIndexName(),
								"replacement-mention-uuid",
								false))),
				Arguments.of(
						"Event index заблокирован для записи",
						Optional.of(new ObservedIndex(
								active.names().eventIndexName(),
								active.eventIndexUuid(),
								true)),
						Optional.of(mention)),
				Arguments.of(
						"Mention index заблокирован для записи",
						Optional.of(event),
						Optional.of(new ObservedIndex(
								active.names().mentionIndexName(),
								active.mentionIndexUuid(),
								true))));
	}

	private static Stream<Arguments> invalidStableAliasMemberships() {
		CleanupProtectedActive active = protectedActive();
		return Stream.of(
				Arguments.of(
						"alias содержит еще одно поколение partition",
						new AliasMembership(
								Set.of(
										active.names().eventIndexName(),
										"gdelt-events-v1-p20260727-g9999"),
								Set.of(active.names().mentionIndexName()))),
				Arguments.of(
						"alias указывает на некорректное physical name",
						new AliasMembership(
								Set.of(active.names().eventIndexName()),
								Set.of("gdelt-mentions-v1-p20260727-ginvalid"))));
	}

	private static Stream<Arguments> orphanUnsafePhases() {
		return Stream.of(
				Arguments.of(
						"owner остановился после установки блокировки",
						IndexMaintenancePhase.FROZEN),
				Arguments.of(
						"owner остановился во время записи",
						IndexMaintenancePhase.BUILDING));
	}

	private CleanupProtectedActive configureHealthySuperseded() {
		CleanupProtectedActive active = protectedActive();
		CleanupCandidateSnapshot superseded = supersededCandidate(active);
		when(lifecycleLedger.findPartition(PARTITION_KEY))
				.thenReturn(Optional.of(partition(active.generationId())));
		when(lifecycleLedger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of(activeGeneration(active)));
		when(cleanupLedger.findCandidates(any(), any(), any()))
				.thenReturn(List.of(superseded));
		stubPresentPair(superseded, Set.of(), Set.of());
		when(elasticsearch.readAliases()).thenReturn(new AliasMembership(
				Set.of(active.names().eventIndexName()),
				Set.of(active.names().mentionIndexName())));
		when(elasticsearch.observeExactIndex(active.names().eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						active.names().eventIndexName(),
						active.eventIndexUuid(),
						false)));
		when(elasticsearch.observeExactIndex(active.names().mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						active.names().mentionIndexName(),
						active.mentionIndexUuid(),
						false)));
		when(processingLedger.findByPartition(PARTITION_KEY))
				.thenReturn(List.of(indexedProcessingState(active)));
		when(elasticsearch.verifyReceipt(any())).thenReturn(matchedReceipt());
		return active;
	}

	private ArchiveProcessingState indexedProcessingState(CleanupProtectedActive active) {
		String archiveKey = replaySource.archive().idempotencyKey();
		String digest = "c".repeat(64);
		ArchiveProcessingTargetBinding binding = new ArchiveProcessingTargetBinding(
				GdeltIndexKind.EVENT,
				PARTITION_KEY,
				4,
				active.generationId(),
				active.generationUuid(),
				active.names().eventIndexName(),
				active.eventIndexUuid());
		return new ArchiveProcessingState(
				archiveKey,
				new ArchiveProcessingFingerprint(
						"source-fingerprint",
						"projection-v1",
						"b".repeat(64)),
				ArchiveProcessingStatus.INDEXED,
				4,
				new ArchiveProcessingAttemptState(
						1,
						null,
						NOW.minus(Duration.ofHours(1)),
						null,
						AutomaticRetryState.initial(3)),
				binding,
				new ArchiveProcessingProgress(10, 0, 0, 10, 10, 0, 10, null),
				new ArchiveProcessingReceipt(
						10,
						10,
						ArchiveIdentityDigest.ALGORITHM,
						digest,
						digest,
						active.generationId(),
						active.eventIndexUuid(),
						NOW.minus(Duration.ofMinutes(30))),
				null,
				PARTITION_START,
				NOW.minus(Duration.ofMinutes(30)));
	}

	private static ArchiveReceiptVerification matchedReceipt() {
		ArchiveIdentityDigest digest = new ArchiveIdentityDigest("c".repeat(64));
		return new ArchiveReceiptVerification(
				GdeltIndexKind.EVENT,
				10,
				10,
				digest,
				digest,
				ArchiveReceiptStatus.MATCHED);
	}

	private void assertCleanupError(
			GenerationCleanupPlan plan,
			GenerationCleanupErrorCode expected
	) {
		assertThatExceptionOfType(GenerationCleanupRejectedException.class)
				.isThrownBy(() -> service.execute(new GenerationCleanupCommand(
						plan,
						ACTOR,
						REASON_CODE)))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(expected));
	}

	private void stubPresentPair(
			CleanupCandidateSnapshot candidate,
			Set<String> eventAliases,
			Set<String> mentionAliases
	) {
		when(elasticsearch.observeAllAliasesForExactIndex(
				candidate.names().eventIndexName()))
				.thenReturn(Optional.of(new ExactIndexAliasMembership(
						candidate.names().eventIndexName(),
						candidate.eventIndexUuid(),
						eventAliases)));
		when(elasticsearch.observeAllAliasesForExactIndex(
				candidate.names().mentionIndexName()))
				.thenReturn(Optional.of(new ExactIndexAliasMembership(
						candidate.names().mentionIndexName(),
						candidate.mentionIndexUuid(),
						mentionAliases)));
	}

	private void stubDeleteSequence(CleanupCandidateSnapshot candidate) {
		ExactIndexAliasMembership event = new ExactIndexAliasMembership(
				candidate.names().eventIndexName(),
				candidate.eventIndexUuid(),
				Set.of());
		ExactIndexAliasMembership mention = new ExactIndexAliasMembership(
				candidate.names().mentionIndexName(),
				candidate.mentionIndexUuid(),
				Set.of());
		when(elasticsearch.observeAllAliasesForExactIndex(
				candidate.names().eventIndexName()))
				.thenReturn(Optional.of(event))
				.thenReturn(Optional.of(event))
				.thenReturn(Optional.of(event))
				.thenReturn(Optional.of(event))
				.thenReturn(Optional.empty());
		when(elasticsearch.observeAllAliasesForExactIndex(
				candidate.names().mentionIndexName()))
				.thenReturn(Optional.of(mention))
				.thenReturn(Optional.of(mention))
				.thenReturn(Optional.of(mention))
				.thenReturn(Optional.of(mention))
				.thenReturn(Optional.empty());
	}

	private void stubDeleteRecovery(GenerationCleanupPlan plan) {
		CleanupOperation expired = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				4,
				NOW.minusSeconds(1));
		CleanupOperation resumed = operation(
				plan,
				IndexMaintenancePhase.DELETE_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		when(cleanupLedger.findRecoverable(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(cleanupLedger.resume(
				PARTITION_KEY,
				plan.fingerprint(),
				Duration.ofMinutes(15)))
				.thenReturn(Optional.of(resumed));
		when(cleanupLedger.renew(any(), any())).thenReturn(Optional.of(resumed));
	}

	private static CleanupCandidateSnapshot failedCandidate() {
		return new CleanupCandidateSnapshot(
				PARTITION_KEY,
				4,
				false,
				10,
				GENERATION_UUID,
				2,
				IndexGenerationStatus.FAILED,
				3,
				GENERATION_NAMES,
				EVENT_UUID,
				MENTION_UUID,
				"BUILD_FAILED",
				NOW.minus(Duration.ofDays(2)),
				null,
				NOW.minus(Duration.ofDays(2)),
				null,
				CleanupBuildWriteOutcome.NONE,
				false,
				null);
	}

	private static CleanupCandidateSnapshot orphanCandidate(
			CleanupBuildWriteOutcome writeOutcome,
			String eventUuid,
			String mentionUuid
	) {
		return orphanCandidate(
				writeOutcome,
				eventUuid,
				mentionUuid,
				IndexMaintenancePhase.PLANNED);
	}

	private static CleanupCandidateSnapshot orphanCandidate(
			CleanupBuildWriteOutcome writeOutcome,
			String eventUuid,
			String mentionUuid,
			IndexMaintenancePhase ownerPhase
	) {
		return new CleanupCandidateSnapshot(
				PARTITION_KEY,
				4,
				false,
				10,
				GENERATION_UUID,
				2,
				IndexGenerationStatus.BUILDING,
				3,
				GENERATION_NAMES,
				eventUuid,
				mentionUuid,
				null,
				NOW.minus(Duration.ofDays(2)),
				null,
				null,
				new CleanupOrphanOwner(
						20,
						IndexMaintenanceType.REBUILD,
						ownerPhase,
						UUID.fromString("33333333-3333-3333-3333-333333333333"),
						2,
						NOW.minusSeconds(1),
						NOW.minus(Duration.ofDays(2))),
				writeOutcome,
				false,
				null);
	}

	private static CleanupCandidateSnapshot supersededCandidate(
			CleanupProtectedActive active
	) {
		return new CleanupCandidateSnapshot(
				PARTITION_KEY,
				4,
				false,
				10,
				GENERATION_UUID,
				2,
				IndexGenerationStatus.SUPERSEDED,
				3,
				GENERATION_NAMES,
				EVENT_UUID,
				MENTION_UUID,
				null,
				NOW.minus(Duration.ofDays(10)),
				NOW.minus(Duration.ofDays(8)),
				null,
				null,
				CleanupBuildWriteOutcome.COMPLETED,
				false,
				active);
	}

	private static CleanupProtectedActive protectedActive() {
		return new CleanupProtectedActive(
				20,
				UUID.fromString("44444444-4444-4444-4444-444444444444"),
				7,
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0003",
						"gdelt-mentions-v1-p20260727-g0003"),
				"active-event-uuid",
				"active-mention-uuid");
	}

	private static IndexGeneration activeGeneration(CleanupProtectedActive active) {
		return new IndexGeneration(
				active.generationId(),
				active.generationUuid(),
				PARTITION_KEY,
				3,
				IndexGenerationStatus.ACTIVE,
				active.names(),
				active.eventIndexUuid(),
				active.mentionIndexUuid(),
				active.generationVersion());
	}

	private static IndexPartition partition(Long activeGenerationId) {
		return new IndexPartition(
				new IndexPartitionDefinition(
						PARTITION_KEY,
						PARTITION_START,
						PARTITION_END,
						Period.ofDays(7)),
				4,
				activeGenerationId);
	}

	private static IngestionArchiveState replaySource() {
		String md5 = "a".repeat(32);
		DiscoveredArchive archive = new DiscoveredArchive(
				Instant.parse("2026-07-27T00:00:00Z"),
				"20260727000000.translation.export.CSV.zip",
				URI.create("http://data.gdeltproject.org/gdeltv2/"
						+ "20260727000000.translation.export.CSV.zip"),
				md5,
				ArchiveType.TRANSLATION_EVENTS,
				100);
		return new IngestionArchiveState(
				1,
				archive,
				IngestionArchiveStatus.STAGED,
				new ArchiveAttemptState(
						0,
						null,
						null,
						null,
						AutomaticRetryState.initial(3)),
				new StagedArchive(
						Path.of("staging/archive.zip"),
						Path.of("staging/archive.csv"),
						100,
						md5),
				null,
				PARTITION_START,
				PARTITION_START);
	}

	private static GenerationCleanupPlan signedFailedPlan() {
		ReplaySourcePlan source = new ReplaySourcePlan(
				replaySource().archive().idempotencyKey(),
				PARTITION_START,
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				"staging/archive.zip",
				"staging/archive.csv");
		GenerationCleanupPlan material = new GenerationCleanupPlan(
				PARTITION_KEY,
				PARTITION_START,
				PARTITION_END,
				4,
				10,
				GENERATION_UUID,
				2,
				IndexGenerationStatus.FAILED,
				3,
				GENERATION_NAMES,
				EVENT_UUID,
				MENTION_UUID,
				"BUILD_FAILED",
				GenerationWriteOutcome.NONE,
				false,
				false,
				NOW.minus(Duration.ofDays(2)),
				NOW.minus(Duration.ofDays(2)),
				null,
				EVENT_UUID,
				Set.of(),
				MENTION_UUID,
				Set.of(),
				0,
				null,
				List.of(source),
				List.of(),
				NOW.plus(Duration.ofMinutes(15)),
				"0".repeat(64));
		return withFingerprint(
				material,
				GenerationCleanupFingerprint.calculate(material));
	}

	private static CleanupOperation operation(
			GenerationCleanupPlan plan,
			IndexMaintenancePhase phase,
			long version,
			Instant leaseExpiresAt
	) {
		return new CleanupOperation(
				30,
				PARTITION_KEY,
				phase,
				OPERATION_TOKEN,
				version,
				leaseExpiresAt,
				plan.partitionStateVersion(),
				plan.generationId(),
				plan.generationStateVersion() + 1,
				null,
				null,
				plan.fingerprint(),
				plan.expiresAt(),
				ACTOR,
				REASON_CODE);
	}

	private static GenerationCleanupPlan withExpiresAt(
			GenerationCleanupPlan plan,
			Instant expiresAt
	) {
		GenerationCleanupPlan material = copy(plan, expiresAt, "0".repeat(64));
		return withFingerprint(
				material,
				GenerationCleanupFingerprint.calculate(material));
	}

	private static GenerationCleanupPlan withFingerprint(
			GenerationCleanupPlan plan,
			String fingerprint
	) {
		return copy(plan, plan.expiresAt(), fingerprint);
	}

	private static GenerationCleanupPlan copy(
			GenerationCleanupPlan plan,
			Instant expiresAt,
			String fingerprint
	) {
		return new GenerationCleanupPlan(
				plan.partitionKey(),
				plan.partitionStartAt(),
				plan.partitionEndAt(),
				plan.partitionStateVersion(),
				plan.generationId(),
				plan.generationUuid(),
				plan.generationNumber(),
				plan.generationStatus(),
				plan.generationStateVersion(),
				plan.generationNames(),
				plan.expectedEventIndexUuid(),
				plan.expectedMentionIndexUuid(),
				plan.failureOrigin(),
				plan.writeOutcome(),
				plan.repairOpen(),
				plan.activeProcessing(),
				plan.generationHeartbeatAt(),
				plan.candidateSinceAt(),
				plan.ownerEvidence(),
				plan.observedEventIndexUuid(),
				plan.eventAliases(),
				plan.observedMentionIndexUuid(),
				plan.mentionAliases(),
				plan.observedStoreBytes(),
				plan.protectedActive(),
				plan.replaySources(),
				plan.currentReceipts(),
				expiresAt,
				fingerprint);
	}

	private static BackendDataProperties properties() {
		return new BackendDataProperties(
				Duration.ofDays(7),
				new BackendDataProperties.Retry(
						Duration.ofMinutes(1),
						2,
						Duration.ofMinutes(15),
						0,
						3),
				Duration.ofMinutes(12),
				500,
				new BackendDataProperties.DiskPressure(1, 1),
				new BackendDataProperties.Rebuild(
						Duration.ofMinutes(15),
						Duration.ofMinutes(15)),
				new BackendDataProperties.Cleanup(
						Duration.ofHours(24),
						Duration.ofDays(7),
						Duration.ofMinutes(15),
						Duration.ofMinutes(15),
						false));
	}
}
