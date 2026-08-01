package com.neighbor.eventmosaic.ingestion.maintenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.BaseGenerationDisposition;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ObservedIndex;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolver;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildErrorCode;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildException;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.ArchivePlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildCommand;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildOutcome;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildPlan;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Восстановление partition rebuild после неопределенного результата")
class DefaultPartitionRebuildServiceFaultTest {

	private static final String PARTITION_KEY = "p20260727";
	private static final IndexGenerationNames BASE_NAMES = new IndexGenerationNames(
			"gdelt-events-v1-p20260727-g0001",
			"gdelt-mentions-v1-p20260727-g0001");
	private static final IndexGenerationNames TARGET_NAMES = new IndexGenerationNames(
			"gdelt-events-v1-p20260727-g0002",
			"gdelt-mentions-v1-p20260727-g0002");
	private static final Instant NOW = FixedClockTestConfiguration.NOW;

	private final IndexLifecycleLedger lifecycleLedger = mock(IndexLifecycleLedger.class);
	private final IndexPartitionGenerationResolver generationResolver =
			mock(IndexPartitionGenerationResolver.class);
	private final IndexMaintenanceGateway elasticsearch = mock(IndexMaintenanceGateway.class);
	private final IngestionArchiveLedger archiveLedger = mock(IngestionArchiveLedger.class);
	private final ArchiveProcessingLedger processingLedger = mock(ArchiveProcessingLedger.class);
	private final ZipArchiveStager archiveStager = mock(ZipArchiveStager.class);
	private final GdeltArchiveProcessor archiveProcessor = mock(GdeltArchiveProcessor.class);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
	private final DefaultPartitionRebuildService service = new DefaultPartitionRebuildService(
			lifecycleLedger,
			generationResolver,
			elasticsearch,
			archiveLedger,
			processingLedger,
			archiveStager,
			archiveProcessor,
			properties(),
			clock);

	private PartitionRebuildPlan plan;
	private IndexGeneration base;
	private IndexGeneration target;

	@BeforeEach
	void setUpPlan() {
		plan = signedPlan();
		base = generation(
				1,
				IndexGenerationStatus.ACTIVE,
				BASE_NAMES,
				"old-event-uuid",
				"old-mention-uuid");
		target = generation(
				2,
				IndexGenerationStatus.BUILDING,
				TARGET_NAMES,
				"new-event-uuid",
				"new-mention-uuid");
		when(lifecycleLedger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of(base, target));
		when(lifecycleLedger.recordBuildWriteOutcome(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(CleanupBuildWriteOutcome.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
	}

	@Test
	@DisplayName("Измененный fingerprint и истекший plan отклоняются до durable claim")
	void rejectsTamperedAndExpiredPlansBeforeClaim() {
		PartitionRebuildPlan tampered = withFingerprint(plan, "f".repeat(64));

		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(new PartitionRebuildCommand(
						tampered,
						"local.operator",
						"INDEX_RECEIPT_SURPLUS")))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.STALE_REBUILD_PLAN));
		verify(lifecycleLedger, never()).findRecoverableOperation(anyString());

		PartitionRebuildPlan expired = withExpiresAt(plan, NOW.minusSeconds(1));
		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(new PartitionRebuildCommand(
						expired,
						"local.operator",
						"INDEX_RECEIPT_SURPLUS")))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.STALE_REBUILD_PLAN));
		verify(lifecycleLedger, never()).startRebuild(any(), any(Duration.class));
	}

	@Test
	@DisplayName("При ошибке перестроения прежние данные открываются для записи только после снятия блокировки")
	void failedBuildUnfreezesBaseBeforeClosingOperation() {
		IndexMaintenanceOperation expired = operation(
				IndexMaintenancePhase.BUILDING,
				4,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation reclaimed = operation(
				IndexMaintenancePhase.BUILDING,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation renewed = operation(
				IndexMaintenancePhase.BUILDING,
				6,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(reclaimed));
		when(lifecycleLedger.renewMaintenanceLease(
				anyString(), any(UUID.class), anyLong(), anyLong(), any(Duration.class)))
				.thenReturn(Optional.of(renewed));
		when(lifecycleLedger.advancePhase(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(lifecycleLedger.completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString()))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(elasticsearch.observeExactIndex(TARGET_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						TARGET_NAMES.eventIndexName(), "new-event-uuid", false)));
		when(elasticsearch.observeExactIndex(TARGET_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						TARGET_NAMES.mentionIndexName(), "new-mention-uuid", false)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.eventIndexName(), "old-event-uuid", false)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.mentionIndexName(), "old-mention-uuid", false)));
		when(archiveProcessor.process(any(), any()))
				.thenReturn(ArchiveProcessingResult.failed(
						GdeltArchiveKind.TRANSLATION_EVENTS,
						ArchiveProcessingProgress.empty(),
						new ArchiveProcessingFailure(
								ArchiveProcessingErrorCode.INDEXING_OPERATION_FAILURE,
								true,
								null)));

		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(new PartitionRebuildCommand(
						plan,
						"local.operator",
						"INDEX_RECEIPT_SURPLUS")))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.REBUILD_PROCESSING_FAILED));

		verify(elasticsearch).removeWriteBlock(anyList());
		verify(elasticsearch, never()).cutoverAliases(any());
		verify(elasticsearch, never()).readAliases();
		verify(lifecycleLedger).completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString());
	}

	@Test
	@DisplayName("Неожиданное поколение в читаемом alias останавливает восстановление до переключения")
	void unexpectedRepairTargetStopsBeforeAliasMutation() {
		IndexMaintenanceOperation expired = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				4,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation reclaimed = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(reclaimed));
		when(elasticsearch.readAliases()).thenReturn(new AliasMembership(
				Set.of("gdelt-events-v1-p20260727-g0003"),
				Set.of(BASE_NAMES.mentionIndexName())));

		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(new PartitionRebuildCommand(
						plan,
						"local.operator",
						"INDEX_RECEIPT_SURPLUS")))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT));

		verify(elasticsearch, never()).cutoverAliases(any());
		verify(elasticsearch, never()).addStableAliases(
				anyString(), anyBoolean(), anyString(), anyBoolean());
		verify(lifecycleLedger, never()).completeObservedCutover(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(BaseGenerationDisposition.class),
				anyList());
	}

	@Test
	@DisplayName("Тот же владелец дополняет частично переключенную пару читаемых aliases")
	void partialRepairAddCompletesUnderSameToken() {
		IndexMaintenanceOperation expired = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				4,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation reclaimed = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation renewedBeforeAdd = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				6,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation renewedBeforeCompletion = operation(
				IndexMaintenancePhase.CUTOVER_OBSERVED,
				8,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(reclaimed));
		when(lifecycleLedger.renewMaintenanceLease(
				anyString(), any(UUID.class), anyLong(), anyLong(), any(Duration.class)))
				.thenReturn(Optional.of(renewedBeforeAdd))
				.thenReturn(Optional.of(renewedBeforeCompletion));
		when(lifecycleLedger.advancePhase(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(lifecycleLedger.completeObservedCutover(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(BaseGenerationDisposition.class),
				anyList()))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		AliasMembership completeTargetPair = new AliasMembership(
				Set.of(TARGET_NAMES.eventIndexName()),
				Set.of(TARGET_NAMES.mentionIndexName()));
		when(elasticsearch.readAliases())
				.thenReturn(new AliasMembership(
						Set.of(TARGET_NAMES.eventIndexName()),
						Set.of()))
				.thenReturn(completeTargetPair)
				.thenReturn(completeTargetPair);
		when(elasticsearch.observeExactIndex(BASE_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.eventIndexName(), "old-event-uuid", true)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.mentionIndexName(), "old-mention-uuid", true)));
		when(elasticsearch.verifyReceipt(any())).thenReturn(matchedReceipt());

		var result = service.execute(new PartitionRebuildCommand(
				plan,
				"local.operator",
				"INDEX_RECEIPT_SURPLUS"));

		assertThat(result.outcome()).isEqualTo(PartitionRebuildOutcome.COMPLETED);
		assertThat(result.operationToken()).isEqualTo(reclaimed.token());
		verify(elasticsearch).addStableAliases(
				TARGET_NAMES.eventIndexName(),
				false,
				TARGET_NAMES.mentionIndexName(),
				true);
		verify(elasticsearch, never()).cutoverAliases(any());
	}

	@Test
	@DisplayName("Unknown cutover завершается по фактической новой alias паре без второго switch")
	void reconcilesUnknownCutoverFromObservedAliases() {
		IndexMaintenanceOperation expired = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				4,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation reclaimed = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation renewedBeforeCutover = operation(
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				6,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation renewedBeforeCompletion = operation(
				IndexMaintenancePhase.CUTOVER_OBSERVED,
				8,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(reclaimed));
		when(lifecycleLedger.renewMaintenanceLease(
				anyString(), any(UUID.class), anyLong(), anyLong(), any(Duration.class)))
				.thenReturn(Optional.of(renewedBeforeCutover))
				.thenReturn(Optional.of(renewedBeforeCompletion));
		when(lifecycleLedger.advancePhase(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(lifecycleLedger.completeObservedCutover(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(BaseGenerationDisposition.class),
				anyList()))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(elasticsearch.readAliases())
				.thenReturn(new AliasMembership(
						Set.of(BASE_NAMES.eventIndexName()),
						Set.of(BASE_NAMES.mentionIndexName())))
				.thenReturn(new AliasMembership(
						Set.of(TARGET_NAMES.eventIndexName()),
						Set.of(TARGET_NAMES.mentionIndexName())));
		doThrow(new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE))
				.when(elasticsearch).cutoverAliases(any());
		when(elasticsearch.observeExactIndex(BASE_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.eventIndexName(), "old-event-uuid", true)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.mentionIndexName(), "old-mention-uuid", true)));
		when(elasticsearch.verifyReceipt(any())).thenReturn(matchedReceipt());

		var result = service.execute(new PartitionRebuildCommand(
				plan,
				"local.operator",
				"INDEX_RECEIPT_SURPLUS"));

		assertThat(result.outcome()).isEqualTo(PartitionRebuildOutcome.COMPLETED);
		verify(elasticsearch, times(1)).cutoverAliases(any());
		verify(lifecycleLedger).completeObservedCutover(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(BaseGenerationDisposition.class),
				anyList());
	}

	@Test
	@DisplayName("Unknown freeze повторяет dedicated add-block и не освобождает claims")
	void repeatsDedicatedFreezeAfterUnknownOutcome() {
		IndexMaintenanceOperation firstExpired = operation(
				IndexMaintenancePhase.FREEZE_REQUESTED,
				2,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation firstReclaimed = operation(
				IndexMaintenancePhase.FREEZE_REQUESTED,
				3,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation secondExpired = operation(
				IndexMaintenancePhase.FREEZE_REQUESTED,
				4,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation secondReclaimed = operation(
				IndexMaintenancePhase.FREEZE_REQUESTED,
				5,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(firstExpired))
				.thenReturn(Optional.of(secondExpired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(firstReclaimed))
				.thenReturn(Optional.of(secondReclaimed));
		when(elasticsearch.observeExactIndex(BASE_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.eventIndexName(), "old-event-uuid", false)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.mentionIndexName(), "old-mention-uuid", false)));
		doThrow(new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE))
				.doNothing()
				.when(elasticsearch).addWriteBlock(anyList());
		when(lifecycleLedger.advancePhase(
				anyString(),
				any(UUID.class),
				anyLong(),
				anyLong(),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.OWNERSHIP_LOST);

		PartitionRebuildCommand command = new PartitionRebuildCommand(
				plan,
				"local.operator",
				"INDEX_RECEIPT_SURPLUS");
		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(command))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.REBUILD_OPERATION_UNAVAILABLE));
		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(command));

		verify(elasticsearch, times(2)).addWriteBlock(anyList());
		verify(elasticsearch, never()).removeWriteBlock(anyList());
		verify(lifecycleLedger, never()).completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString());
	}

	@Test
	@DisplayName("Unknown unfreeze сохраняет barrier до наблюдаемого снятия block")
	void keepsBarrierUntilUnknownUnfreezeIsReconciled() {
		IndexMaintenanceOperation firstExpired = operation(
				IndexMaintenancePhase.UNFREEZE_REQUESTED,
				6,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation firstReclaimed = operation(
				IndexMaintenancePhase.UNFREEZE_REQUESTED,
				7,
				NOW.plus(Duration.ofMinutes(15)));
		IndexMaintenanceOperation secondExpired = operation(
				IndexMaintenancePhase.UNFREEZE_REQUESTED,
				8,
				NOW.minusSeconds(1));
		IndexMaintenanceOperation secondReclaimed = operation(
				IndexMaintenancePhase.UNFREEZE_REQUESTED,
				9,
				NOW.plus(Duration.ofMinutes(15)));
		when(lifecycleLedger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(firstExpired))
				.thenReturn(Optional.of(secondExpired));
		when(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(firstReclaimed))
				.thenReturn(Optional.of(secondReclaimed));
		doThrow(new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE))
				.doNothing()
				.when(elasticsearch).removeWriteBlock(anyList());
		when(elasticsearch.observeExactIndex(BASE_NAMES.eventIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.eventIndexName(), "old-event-uuid", false)));
		when(elasticsearch.observeExactIndex(BASE_NAMES.mentionIndexName()))
				.thenReturn(Optional.of(new ObservedIndex(
						BASE_NAMES.mentionIndexName(), "old-mention-uuid", false)));
		when(lifecycleLedger.completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString()))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		PartitionRebuildCommand command = new PartitionRebuildCommand(
				plan,
				"local.operator",
				"INDEX_RECEIPT_SURPLUS");

		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(command))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.REBUILD_OPERATION_UNAVAILABLE));
		verify(lifecycleLedger, never()).completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString());

		assertThatExceptionOfType(PartitionRebuildException.class)
				.isThrownBy(() -> service.execute(command))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(PartitionRebuildErrorCode.REBUILD_PROCESSING_FAILED));
		verify(elasticsearch, times(2)).removeWriteBlock(anyList());
		verify(lifecycleLedger, times(1)).completePreCutoverFailure(
				anyString(), any(UUID.class), anyLong(), anyLong(), anyString());
	}

	private static PartitionRebuildPlan signedPlan() {
		ArchivePlan archive = new ArchivePlan(
				"archive-key",
				Instant.parse("2026-07-27T00:00:00Z"),
				GdeltArchiveKind.TRANSLATION_EVENTS,
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				"staging/archive.zip",
				"staging/archive.csv",
				"b".repeat(64),
				ArchiveProcessingStatus.FAILED,
				3,
				1,
				2,
				"c".repeat(64));
		PartitionRebuildPlan material = new PartitionRebuildPlan(
				PARTITION_KEY,
				Instant.parse("2026-07-27T00:00:00Z"),
				Instant.parse("2026-08-03T00:00:00Z"),
				IndexRepairCause.SURPLUS,
				2,
				1,
				UUID.fromString("11111111-1111-1111-1111-111111111111"),
				BASE_NAMES,
				"old-event-uuid",
				"old-mention-uuid",
				"old-event-uuid",
				false,
				"old-mention-uuid",
				false,
				Set.of(BASE_NAMES.eventIndexName()),
				Set.of(BASE_NAMES.mentionIndexName()),
				2,
				TARGET_NAMES,
				List.of(archive),
				100,
				200,
				NOW.plus(Duration.ofMinutes(15)),
				"0".repeat(64));
		return withFingerprint(material, PartitionRebuildFingerprint.calculate(material));
	}

	private static PartitionRebuildPlan withFingerprint(
			PartitionRebuildPlan plan,
			String fingerprint
	) {
		return new PartitionRebuildPlan(
				plan.partitionKey(),
				plan.partitionStartAt(),
				plan.partitionEndAt(),
				plan.repairCause(),
				plan.partitionStateVersion(),
				plan.baseGenerationId(),
				plan.baseGenerationUuid(),
				plan.baseNames(),
				plan.expectedEventIndexUuid(),
				plan.expectedMentionIndexUuid(),
				plan.observedEventIndexUuid(),
				plan.observedEventWriteBlocked(),
				plan.observedMentionIndexUuid(),
				plan.observedMentionWriteBlocked(),
				plan.eventAliasMembership(),
				plan.mentionAliasMembership(),
				plan.targetGenerationNumber(),
				plan.targetNames(),
				plan.archives(),
				plan.stagingAvailableDiskBytes(),
				plan.elasticsearchAvailableDiskBytes(),
				plan.expiresAt(),
				fingerprint);
	}

	private static PartitionRebuildPlan withExpiresAt(
			PartitionRebuildPlan plan,
			Instant expiresAt
	) {
		PartitionRebuildPlan material = new PartitionRebuildPlan(
				plan.partitionKey(),
				plan.partitionStartAt(),
				plan.partitionEndAt(),
				plan.repairCause(),
				plan.partitionStateVersion(),
				plan.baseGenerationId(),
				plan.baseGenerationUuid(),
				plan.baseNames(),
				plan.expectedEventIndexUuid(),
				plan.expectedMentionIndexUuid(),
				plan.observedEventIndexUuid(),
				plan.observedEventWriteBlocked(),
				plan.observedMentionIndexUuid(),
				plan.observedMentionWriteBlocked(),
				plan.eventAliasMembership(),
				plan.mentionAliasMembership(),
				plan.targetGenerationNumber(),
				plan.targetNames(),
				plan.archives(),
				plan.stagingAvailableDiskBytes(),
				plan.elasticsearchAvailableDiskBytes(),
				expiresAt,
				"0".repeat(64));
		return withFingerprint(material, PartitionRebuildFingerprint.calculate(material));
	}

	private static IndexMaintenanceOperation operation(
			IndexMaintenancePhase phase,
			long version,
			Instant leaseExpiresAt
	) {
		return new IndexMaintenanceOperation(
				10,
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				phase,
				UUID.fromString("22222222-2222-2222-2222-222222222222"),
				leaseExpiresAt,
				3,
				1L,
				2,
				version,
				IndexRepairCause.SURPLUS,
				planFingerprint(),
				NOW.plus(Duration.ofMinutes(15)),
				"local.operator",
				"INDEX_RECEIPT_SURPLUS");
	}

	private static String planFingerprint() {
		return signedPlan().fingerprint();
	}

	private static IndexGeneration generation(
			int number,
			IndexGenerationStatus status,
			IndexGenerationNames names,
			String eventUuid,
			String mentionUuid
	) {
		return new IndexGeneration(
				number,
				UUID.nameUUIDFromBytes(("generation-" + number).getBytes(StandardCharsets.UTF_8)),
				PARTITION_KEY,
				number,
				status,
				names,
				eventUuid,
				mentionUuid,
				0);
	}

	private static ArchiveReceiptVerification matchedReceipt() {
		ArchiveIdentityDigest digest = new ArchiveIdentityDigest("c".repeat(64));
		return new ArchiveReceiptVerification(
				GdeltIndexKind.EVENT,
				2,
				2,
				digest,
				digest,
				ArchiveReceiptStatus.MATCHED);
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
