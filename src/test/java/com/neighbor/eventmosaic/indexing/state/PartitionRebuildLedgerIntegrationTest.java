package com.neighbor.eventmosaic.indexing.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.ArchiveReceiptBinding;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.BaseGenerationDisposition;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.IndexRebuildClaim;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Транзакции PostgreSQL для partition rebuild")
class PartitionRebuildLedgerIntegrationTest {

	private static final String PARTITION_KEY = "p20260727";
	private static final Instant PARTITION_START = Instant.parse("2026-07-27T00:00:00Z");
	private static final Duration LEASE = Duration.ofMinutes(15);

	@Autowired
	private IndexLifecycleLedger lifecycleLedger;

	@Autowired
	private IngestionArchiveLedger archiveLedger;

	@Autowired
	private ArchiveProcessingLedger processingLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanState() {
		jdbcClient.sql("""
				truncate table
				    index_maintenance_operations,
				    ingestion_archive_processing,
				    index_generations,
				    index_logical_partitions,
				    ingestion_source_poll_state,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Observed cutover атомарно активирует generation и переносит receipts")
	void observedCutoverAtomicallyRebindsReceipts() {
		registerPartition();
		completeInitialPromotion();
		ActiveIndexTargets baseTargets = lifecycleLedger.findActiveTargets(PARTITION_KEY)
				.orElseThrow();
		List<ArchiveReceiptBinding> receipts = createIndexedArchivePair(baseTargets);
		IndexMaintenanceOperation rebuild = startRebuild(baseTargets, "a".repeat(64));
		assertThat(lifecycleLedger.findPartition(PARTITION_KEY).orElseThrow())
				.satisfies(partition -> {
					assertThat(partition.repairCause()).isEqualTo(IndexRepairCause.SURPLUS);
					assertThat(partition.repairRequestedAt()).isEqualTo(
							FixedClockTestConfiguration.NOW);
				});
		assertThat(rebuild.planFingerprint()).isEqualTo("a".repeat(64));
		assertThat(rebuild.actor()).isEqualTo("local.operator");

		rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN);
		rebuild = advance(rebuild, IndexMaintenancePhase.BUILDING);
		rebuild = recordUuids(rebuild, "event-uuid-g2", "mention-uuid-g2");
		rebuild = advance(rebuild, IndexMaintenancePhase.VERIFIED);
		rebuild = advance(rebuild, IndexMaintenancePhase.CUTOVER_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.CUTOVER_OBSERVED);

		assertThat(lifecycleLedger.completeObservedCutover(
				PARTITION_KEY,
				rebuild.token(),
				rebuild.partitionVersion(),
				rebuild.operationVersion(),
				BaseGenerationDisposition.FAILED,
				receipts)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);

		List<IndexGeneration> generations = lifecycleLedger.findGenerations(PARTITION_KEY);
		assertThat(generations)
				.extracting(IndexGeneration::status)
				.containsExactly(IndexGenerationStatus.FAILED, IndexGenerationStatus.ACTIVE);
		IndexGeneration active = generations.getLast();
		assertThat(lifecycleLedger.findPartition(PARTITION_KEY).orElseThrow())
				.satisfies(partition -> {
					assertThat(partition.activeGenerationId()).isEqualTo(active.id());
					assertThat(partition.repairCause()).isNull();
					assertThat(partition.repairRequestedAt()).isNull();
				});
		for (ArchiveReceiptBinding receipt : receipts) {
			assertThat(processingLedger.findByArchiveIdempotencyKey(
					receipt.archiveKey()).orElseThrow())
					.satisfies(state -> {
						assertThat(state.status())
								.isEqualTo(com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus.INDEXED);
						assertThat(state.targetBinding().generationId()).isEqualTo(active.id());
						assertThat(state.receipt().verifiedGenerationId()).isEqualTo(active.id());
						assertThat(state.receipt().matched()).isTrue();
					});
		}
		assertThat(jdbcClient.sql("""
				select failure_origin
				from index_generations
				where id = :generationId
				""")
				.param("generationId", baseTargets.generationId())
				.query(String.class)
				.single()).isEqualTo("REBUILD_BASE_INVALID");
	}

	@Test
	@DisplayName("Pre-cutover failure закрывает ownership только после UNFREEZE_REQUESTED")
	void preCutoverFailureRequiresObservedUnfreezePhase() {
		registerPartition();
		completeInitialPromotion();
		ActiveIndexTargets base = lifecycleLedger.findActiveTargets(PARTITION_KEY)
				.orElseThrow();
		IndexMaintenanceOperation rebuild = startRebuild(base, "b".repeat(64));
		rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN);

		assertThat(lifecycleLedger.completePreCutoverFailure(
				PARTITION_KEY,
				rebuild.token(),
				rebuild.partitionVersion(),
				rebuild.operationVersion(),
				"PARTITION_REBUILD_PRE_CUTOVER_FAILED"))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);

		rebuild = advance(rebuild, IndexMaintenancePhase.UNFREEZE_REQUESTED);
		assertThat(lifecycleLedger.completePreCutoverFailure(
				PARTITION_KEY,
				rebuild.token(),
				rebuild.partitionVersion(),
				rebuild.operationVersion(),
				"PARTITION_REBUILD_PRE_CUTOVER_FAILED"))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);

		assertThat(lifecycleLedger.findRecoverableOperation(PARTITION_KEY)).isEmpty();
		assertThat(lifecycleLedger.findPartition(PARTITION_KEY).orElseThrow().repairCause())
				.isEqualTo(IndexRepairCause.SURPLUS);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY))
				.extracting(IndexGeneration::status)
				.containsExactly(IndexGenerationStatus.ACTIVE, IndexGenerationStatus.FAILED);
	}

	@Test
	@DisplayName("Rebuild не стартует, пока обычный processing claim владеет partition")
	void rejectsRebuildWhileProcessingClaimIsActive() {
		registerPartition();
		completeInitialPromotion();
		ActiveIndexTargets base = lifecycleLedger.findActiveTargets(PARTITION_KEY)
				.orElseThrow();
		createActiveProcessingClaim(base);

		assertThat(lifecycleLedger.startRebuild(
				new IndexRebuildClaim(
						PARTITION_KEY,
						base.partitionStateVersion(),
						base.generationId(),
						base.generationUuid(),
						IndexRepairCause.SURPLUS,
						names(2),
						"f".repeat(64),
						FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(30)),
						"local.operator",
						"INDEX_RECEIPT_SURPLUS"),
				LEASE)).isEmpty();
		assertThat(lifecycleLedger.findRecoverableOperation(PARTITION_KEY)).isEmpty();
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY))
				.extracting(IndexGeneration::status)
				.containsExactly(IndexGenerationStatus.ACTIVE);
	}

	private void registerPartition() {
		lifecycleLedger.registerPartition(new IndexPartitionDefinition(
				PARTITION_KEY,
				PARTITION_START,
				PARTITION_START.plus(Duration.ofDays(7)),
				Period.ofDays(7)));
	}

	private void completeInitialPromotion() {
		IndexMaintenanceOperation operation = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();
		operation = advance(operation, IndexMaintenancePhase.BUILDING);
		operation = recordUuids(operation, "event-uuid-g1", "mention-uuid-g1");
		operation = advance(operation, IndexMaintenancePhase.VERIFIED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_REQUESTED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_OBSERVED);
		assertThat(lifecycleLedger.completeInitialActivation(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion()))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
	}

	private IndexMaintenanceOperation startRebuild(
			ActiveIndexTargets base,
			String planFingerprint
	) {
		return lifecycleLedger.startRebuild(
				new IndexRebuildClaim(
						PARTITION_KEY,
						base.partitionStateVersion(),
						base.generationId(),
						base.generationUuid(),
						IndexRepairCause.SURPLUS,
						names(2),
						planFingerprint,
						FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(30)),
						"local.operator",
						"INDEX_RECEIPT_SURPLUS"),
				LEASE).orElseThrow();
	}

	private List<ArchiveReceiptBinding> createIndexedArchivePair(
			ActiveIndexTargets targets
	) {
		DiscoveredUpdate update = update(PARTITION_START.plus(Duration.ofHours(12)));
		archiveLedger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null);
		List<ArchiveReceiptBinding> receipts = new ArrayList<>();
		for (DiscoveredArchive archive : update.archives()) {
			var stagingAttempt = archiveLedger.claimArchive(
					archive.idempotencyKey(), LEASE).orElseThrow();
			assertThat(archiveLedger.markStaged(
					archive.idempotencyKey(),
					stagingAttempt.token(),
					new StagedArchive(
							Path.of("staging", archive.archiveName()),
							Path.of("staging", archive.archiveName().replaceFirst("\\.zip$", "")),
							archive.expectedSizeBytes(),
							archive.expectedMd5())))
					.isEqualTo(AttemptTransitionResult.APPLIED);
			String fingerprintValue = archive.archiveType() == ArchiveType.TRANSLATION_EVENTS
					? "c".repeat(64)
					: "d".repeat(64);
			ArchiveProcessingFingerprint fingerprint = new ArchiveProcessingFingerprint(
					archive.expectedMd5(),
					"projection-v1",
					fingerprintValue);
			processingLedger.register(archive.idempotencyKey(), fingerprint);
			ArchiveProcessingTargetBinding binding = binding(targets, archive.archiveType());
			ArchiveProcessingAttempt attempt = processingLedger.claim(
					archive.idempotencyKey(), binding, LEASE)
					.claimedAttempt().orElseThrow();
			ArchiveIdentityDigest digest = new ArchiveIdentityDigest(
					archive.archiveType() == ArchiveType.TRANSLATION_EVENTS
							? "e".repeat(64)
							: "f".repeat(64));
			ArchiveProcessingProgress progress = new ArchiveProcessingProgress(
					2, 0, 0, 2, 2, 0, 2, null);
			ArchiveReceiptVerification verification = new ArchiveReceiptVerification(
					binding.indexKind(),
					2,
					2,
					digest,
					digest,
					ArchiveReceiptStatus.MATCHED);
			assertThat(processingLedger.markIndexed(attempt, progress, verification))
					.isEqualTo(AttemptTransitionResult.APPLIED);
			ArchiveProcessingState state = processingLedger.findByArchiveIdempotencyKey(
					archive.idempotencyKey()).orElseThrow();
			receipts.add(new ArchiveReceiptBinding(
					archive.idempotencyKey(),
					fingerprintValue,
					state.stateVersion(),
					state.attempt().count(),
					binding.indexKind(),
					2,
					digest));
		}
		return List.copyOf(receipts);
	}

	private void createActiveProcessingClaim(ActiveIndexTargets targets) {
		DiscoveredUpdate discoveredUpdate = update(
				PARTITION_START.plus(Duration.ofHours(12)));
		DiscoveredArchive archive = discoveredUpdate.archives().getFirst();
		archiveLedger.registerDiscoveredUpdate(
				discoveredUpdate,
				FirstRunPolicy.LATEST,
				null);
		var stagingAttempt = archiveLedger.claimArchive(
				archive.idempotencyKey(), LEASE).orElseThrow();
		assertThat(archiveLedger.markStaged(
				archive.idempotencyKey(),
				stagingAttempt.token(),
				new StagedArchive(
						Path.of("staging", archive.archiveName()),
						Path.of("staging", archive.archiveName().replaceFirst("\\.zip$", "")),
						archive.expectedSizeBytes(),
						archive.expectedMd5())))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		processingLedger.register(
				archive.idempotencyKey(),
				new ArchiveProcessingFingerprint(
						archive.expectedMd5(),
						"projection-v1",
						"a".repeat(64)));
		assertThat(processingLedger.claim(
				archive.idempotencyKey(),
				binding(targets, archive.archiveType()),
				LEASE).claimedAttempt()).isPresent();
	}

	private static ArchiveProcessingTargetBinding binding(
			ActiveIndexTargets targets,
			ArchiveType type
	) {
		GdeltIndexKind kind = type == ArchiveType.TRANSLATION_EVENTS
				? GdeltIndexKind.EVENT
				: GdeltIndexKind.MENTION;
		return new ArchiveProcessingTargetBinding(
				kind,
				targets.partitionKey(),
				targets.partitionStateVersion(),
				targets.generationId(),
				targets.generationUuid(),
				targets.target(kind).indexName(),
				targets.target(kind).indexUuid());
	}

	private IndexMaintenanceOperation recordUuids(
			IndexMaintenanceOperation operation,
			String eventUuid,
			String mentionUuid
	) {
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				eventUuid,
				mentionUuid)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private IndexMaintenanceOperation advance(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase next
	) {
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				operation.phase(),
				next)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private static IndexGenerationNames names(int generationNumber) {
		return new IndexGenerationNames(
				"gdelt-events-v1-" + PARTITION_KEY + "-g000" + generationNumber,
				"gdelt-mentions-v1-" + PARTITION_KEY + "-g000" + generationNumber);
	}
}
