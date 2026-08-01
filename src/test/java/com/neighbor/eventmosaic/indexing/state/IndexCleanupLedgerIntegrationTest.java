package com.neighbor.eventmosaic.indexing.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.CleanupCandidateSnapshot;
import com.neighbor.eventmosaic.indexing.api.CleanupClaim;
import com.neighbor.eventmosaic.indexing.api.CleanupCompletionEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupDeleteEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupOperation;
import com.neighbor.eventmosaic.indexing.api.CleanupOwnership;
import com.neighbor.eventmosaic.indexing.api.CleanupTransitionResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexCleanupLedger;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Fenced cleanup generations в PostgreSQL")
class IndexCleanupLedgerIntegrationTest {

	private static final String PARTITION_KEY = "p20260713";
	private static final Instant PARTITION_START = Instant.parse("2026-07-13T00:00:00Z");
	private static final Instant NOW = FixedClockTestConfiguration.NOW;
	private static final Duration LEASE = Duration.ofMinutes(15);
	private static final Duration ORPHAN_AGE = Duration.ofHours(24);
	private static final Duration SUPERSEDED_AGE = Duration.ofDays(7);

	@Autowired
	private IndexCleanupLedger cleanupLedger;

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
	@DisplayName("Empty FAILED cleanup сохраняет token при recovery и завершается только после double absence")
	void failedCleanupKeepsTokenAndRequiresBothIndicesAbsent() {
		// Given
		CleanupCandidateSnapshot candidate = createFailedEmptyBuild();
		CleanupOperation claimed = cleanupLedger.claim(claim(candidate, "a".repeat(64)), LEASE)
				.orElseThrow();
		assertThat(claimed.phase()).isEqualTo(IndexMaintenancePhase.CLEANUP_PENDING);
		assertThat(cleanupLedger.hasOpenMaintenance(PARTITION_KEY)).isTrue();
		assertThat(lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(3),
				LEASE)).isEmpty();

		CleanupOwnership stale = new CleanupOwnership(
				claimed.partitionKey(),
				claimed.operationToken(),
				claimed.operationVersion() + 1,
				claimed.partitionVersion(),
				claimed.generationId(),
				claimed.generationVersion(),
				claimed.planFingerprint());
		assertThat(cleanupLedger.requestDelete(stale, deleteEvidence(candidate)))
				.isEqualTo(CleanupTransitionResult.OWNERSHIP_LOST);
		CleanupOperation renewed = cleanupLedger.renew(CleanupOwnership.from(claimed), LEASE)
				.orElseThrow();
		assertThat(renewed.operationToken()).isEqualTo(claimed.operationToken());
		assertThat(renewed.operationVersion()).isEqualTo(claimed.operationVersion() + 1);
		claimed = renewed;

		// When
		assertThat(cleanupLedger.requestDelete(
				CleanupOwnership.from(claimed),
				deleteEvidence(candidate))).isEqualTo(CleanupTransitionResult.APPLIED);
		CleanupOperation deleteRequested = cleanupLedger.findRecoverable(PARTITION_KEY).orElseThrow();
		UUID immutableToken = deleteRequested.operationToken();
		expireLease(deleteRequested.operationId());
		assertThat(cleanupLedger.resume(PARTITION_KEY, "9".repeat(64), LEASE)).isEmpty();
		CleanupOperation resumed = cleanupLedger.resume(
				PARTITION_KEY,
				deleteRequested.planFingerprint(),
				LEASE).orElseThrow();

		// Then
		assertThat(resumed.operationToken()).isEqualTo(immutableToken);
		assertThat(resumed.operationVersion()).isEqualTo(deleteRequested.operationVersion() + 1);
		assertThat(cleanupLedger.complete(
				CleanupOwnership.from(resumed),
				completionEvidence(candidate, true, false)))
				.isEqualTo(CleanupTransitionResult.OWNERSHIP_LOST);
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.DELETE_REQUESTED.name());
		assertThat(cleanupLedger.complete(
				CleanupOwnership.from(resumed),
				completionEvidence(candidate, true, true)))
				.isEqualTo(CleanupTransitionResult.APPLIED);
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.CLEANED.name());
		assertThat(cleanupLedger.findRecoverable(PARTITION_KEY)).isEmpty();
		assertThat(operationToken(claimed.operationId())).isEqualTo(immutableToken);
	}

	@Test
	@DisplayName("Expired REBUILD в PLANNED atomically fences прежнего owner и становится cleanup operation")
	void orphanBuildingClaimFencesPreviousOwner() {
		// Given
		IndexMaintenanceOperation orphan = createRebuildOrphan(
				IndexMaintenancePhase.PLANNED,
				CleanupBuildWriteOutcome.NONE);
		CleanupCandidateSnapshot candidate = onlyCandidate();
		assertThat(candidate.status()).isEqualTo(IndexGenerationStatus.BUILDING);
		assertThat(candidate.buildWriteOutcome()).isEqualTo(CleanupBuildWriteOutcome.NONE);
		assertThat(candidate.orphanOwner()).isNotNull()
				.extracting(owner -> owner.phase())
				.isEqualTo(IndexMaintenancePhase.PLANNED);

		// When
		CleanupOperation cleanup = cleanupLedger.claim(claim(candidate, "b".repeat(64)), LEASE)
				.orElseThrow();

		// Then
		assertThat(cleanup.operationToken()).isNotEqualTo(orphan.token());
		assertThat(jdbcClient.sql("""
				select phase
				from index_maintenance_operations
				where id = :operationId
				""").param("operationId", orphan.id()).query(String.class).single())
				.isEqualTo(IndexMaintenancePhase.FAILED.name());
		assertThat(jdbcClient.sql("""
				select failure_origin
				from index_generations
				where id = :generationId
				""").param("generationId", candidate.generationId()).query(String.class).single())
				.isEqualTo("ORPHANED_BUILD");
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.CLEANUP_PENDING.name());
	}

	@ParameterizedTest(name = "owner phase {0}")
	@EnumSource(value = IndexMaintenancePhase.class, names = {"FROZEN", "BUILDING"})
	@DisplayName("Expired REBUILD после начала freeze fail-closed остается под прежним owner")
	void rebuildAfterFreezeCannotBecomeCleanup(IndexMaintenancePhase ownerPhase) {
		// Given
		IndexMaintenanceOperation orphan = createRebuildOrphan(
				ownerPhase,
				CleanupBuildWriteOutcome.NONE);
		CleanupCandidateSnapshot candidate = onlyCandidate();
		assertThat(candidate.buildWriteOutcome()).isEqualTo(CleanupBuildWriteOutcome.NONE);
		assertThat(candidate.orphanOwner()).isNotNull()
				.extracting(owner -> owner.phase())
				.isEqualTo(ownerPhase);

		// When / Then
		assertThat(cleanupLedger.claim(claim(candidate, "7".repeat(64)), LEASE)).isEmpty();
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.BUILDING.name());
		assertThat(operationPhase(orphan.id())).isEqualTo(ownerPhase.name());
		assertThat(cleanupLedger.hasOpenMaintenance(PARTITION_KEY)).isTrue();
	}

	@ParameterizedTest(name = "owner phase {0}")
	@EnumSource(value = IndexMaintenancePhase.class, names = {"PLANNED", "BUILDING"})
	@DisplayName("Expired INITIAL_PROMOTION в ранней фазе остается допустимым empty orphan")
	void initialPromotionEarlyOrphanCanBeClaimed(IndexMaintenancePhase ownerPhase) {
		// Given
		IndexMaintenanceOperation orphan = createInitialPromotionOrphan(
				ownerPhase,
				CleanupBuildWriteOutcome.NONE);
		CleanupCandidateSnapshot candidate = onlyCandidate();

		// When
		CleanupOperation cleanup = cleanupLedger.claim(claim(candidate, "8".repeat(64)), LEASE)
				.orElseThrow();

		// Then
		assertThat(cleanup.operationToken()).isNotEqualTo(orphan.token());
		assertThat(operationPhase(orphan.id())).isEqualTo(IndexMaintenancePhase.FAILED.name());
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.CLEANUP_PENDING.name());
	}

	@Test
	@DisplayName("Orphan с UNKNOWN write outcome остается недоступным для destructive claim")
	void orphanWithUnknownWritesIsRejected() {
		// Given
		createInitialPromotionOrphan(
				IndexMaintenancePhase.BUILDING,
				CleanupBuildWriteOutcome.UNKNOWN);
		CleanupCandidateSnapshot candidate = onlyCandidate();
		assertThat(candidate.buildWriteOutcome()).isEqualTo(CleanupBuildWriteOutcome.UNKNOWN);

		// When / Then
		assertThat(cleanupLedger.claim(claim(candidate, "c".repeat(64)), LEASE)).isEmpty();
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.BUILDING.name());
	}

	@Test
	@DisplayName("Active processing входит в inspect snapshot и блокирует cleanup claim")
	void activeProcessingClaimBlocksCleanup() {
		// Given
		CleanupCandidateSnapshot initial = createFailedEmptyBuild();
		createActiveProcessingClaim(lifecycleLedger.findActiveTargets(PARTITION_KEY).orElseThrow());
		CleanupCandidateSnapshot candidate = cleanupLedger.findCandidates(
				PARTITION_KEY,
				ORPHAN_AGE,
				SUPERSEDED_AGE).stream()
				.filter(item -> item.generationId() == initial.generationId())
				.findFirst()
				.orElseThrow();

		// When / Then
		assertThat(candidate.activeProcessing()).isTrue();
		assertThat(cleanupLedger.claim(claim(candidate, "d".repeat(64)), LEASE)).isEmpty();
		assertThat(generationState(candidate.generationId()))
				.isEqualTo(IndexGenerationStatus.FAILED.name());
	}

	@Test
	@DisplayName("SUPERSEDED cleanup повторно проверяет protected ACTIVE id и state version")
	void supersededCleanupFencesProtectedActiveVersion() {
		// Given
		insertPartition();
		long supersededId = insertSupersededGeneration(1);
		long activeId = insertActiveGeneration(2);
		insertCompletedOwner(supersededId);
		CleanupCandidateSnapshot stale = onlyCandidate();
		jdbcClient.sql("""
				update index_generations
				set state_version = state_version + 1
				where id = :generationId
				""").param("generationId", activeId).update();
		assertThat(cleanupLedger.claim(claim(stale, "e".repeat(64)), LEASE)).isEmpty();

		CleanupCandidateSnapshot fresh = onlyCandidate();
		CleanupOperation cleanup = cleanupLedger.claim(claim(fresh, "f".repeat(64)), LEASE)
				.orElseThrow();
		assertThat(cleanup.protectedActiveGenerationId()).isEqualTo(activeId);
		assertThat(cleanup.protectedActiveGenerationVersion()).isEqualTo(1L);

		// When
		jdbcClient.sql("""
				update index_generations
				set state_version = state_version + 1
				where id = :generationId
				""").param("generationId", activeId).update();

		// Then
		assertThat(cleanupLedger.requestDelete(
				CleanupOwnership.from(cleanup),
				deleteEvidence(fresh))).isEqualTo(CleanupTransitionResult.OWNERSHIP_LOST);
		assertThat(generationState(supersededId))
				.isEqualTo(IndexGenerationStatus.CLEANUP_PENDING.name());
	}

	@Test
	@DisplayName("ACTIVE generation не становится cleanup candidate независимо от возраста")
	void activeGenerationIsNeverSelectedByAge() {
		registerPartition();
		completeInitialPromotion();
		jdbcClient.sql("""
				update index_generations
				set heartbeat_at = :oldTimestamp,
				    activated_at = :oldTimestamp,
				    updated_at = :oldTimestamp
				where state = 'ACTIVE'
				""")
				.param("oldTimestamp", Timestamp.from(NOW.minus(Duration.ofDays(365))))
				.update();

		assertThat(cleanupLedger.findCandidates(
				PARTITION_KEY,
				ORPHAN_AGE,
				SUPERSEDED_AGE)).isEmpty();
	}

	private CleanupCandidateSnapshot createFailedEmptyBuild() {
		registerPartition();
		completeInitialPromotion();
		IndexMaintenanceOperation rebuild = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(2),
				LEASE).orElseThrow();
		rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN);
		rebuild = advance(rebuild, IndexMaintenancePhase.BUILDING);
		rebuild = advance(rebuild, IndexMaintenancePhase.UNFREEZE_REQUESTED);
		assertThat(lifecycleLedger.completePreCutoverFailure(
				PARTITION_KEY,
				rebuild.token(),
				rebuild.partitionVersion(),
				rebuild.operationVersion(),
				"EMPTY_BUILD")).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		CleanupCandidateSnapshot candidate = onlyCandidate();
		assertThat(candidate.status()).isEqualTo(IndexGenerationStatus.FAILED);
		assertThat(candidate.buildWriteOutcome()).isEqualTo(CleanupBuildWriteOutcome.NONE);
		return candidate;
	}

	private IndexMaintenanceOperation createRebuildOrphan(
			IndexMaintenancePhase ownerPhase,
			CleanupBuildWriteOutcome outcome
	) {
		registerPartition();
		completeInitialPromotion();
		IndexMaintenanceOperation rebuild = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(2),
				LEASE).orElseThrow();
		if (ownerPhase == IndexMaintenancePhase.FROZEN || ownerPhase == IndexMaintenancePhase.BUILDING) {
			rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED);
			rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN);
		}
		if (ownerPhase == IndexMaintenancePhase.BUILDING) {
			rebuild = advance(rebuild, IndexMaintenancePhase.BUILDING);
		}
		if (rebuild.phase() != ownerPhase) {
			throw new IllegalArgumentException("Неподдерживаемая orphan phase для test fixture");
		}
		if (outcome != CleanupBuildWriteOutcome.NONE) {
			if (ownerPhase != IndexMaintenancePhase.BUILDING) {
				throw new IllegalArgumentException("Write outcome можно повысить только в BUILDING");
			}
			assertThat(lifecycleLedger.recordBuildWriteOutcome(
					PARTITION_KEY,
					rebuild.token(),
					rebuild.partitionVersion(),
					rebuild.operationVersion(),
					outcome)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
			rebuild = recoverLifecycle();
		}
		expireOrphan(rebuild);
		return rebuild;
	}

	private IndexMaintenanceOperation createInitialPromotionOrphan(
			IndexMaintenancePhase ownerPhase,
			CleanupBuildWriteOutcome outcome
	) {
		registerPartition();
		IndexMaintenanceOperation initial = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();
		if (ownerPhase == IndexMaintenancePhase.BUILDING) {
			initial = advance(initial, IndexMaintenancePhase.BUILDING);
		}
		if (initial.phase() != ownerPhase) {
			throw new IllegalArgumentException("Неподдерживаемая initial orphan phase для test fixture");
		}
		if (outcome != CleanupBuildWriteOutcome.NONE) {
			assertThat(lifecycleLedger.recordBuildWriteOutcome(
					PARTITION_KEY,
					initial.token(),
					initial.partitionVersion(),
					initial.operationVersion(),
					outcome)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
			initial = recoverLifecycle();
		}
		expireOrphan(initial);
		return initial;
	}

	private void expireOrphan(IndexMaintenanceOperation operation) {
		jdbcClient.sql("""
				update index_maintenance_operations
				set heartbeat_at = :heartbeatAt,
				    lease_expires_at = :leaseExpiresAt
				where id = :operationId
				""")
				.param("heartbeatAt", Timestamp.from(NOW.minus(Duration.ofHours(26))))
				.param("leaseExpiresAt", Timestamp.from(NOW.minus(Duration.ofHours(25))))
				.param("operationId", operation.id())
				.update();
		jdbcClient.sql("""
				update index_generations
				set heartbeat_at = :heartbeatAt
				where id = :generationId
				""")
				.param("heartbeatAt", Timestamp.from(NOW.minus(Duration.ofHours(26))))
				.param("generationId", operation.buildingGenerationId())
				.update();
	}

	private CleanupCandidateSnapshot onlyCandidate() {
		List<CleanupCandidateSnapshot> candidates = cleanupLedger.findCandidates(
				PARTITION_KEY,
				ORPHAN_AGE,
				SUPERSEDED_AGE);
		assertThat(candidates).hasSize(1);
		return candidates.getFirst();
	}

	private CleanupClaim claim(CleanupCandidateSnapshot candidate, String fingerprint) {
		return new CleanupClaim(
				candidate,
				fingerprint,
				NOW.plus(Duration.ofHours(1)),
				"local.operator",
				"EXPLICIT_CLEANUP",
				true,
				candidate.status() == IndexGenerationStatus.SUPERSEDED,
				true);
	}

	private static CleanupDeleteEvidence deleteEvidence(CleanupCandidateSnapshot candidate) {
		return new CleanupDeleteEvidence(
				candidate.names(),
				candidate.eventIndexUuid(),
				candidate.mentionIndexUuid(),
				true,
				candidate.supersededAt() != null && candidate.failedAt() == null,
				true);
	}

	private static CleanupCompletionEvidence completionEvidence(
			CleanupCandidateSnapshot candidate,
			boolean eventAbsent,
			boolean mentionAbsent
	) {
		return new CleanupCompletionEvidence(
				candidate.names(),
				candidate.eventIndexUuid(),
				candidate.mentionIndexUuid(),
				eventAbsent,
				mentionAbsent);
	}

	private void expireLease(long operationId) {
		jdbcClient.sql("""
				update index_maintenance_operations
				set heartbeat_at = :heartbeatAt,
				    lease_expires_at = :leaseExpiresAt
				where id = :operationId
				""")
				.param("heartbeatAt", Timestamp.from(NOW.minus(Duration.ofMinutes(20))))
				.param("leaseExpiresAt", Timestamp.from(NOW.minus(Duration.ofMinutes(5))))
				.param("operationId", operationId)
				.update();
	}

	private void createActiveProcessingClaim(ActiveIndexTargets targets) {
		DiscoveredUpdate discoveredUpdate = update(PARTITION_START.plus(Duration.ofHours(12)));
		DiscoveredArchive archive = discoveredUpdate.archives().getFirst();
		archiveLedger.registerDiscoveredUpdate(discoveredUpdate, FirstRunPolicy.LATEST, null);
		var stagingAttempt = archiveLedger.claimArchive(archive.idempotencyKey(), LEASE).orElseThrow();
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
						"1".repeat(64)));
		GdeltIndexKind kind = archive.archiveType() == ArchiveType.TRANSLATION_EVENTS
				? GdeltIndexKind.EVENT
				: GdeltIndexKind.MENTION;
		assertThat(processingLedger.claim(
				archive.idempotencyKey(),
				new ArchiveProcessingTargetBinding(
						kind,
						targets.partitionKey(),
						targets.partitionStateVersion(),
						targets.generationId(),
						targets.generationUuid(),
						targets.target(kind).indexName(),
						targets.target(kind).indexUuid()),
				LEASE).claimedAttempt()).isPresent();
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
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				"event-uuid-g1",
				"mention-uuid-g1")).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		operation = recoverLifecycle();
		operation = advance(operation, IndexMaintenancePhase.VERIFIED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_REQUESTED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_OBSERVED);
		assertThat(lifecycleLedger.completeInitialActivation(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion())).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
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
		return recoverLifecycle();
	}

	private IndexMaintenanceOperation recoverLifecycle() {
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private void insertPartition() {
		jdbcClient.sql("""
				insert into index_logical_partitions (
				    partition_key,
				    partition_start_at,
				    partition_end_at,
				    partition_interval,
				    state_version,
				    created_at,
				    updated_at
				)
				values (
				    :partitionKey,
				    :startAt,
				    :endAt,
				    'P7D',
				    7,
				    :createdAt,
				    :updatedAt
				)
				""")
				.param("partitionKey", PARTITION_KEY)
				.param("startAt", Timestamp.from(PARTITION_START))
				.param("endAt", Timestamp.from(PARTITION_START.plus(Duration.ofDays(7))))
				.param("createdAt", Timestamp.from(NOW.minus(Duration.ofDays(30))))
				.param("updatedAt", Timestamp.from(NOW.minus(Duration.ofDays(8))))
				.update();
	}

	private long insertSupersededGeneration(int number) {
		return insertGeneration(number, IndexGenerationStatus.SUPERSEDED, NOW.minus(Duration.ofDays(8)));
	}

	private long insertActiveGeneration(int number) {
		return insertGeneration(number, IndexGenerationStatus.ACTIVE, null);
	}

	private long insertGeneration(
			int number,
			IndexGenerationStatus status,
			Instant supersededAt
	) {
		return jdbcClient.sql("""
				insert into index_generations (
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid,
				    heartbeat_at,
				    activated_at,
				    superseded_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    :generationNumber,
				    :state,
				    :eventIndexName,
				    :eventIndexUuid,
				    :mentionIndexName,
				    :mentionIndexUuid,
				    :heartbeatAt,
				    :activatedAt,
				    :supersededAt,
				    :createdAt,
				    :updatedAt
				)
				returning id
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", PARTITION_KEY)
				.param("generationNumber", number)
				.param("state", status.name())
				.param("eventIndexName", names(number).eventIndexName())
				.param("eventIndexUuid", "event-uuid-g" + number)
				.param("mentionIndexName", names(number).mentionIndexName())
				.param("mentionIndexUuid", "mention-uuid-g" + number)
				.param("heartbeatAt", Timestamp.from(NOW.minus(Duration.ofDays(30))))
				.param("activatedAt", Timestamp.from(NOW.minus(Duration.ofDays(30))))
				.param("supersededAt", supersededAt == null ? null : Timestamp.from(supersededAt),
						java.sql.Types.TIMESTAMP)
				.param("createdAt", Timestamp.from(NOW.minus(Duration.ofDays(30))))
				.param("updatedAt", Timestamp.from(NOW.minus(Duration.ofDays(8))))
				.query(Long.class)
				.single();
	}

	private void insertCompletedOwner(long generationId) {
		jdbcClient.sql("""
				insert into index_maintenance_operations (
				    operation_token,
				    partition_key,
				    operation_kind,
				    phase,
				    expected_partition_state_version,
				    target_generation_id,
				    operation_version,
				    lease_expires_at,
				    heartbeat_at,
				    build_write_outcome,
				    completed_at,
				    created_at,
				    updated_at
				)
				values (
				    :operationToken,
				    :partitionKey,
				    'INITIAL_PROMOTION',
				    'COMPLETED',
				    1,
				    :generationId,
				    4,
				    :leaseExpiresAt,
				    :heartbeatAt,
				    'COMPLETED',
				    :completedAt,
				    :createdAt,
				    :updatedAt
				)
				""")
				.param("operationToken", UUID.randomUUID())
				.param("partitionKey", PARTITION_KEY)
				.param("generationId", generationId)
				.param("leaseExpiresAt", Timestamp.from(NOW.minus(Duration.ofDays(8))))
				.param("heartbeatAt", Timestamp.from(NOW.minus(Duration.ofDays(9))))
				.param("completedAt", Timestamp.from(NOW.minus(Duration.ofDays(8))))
				.param("createdAt", Timestamp.from(NOW.minus(Duration.ofDays(30))))
				.param("updatedAt", Timestamp.from(NOW.minus(Duration.ofDays(8))))
				.update();
	}

	private String generationState(long generationId) {
		return jdbcClient.sql("""
				select state
				from index_generations
				where id = :generationId
				""").param("generationId", generationId).query(String.class).single();
	}

	private String operationPhase(long operationId) {
		return jdbcClient.sql("""
				select phase
				from index_maintenance_operations
				where id = :operationId
				""").param("operationId", operationId).query(String.class).single();
	}

	private UUID operationToken(long operationId) {
		return jdbcClient.sql("""
				select operation_token
				from index_maintenance_operations
				where id = :operationId
				""").param("operationId", operationId).query(UUID.class).single();
	}

	private static IndexGenerationNames names(int generation) {
		String suffix = PARTITION_KEY + "-g%04d".formatted(generation);
		return new IndexGenerationNames(
				"gdelt-events-v1-" + suffix,
				"gdelt-mentions-v1-" + suffix);
	}
}
