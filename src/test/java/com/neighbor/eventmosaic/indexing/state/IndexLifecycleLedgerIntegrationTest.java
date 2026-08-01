package com.neighbor.eventmosaic.indexing.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Интеграция lifecycle logical partitions с PostgreSQL")
class IndexLifecycleLedgerIntegrationTest {

	private static final String PARTITION_KEY = "p20260727";
	private static final Instant PARTITION_START = Instant.parse("2026-07-27T00:00:00Z");
	private static final Duration LEASE = Duration.ofMinutes(15);

	@Autowired
	private IndexLifecycleLedger lifecycleLedger;

	@Autowired
	private JdbcIndexLifecycleRepository lifecycleRepository;

	@Autowired
	private Clock clock;

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
	@DisplayName("PostgreSQL допускает только одну ACTIVE и одну BUILDING generation")
	void postgreSqlAllowsOnlyOneActiveAndOneBuildingGeneration() {
		// Given
		registerPartition();
		completeInitialPromotion();
		IndexMaintenanceOperation rebuild = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(2),
				LEASE).orElseThrow();

		// When / Then
		assertThat(lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(3),
				LEASE)).isEmpty();
		assertDuplicateGenerationRejected(IndexGenerationStatus.ACTIVE, 3);
		assertDuplicateGenerationRejected(IndexGenerationStatus.BUILDING, 4);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY))
				.extracting(IndexGeneration::status)
				.containsExactly(IndexGenerationStatus.ACTIVE, IndexGenerationStatus.BUILDING);
		assertThat(rebuild.baseGenerationId()).isNotNull();
	}

	@Test
	@DisplayName("Stale token, partition version и operation version не меняют BUILDING generation")
	void staleOwnershipCannotChangeBuildingGeneration() {
		// Given
		registerPartition();
		IndexMaintenanceOperation operation = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();

		operation = advance(operation, IndexMaintenancePhase.PLANNED, IndexMaintenancePhase.BUILDING);

		// When / Then
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				UUID.randomUUID(),
				operation.partitionVersion(),
				operation.operationVersion(),
				"event-uuid-g1",
				"mention-uuid-g1"))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion() + 1,
				operation.operationVersion(),
				"event-uuid-g1",
				"mention-uuid-g1"))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY).getFirst().eventIndexUuid())
				.isNull();

		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				"event-uuid-g1",
				null))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY).getFirst())
				.satisfies(generation -> {
					assertThat(generation.eventIndexUuid()).isEqualTo("event-uuid-g1");
					assertThat(generation.mentionIndexUuid()).isNull();
				});
		IndexMaintenanceOperation partial = recoverOperation();
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				partial.token(),
				partial.partitionVersion(),
				partial.operationVersion(),
				null,
				"event-uuid-g1"))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY).getFirst())
				.satisfies(generation -> {
					assertThat(generation.eventIndexUuid()).isEqualTo("event-uuid-g1");
					assertThat(generation.mentionIndexUuid()).isNull();
				});
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				partial.token(),
				partial.partitionVersion(),
				partial.operationVersion(),
				null,
				"mention-uuid-g1"))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				IndexMaintenancePhase.PLANNED,
				IndexMaintenancePhase.BUILDING))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(recoverOperation())
				.satisfies(recovered -> {
					assertThat(recovered.phase()).isEqualTo(IndexMaintenancePhase.BUILDING);
					assertThat(recovered.operationVersion()).isEqualTo(3);
				});
	}

	@Test
	@DisplayName("CUTOVER_REQUESTED восстанавливается и observed cutover атомарно меняет поколения")
	void cutoverRequestedRecoversAndCompletesAtomically() {
		// Given
		registerPartition();
		IndexMaintenanceOperation initial = completeInitialPromotion();
		long initialGenerationId = lifecycleLedger.findPartition(PARTITION_KEY)
				.orElseThrow()
				.activeGenerationId();
		IndexMaintenanceOperation rebuild = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(2),
				LEASE).orElseThrow();
		rebuild = advance(rebuild, IndexMaintenancePhase.PLANNED, IndexMaintenancePhase.FREEZE_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED, IndexMaintenancePhase.FROZEN);
		rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN, IndexMaintenancePhase.BUILDING);
		rebuild = recordUuids(rebuild, 2);
		rebuild = recordCompletedWriteOutcome(rebuild);
		rebuild = advance(rebuild, IndexMaintenancePhase.BUILDING, IndexMaintenancePhase.VERIFIED);
		rebuild = advance(rebuild, IndexMaintenancePhase.VERIFIED, IndexMaintenancePhase.CUTOVER_REQUESTED);

		// When
		IndexLifecycleLedger restartedLedger = new JdbcIndexLifecycleLedger(lifecycleRepository, clock);
		IndexMaintenanceOperation recovered = restartedLedger.findRecoverableOperation(PARTITION_KEY)
				.orElseThrow();

		// Then
		assertThat(recovered.id()).isEqualTo(rebuild.id());
		assertThat(recovered.token()).isEqualTo(rebuild.token());
		assertThat(recovered.phase()).isEqualTo(IndexMaintenancePhase.CUTOVER_REQUESTED);
		assertThat(recovered.baseGenerationId()).isEqualTo(initialGenerationId);
		assertThat(restartedLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(3),
				LEASE)).isEmpty();

		recovered = advance(
				recovered,
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				IndexMaintenancePhase.CUTOVER_OBSERVED);
		assertThat(restartedLedger.completeObservedCutover(
				PARTITION_KEY,
				recovered.token(),
				recovered.partitionVersion(),
				recovered.operationVersion()))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);

		List<IndexGeneration> generations = restartedLedger.findGenerations(PARTITION_KEY);
		assertThat(generations)
				.extracting(IndexGeneration::status)
				.containsExactly(IndexGenerationStatus.SUPERSEDED, IndexGenerationStatus.ACTIVE);
		assertThat(restartedLedger.findPartition(PARTITION_KEY).orElseThrow())
				.satisfies(partition -> {
					assertThat(partition.activeGenerationId()).isEqualTo(generations.getLast().id());
					assertThat(partition.stateVersion()).isEqualTo(initial.partitionVersion() + 3);
				});
		assertThat(restartedLedger.findRecoverableOperation(PARTITION_KEY)).isEmpty();
		assertThat(jdbcClient.sql("""
				select phase
				from index_maintenance_operations
				where id = :operationId
				""")
				.param("operationId", rebuild.id())
				.query(String.class)
				.single()).isEqualTo(IndexMaintenancePhase.COMPLETED.name());
	}

	@Test
	@DisplayName("Expired operation получает новый fenced owner без второй generation")
	void expiredOperationIsConditionallyReclaimedWithoutNewGeneration() {
		registerPartition();
		IndexMaintenanceOperation original = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();

		assertThat(lifecycleLedger.reclaimExpiredMaintenance(PARTITION_KEY, LEASE)).isEmpty();
		jdbcClient.sql("""
				update index_maintenance_operations
				set heartbeat_at = :expiredHeartbeatAt,
				    lease_expires_at = :expiredAt
				where id = :operationId
				""")
				.param("expiredHeartbeatAt", Timestamp.from(clock.instant().minusSeconds(2)))
				.param("expiredAt", Timestamp.from(clock.instant().minusSeconds(1)))
				.param("operationId", original.id())
				.update();

		IndexMaintenanceOperation reclaimed = lifecycleLedger.reclaimExpiredMaintenance(
				PARTITION_KEY,
				LEASE).orElseThrow();

		assertThat(reclaimed.id()).isEqualTo(original.id());
		assertThat(reclaimed.buildingGenerationId())
				.isEqualTo(original.buildingGenerationId());
		assertThat(reclaimed.token()).isNotEqualTo(original.token());
		assertThat(reclaimed.operationVersion()).isEqualTo(original.operationVersion() + 1);
		assertThat(reclaimed.leaseExpiresAt()).isAfter(clock.instant());
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY)).hasSize(1);
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				original.token(),
				original.partitionVersion(),
				original.operationVersion(),
				IndexMaintenancePhase.PLANNED,
				IndexMaintenancePhase.BUILDING))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				reclaimed.token(),
				reclaimed.partitionVersion(),
				reclaimed.operationVersion(),
				IndexMaintenancePhase.PLANNED,
				IndexMaintenancePhase.BUILDING))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
	}

	private void registerPartition() {
		lifecycleLedger.registerPartition(new IndexPartitionDefinition(
				PARTITION_KEY,
				PARTITION_START,
				PARTITION_START.plus(Duration.ofDays(7)),
				Period.ofDays(7)));
	}

	private IndexMaintenanceOperation completeInitialPromotion() {
		IndexMaintenanceOperation operation = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();
		operation = advance(operation, IndexMaintenancePhase.PLANNED, IndexMaintenancePhase.BUILDING);
		operation = recordUuids(operation, 1);
		operation = advance(operation, IndexMaintenancePhase.BUILDING, IndexMaintenancePhase.VERIFIED);
		operation = advance(operation, IndexMaintenancePhase.VERIFIED, IndexMaintenancePhase.CUTOVER_REQUESTED);
		operation = advance(
				operation,
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				IndexMaintenancePhase.CUTOVER_OBSERVED);
		assertThat(lifecycleLedger.completeInitialActivation(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion()))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return operation;
	}

	private IndexMaintenanceOperation recordUuids(IndexMaintenanceOperation operation, int generation) {
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				"event-uuid-g" + generation,
				"mention-uuid-g" + generation))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recoverOperation();
	}

	private IndexMaintenanceOperation recordCompletedWriteOutcome(IndexMaintenanceOperation operation) {
		assertThat(lifecycleLedger.recordBuildWriteOutcome(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				CleanupBuildWriteOutcome.COMPLETED))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recoverOperation();
	}

	private IndexMaintenanceOperation advance(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase expected,
			IndexMaintenancePhase next
	) {
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				expected,
				next))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recoverOperation();
	}

	private IndexMaintenanceOperation recoverOperation() {
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private void assertDuplicateGenerationRejected(IndexGenerationStatus status, int generationNumber) {
		assertThatThrownBy(() -> insertDuplicateGeneration(status, generationNumber))
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining(status == IndexGenerationStatus.ACTIVE
						? "uq_index_generations_active_partition"
						: "uq_index_generations_building_partition");
	}

	private void insertDuplicateGeneration(IndexGenerationStatus status, int generationNumber) {
		String suffix = String.format(Locale.ROOT, "%04d", generationNumber);
		if (status == IndexGenerationStatus.ACTIVE) {
			jdbcClient.sql("""
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
				    :createdAt,
				    :updatedAt
				)
				""")
					.param("generationUuid", UUID.randomUUID())
					.param("partitionKey", PARTITION_KEY)
					.param("generationNumber", generationNumber)
					.param("state", status.name())
					.param("eventIndexName", "gdelt-events-v1-" + PARTITION_KEY + "-g" + suffix)
					.param("eventIndexUuid", "duplicate-event-uuid")
					.param("mentionIndexName", "gdelt-mentions-v1-" + PARTITION_KEY + "-g" + suffix)
					.param("mentionIndexUuid", "duplicate-mention-uuid")
					.param("heartbeatAt", Timestamp.from(FixedClockTestConfiguration.NOW))
					.param("activatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
					.param("createdAt", Timestamp.from(FixedClockTestConfiguration.NOW))
					.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
					.update();
			return;
		}
		jdbcClient.sql("""
				insert into index_generations (
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    mention_index_name,
				    heartbeat_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    :generationNumber,
				    :state,
				    :eventIndexName,
				    :mentionIndexName,
				    :heartbeatAt,
				    :createdAt,
				    :updatedAt
				)
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", PARTITION_KEY)
				.param("generationNumber", generationNumber)
				.param("state", status.name())
				.param("eventIndexName", "gdelt-events-v1-" + PARTITION_KEY + "-g" + suffix)
				.param("mentionIndexName", "gdelt-mentions-v1-" + PARTITION_KEY + "-g" + suffix)
				.param("heartbeatAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("createdAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.update();
	}

	private static IndexGenerationNames names(int generationNumber) {
		String suffix = String.format(Locale.ROOT, "%04d", generationNumber);
		return new IndexGenerationNames(
				"gdelt-events-v1-" + PARTITION_KEY + "-g" + suffix,
				"gdelt-mentions-v1-" + PARTITION_KEY + "-g" + suffix);
	}
}
