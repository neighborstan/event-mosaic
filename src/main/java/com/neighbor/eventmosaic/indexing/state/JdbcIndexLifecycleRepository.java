package com.neighbor.eventmosaic.indexing.state;

import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.ArchiveReceiptBinding;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.BaseGenerationDisposition;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.IndexRebuildClaim;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Выполняет fenced SQL transitions logical partitions и physical generations. */
@Repository
class JdbcIndexLifecycleRepository {

	private static final String ACTIVE = IndexGenerationStatus.ACTIVE.name();
	private static final String BUILDING = IndexGenerationStatus.BUILDING.name();
	private static final String COMPLETED = IndexMaintenancePhase.COMPLETED.name();
	private static final String SELECT_PARTITION = """
			select
			    p.partition_key,
			    p.partition_start_at,
			    p.partition_end_at,
			    p.partition_interval,
			    p.state_version,
			    p.repair_cause,
			    p.repair_requested_at,
			    active.id as active_generation_id
			from index_logical_partitions p
			left join index_generations active
			  on active.partition_key = p.partition_key
			 and active.state = 'ACTIVE'
			where p.partition_key = :partitionKey
			""";
	private static final String SELECT_OPERATION = """
			select
			    id,
			    operation_token,
			    partition_key,
			    operation_kind,
			    phase,
			    lease_expires_at,
			    expected_partition_state_version,
			    base_generation_id,
			    target_generation_id,
			    operation_version,
			    repair_cause,
			    plan_fingerprint,
			    plan_expires_at,
			    actor,
			    reason_code
			from index_maintenance_operations
			where partition_key = :partitionKey
			  and operation_kind in ('INITIAL_PROMOTION', 'REBUILD')
			  and phase not in ('COMPLETED', 'FAILED')
			""";

	private final JdbcClient jdbcClient;

	JdbcIndexLifecycleRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	IndexPartition registerPartition(IndexPartitionDefinition definition, Instant now) {
		jdbcClient.sql("""
				insert into index_logical_partitions (
				    partition_key,
				    partition_start_at,
				    partition_end_at,
				    partition_interval,
				    created_at,
				    updated_at
				)
				values (
				    :partitionKey,
				    :partitionStartAt,
				    :partitionEndAt,
				    :partitionInterval,
				    :createdAt,
				    :updatedAt
				)
				on conflict (partition_key) do nothing
				""")
				.param("partitionKey", definition.partitionKey())
				.param("partitionStartAt", Timestamp.from(definition.startAt()))
				.param("partitionEndAt", Timestamp.from(definition.endAt()))
				.param("partitionInterval", definition.interval().toString())
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.update();
		IndexPartition partition = findPartition(definition.partitionKey()).orElseThrow();
		if (!partition.definition().equals(definition)) {
			throw new IllegalStateException("Partition definition conflicts with registered state");
		}
		return partition;
	}

	Optional<IndexMaintenanceOperation> startMaintenance(
			String partitionKey,
			IndexMaintenanceType type,
			IndexGenerationNames names,
			Duration leaseDuration,
			Instant now
	) {
		IndexPartition partition = findPartitionForUpdate(partitionKey).orElseThrow(
				() -> new IllegalArgumentException("Unknown logical partition"));
		if (hasOpenOperation(partitionKey) || hasBuildingGeneration(partitionKey)) {
			return Optional.empty();
		}
		Long activeGenerationId = partition.activeGenerationId();
		if (type == IndexMaintenanceType.INITIAL_PROMOTION && activeGenerationId != null) {
			return Optional.empty();
		}
		if (type == IndexMaintenanceType.REBUILD && activeGenerationId == null) {
			return Optional.empty();
		}

		int generationNumber = nextGenerationNumber(partitionKey);
		requireNamesMatchGeneration(names, partitionKey, generationNumber);
		long partitionVersion = partition.stateVersion() + 1;
		int fenced = jdbcClient.sql("""
				update index_logical_partitions
				set state_version = :nextVersion,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and state_version = :expectedVersion
				""")
				.param("nextVersion", partitionVersion)
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", partitionKey)
				.param("expectedVersion", partition.stateVersion())
				.update();
		if (fenced != 1) {
			return Optional.empty();
		}

		long targetGenerationId = insertBuildingGeneration(
				partitionKey,
				generationNumber,
				names,
				now);
		UUID operationToken = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		long operationId = jdbcClient.sql("""
				insert into index_maintenance_operations (
				    operation_token,
				    partition_key,
				    operation_kind,
				    phase,
				    expected_partition_state_version,
				    base_generation_id,
				    target_generation_id,
				    lease_expires_at,
				    heartbeat_at,
				    created_at,
				    updated_at
				)
				values (
				    :operationToken,
				    :partitionKey,
				    :operationKind,
				    :phase,
				    :partitionVersion,
				    :baseGenerationId,
				    :targetGenerationId,
				    :leaseExpiresAt,
				    :heartbeatAt,
				    :createdAt,
				    :updatedAt
				)
				returning id
				""")
				.param("operationToken", operationToken)
				.param("partitionKey", partitionKey)
				.param("operationKind", type.name())
				.param("phase", IndexMaintenancePhase.PLANNED.name())
				.param("partitionVersion", partitionVersion)
				.param("baseGenerationId", activeGenerationId, java.sql.Types.BIGINT)
				.param("targetGenerationId", targetGenerationId)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("heartbeatAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(Long.class)
				.single();
		return Optional.of(new IndexMaintenanceOperation(
				operationId,
				partitionKey,
				type,
				IndexMaintenancePhase.PLANNED,
				operationToken,
				leaseExpiresAt,
				partitionVersion,
				activeGenerationId,
				targetGenerationId,
				0));
	}

	Optional<IndexMaintenanceOperation> startRebuild(
			IndexRebuildClaim claim,
			Duration leaseDuration,
			Instant now
	) {
		if (!claim.planExpiresAt().isAfter(now)) {
			return Optional.empty();
		}
		IndexPartition partition = findPartitionForUpdate(claim.partitionKey()).orElseThrow(
				() -> new IllegalArgumentException("Unknown logical partition"));
		if (partition.stateVersion() != claim.expectedPartitionVersion()
				|| partition.activeGenerationId() == null
				|| partition.activeGenerationId() != claim.expectedBaseGenerationId()
				|| hasOpenOperation(claim.partitionKey())
				|| hasBuildingGeneration(claim.partitionKey())
				|| hasActiveProcessingClaim(claim.partitionKey())
				|| !matchesActiveGeneration(claim)) {
			return Optional.empty();
		}

		int generationNumber = nextGenerationNumber(claim.partitionKey());
		requireNamesMatchGeneration(
				claim.targetNames(),
				claim.partitionKey(),
				generationNumber);
		long partitionVersion = partition.stateVersion() + 1;
		int fenced = jdbcClient.sql("""
				update index_logical_partitions
				set state_version = :nextVersion,
				    repair_cause = :repairCause,
				    repair_requested_at = :repairRequestedAt,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and state_version = :expectedVersion
				""")
				.param("nextVersion", partitionVersion)
				.param("repairCause", claim.repairCause().name())
				.param("repairRequestedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", claim.partitionKey())
				.param("expectedVersion", partition.stateVersion())
				.update();
		if (fenced != 1) {
			return Optional.empty();
		}

		long targetGenerationId = insertBuildingGeneration(
				claim.partitionKey(),
				generationNumber,
				claim.targetNames(),
				now);
		UUID operationToken = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		long operationId = jdbcClient.sql("""
				insert into index_maintenance_operations (
				    operation_token,
				    partition_key,
				    operation_kind,
				    phase,
				    repair_cause,
				    expected_partition_state_version,
				    base_generation_id,
				    target_generation_id,
				    lease_expires_at,
				    heartbeat_at,
				    plan_fingerprint,
				    plan_expires_at,
				    actor,
				    reason_code,
				    created_at,
				    updated_at
				)
				values (
				    :operationToken,
				    :partitionKey,
				    'REBUILD',
				    'PLANNED',
				    :repairCause,
				    :partitionVersion,
				    :baseGenerationId,
				    :targetGenerationId,
				    :leaseExpiresAt,
				    :heartbeatAt,
				    :planFingerprint,
				    :planExpiresAt,
				    :actor,
				    :reasonCode,
				    :createdAt,
				    :updatedAt
				)
				returning id
				""")
				.param("operationToken", operationToken)
				.param("partitionKey", claim.partitionKey())
				.param("repairCause", claim.repairCause().name())
				.param("partitionVersion", partitionVersion)
				.param("baseGenerationId", claim.expectedBaseGenerationId())
				.param("targetGenerationId", targetGenerationId)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("heartbeatAt", Timestamp.from(now))
				.param("planFingerprint", claim.planFingerprint())
				.param("planExpiresAt", Timestamp.from(claim.planExpiresAt()))
				.param("actor", claim.actor())
				.param("reasonCode", claim.reasonCode())
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(Long.class)
				.single();
		return Optional.of(new IndexMaintenanceOperation(
				operationId,
				claim.partitionKey(),
				IndexMaintenanceType.REBUILD,
				IndexMaintenancePhase.PLANNED,
				operationToken,
				leaseExpiresAt,
				partitionVersion,
				claim.expectedBaseGenerationId(),
				targetGenerationId,
				0,
				claim.repairCause(),
				claim.planFingerprint(),
				claim.planExpiresAt(),
				claim.actor(),
				claim.reasonCode()));
	}

	private boolean matchesActiveGeneration(IndexRebuildClaim claim) {
		return jdbcClient.sql("""
				select count(*)
				from index_generations
				where id = :generationId
				  and partition_key = :partitionKey
				  and generation_uuid = :generationUuid
				  and state = 'ACTIVE'
				""")
				.param("generationId", claim.expectedBaseGenerationId())
				.param("partitionKey", claim.partitionKey())
				.param("generationUuid", claim.expectedBaseGenerationUuid())
				.query(Integer.class)
				.single() == 1;
	}

	private boolean hasActiveProcessingClaim(String partitionKey) {
		return jdbcClient.sql("""
				select exists (
				    select 1
				    from ingestion_archive_processing
				    where logical_partition_key = :partitionKey
				      and status = 'PROCESSING'
				      and attempt_token is not null
				)
				""")
				.param("partitionKey", partitionKey)
				.query(Boolean.class)
				.single();
	}

	Optional<IndexMaintenanceOperation> reclaimExpiredMaintenance(
			String partitionKey,
			Duration leaseDuration,
			Instant now
	) {
		IndexPartition partition = findPartitionForUpdate(partitionKey).orElseThrow(
				() -> new IllegalArgumentException("Unknown logical partition"));
		Optional<IndexMaintenanceOperation> existing = findOpenOperationForUpdate(partitionKey);
		if (existing.isEmpty()) {
			return Optional.empty();
		}
		IndexMaintenanceOperation operation = existing.orElseThrow();
		if (operation.leaseExpiresAt().isAfter(now)
				|| operation.partitionVersion() != partition.stateVersion()) {
			return Optional.empty();
		}
		UUID nextToken = UUID.randomUUID();
		Instant nextLeaseExpiresAt = now.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set operation_token = :nextToken,
				    lease_expires_at = :nextLeaseExpiresAt,
				    operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_version = :expectedOperationVersion
				  and lease_expires_at <= :now
				  and phase not in ('COMPLETED', 'FAILED')
				""")
				.param("nextToken", nextToken)
				.param("nextLeaseExpiresAt", Timestamp.from(nextLeaseExpiresAt))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.id())
				.param("expectedOperationVersion", operation.operationVersion())
				.param("now", Timestamp.from(now))
				.update();
		if (updated != 1) {
			return Optional.empty();
		}
		return Optional.of(new IndexMaintenanceOperation(
				operation.id(),
				operation.partitionKey(),
				operation.type(),
				operation.phase(),
				nextToken,
				nextLeaseExpiresAt,
				operation.partitionVersion(),
				operation.baseGenerationId(),
				operation.buildingGenerationId(),
				operation.operationVersion() + 1,
				operation.repairCause(),
				operation.planFingerprint(),
				operation.planExpiresAt(),
				operation.actor(),
				operation.reasonCode()));
	}

	Optional<IndexMaintenanceOperation> renewMaintenanceLease(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			Duration leaseDuration,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return Optional.empty();
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		Instant nextLeaseExpiresAt = now.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set lease_expires_at = :leaseExpiresAt,
				    operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_version = :expectedOperationVersion
				  and phase not in ('COMPLETED', 'FAILED')
				""")
				.param("leaseExpiresAt", Timestamp.from(nextLeaseExpiresAt))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.id())
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		if (updated != 1) {
			return Optional.empty();
		}
		return Optional.of(new IndexMaintenanceOperation(
				operation.id(),
				operation.partitionKey(),
				operation.type(),
				operation.phase(),
				operation.token(),
				nextLeaseExpiresAt,
				operation.partitionVersion(),
				operation.baseGenerationId(),
				operation.buildingGenerationId(),
				operation.operationVersion() + 1,
				operation.repairCause(),
				operation.planFingerprint(),
				operation.planExpiresAt(),
				operation.actor(),
				operation.reasonCode()));
	}

	IndexLifecycleTransitionResult recordGenerationUuids(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String eventIndexUuid,
			String mentionIndexUuid,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.phase() != IndexMaintenancePhase.BUILDING) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		int updated = jdbcClient.sql("""
				update index_generations
				set event_index_uuid = coalesce(:eventIndexUuid, event_index_uuid),
				    mention_index_uuid = coalesce(:mentionIndexUuid, mention_index_uuid),
				    state_version = state_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = :buildingState
				  and (
				      :eventIndexUuid is null
				      or event_index_uuid is null
				      or event_index_uuid = :eventIndexUuid
				  )
				  and (
				      :mentionIndexUuid is null
				      or mention_index_uuid is null
				      or mention_index_uuid = :mentionIndexUuid
				  )
				  and (
				      coalesce(:eventIndexUuid, event_index_uuid) is null
				      or coalesce(:mentionIndexUuid, mention_index_uuid) is null
				      or coalesce(:eventIndexUuid, event_index_uuid)
				          <> coalesce(:mentionIndexUuid, mention_index_uuid)
				  )
				""")
				.param("eventIndexUuid", eventIndexUuid, java.sql.Types.VARCHAR)
				.param("mentionIndexUuid", mentionIndexUuid, java.sql.Types.VARCHAR)
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("generationId", operation.buildingGenerationId())
				.param("partitionKey", partitionKey)
				.param("buildingState", BUILDING)
				.update();
		if (updated != 1) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		advanceOperationVersion(operation.id(), expectedOperationVersion, now);
		return IndexLifecycleTransitionResult.APPLIED;
	}

	IndexLifecycleTransitionResult recordBuildWriteOutcome(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			CleanupBuildWriteOutcome minimumOutcome,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty() || owned.orElseThrow().operation().phase() != IndexMaintenancePhase.BUILDING) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set build_write_outcome = case
				        when case build_write_outcome
				                 when 'NONE' then 0
				                 when 'UNKNOWN' then 1
				                 when 'PARTIAL' then 2
				                 when 'COMPLETED' then 3
				             end >= :minimumRank
				            then build_write_outcome
				        else :minimumOutcome
				    end,
				    operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_version = :expectedOperationVersion
				  and phase = 'BUILDING'
				""")
				.param("minimumRank", buildWriteOutcomeRank(minimumOutcome))
				.param("minimumOutcome", minimumOutcome.name())
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.id())
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		return transitionResult(updated);
	}

	IndexLifecycleTransitionResult advancePhase(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			IndexMaintenancePhase expectedPhase,
			IndexMaintenancePhase nextPhase,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.phase() != expectedPhase
				|| !isAllowedForType(operation.type(), expectedPhase, nextPhase)
				|| nextPhase == IndexMaintenancePhase.VERIFIED && !hasExactGenerationUuids(operation)
				|| operation.type() == IndexMaintenanceType.REBUILD
						&& expectedPhase == IndexMaintenancePhase.BUILDING
						&& nextPhase == IndexMaintenancePhase.VERIFIED
						&& !hasCompletedBuildWriteOutcome(operation.id())) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set phase = :nextPhase,
				    operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_version = :expectedOperationVersion
				""")
				.param("nextPhase", nextPhase.name())
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.id())
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		return transitionResult(updated);
	}

	private boolean hasCompletedBuildWriteOutcome(long operationId) {
		return jdbcClient.sql("""
				select build_write_outcome = 'COMPLETED'
				from index_maintenance_operations
				where id = :operationId
				""")
				.param("operationId", operationId)
				.query(Boolean.class)
				.optional()
				.orElse(false);
	}

	IndexLifecycleTransitionResult completeInitialActivation(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.type() != IndexMaintenanceType.INITIAL_PROMOTION
				|| operation.phase() != IndexMaintenancePhase.CUTOVER_OBSERVED
				|| operation.baseGenerationId() != null
				|| owned.orElseThrow().activeGenerationId() != null
				|| !hasExactGenerationUuids(operation)) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		activateGeneration(operation.buildingGenerationId(), partitionKey, now);
		completeOperation(operation.id(), expectedOperationVersion, now);
		bumpPartitionAfterCompletion(partitionKey, expectedPartitionVersion, now);
		return IndexLifecycleTransitionResult.APPLIED;
	}

	IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.type() != IndexMaintenanceType.REBUILD
				|| operation.phase() != IndexMaintenancePhase.CUTOVER_OBSERVED
				|| operation.baseGenerationId() == null
				|| !operation.baseGenerationId().equals(owned.orElseThrow().activeGenerationId())
				|| !hasExactGenerationUuids(operation)) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		supersedeGeneration(operation.baseGenerationId(), partitionKey, now);
		activateGeneration(operation.buildingGenerationId(), partitionKey, now);
		completeOperation(operation.id(), expectedOperationVersion, now);
		bumpPartitionAfterCompletion(partitionKey, expectedPartitionVersion, now);
		return IndexLifecycleTransitionResult.APPLIED;
	}

	IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			BaseGenerationDisposition baseDisposition,
			List<ArchiveReceiptBinding> receipts,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.type() != IndexMaintenanceType.REBUILD
				|| operation.phase() != IndexMaintenancePhase.CUTOVER_OBSERVED
				|| operation.baseGenerationId() == null
				|| !operation.baseGenerationId().equals(owned.orElseThrow().activeGenerationId())
				|| operation.planFingerprint() == null
				|| operation.repairCause() == null
				|| !hasExactGenerationUuids(operation)) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		if (receipts.stream().map(ArchiveReceiptBinding::archiveKey).distinct().count()
				!= receipts.size()) {
			throw new IllegalArgumentException("receipt archive keys must be unique");
		}
		IndexGeneration target = findGeneration(
				partitionKey,
				operation.buildingGenerationId()).orElseThrow();
		if (baseDisposition == BaseGenerationDisposition.SUPERSEDED) {
			supersedeGeneration(operation.baseGenerationId(), partitionKey, now);
		}
		else {
			failActiveGeneration(operation.baseGenerationId(), partitionKey, now);
		}
		activateGeneration(operation.buildingGenerationId(), partitionKey, now);
		for (ArchiveReceiptBinding receipt : receipts) {
			rebindReceipt(
					partitionKey,
					expectedPartitionVersion + 1,
					operation.baseGenerationId(),
					target,
					receipt,
					now);
		}
		completeOperation(operation.id(), expectedOperationVersion, now);
		bumpPartitionAfterCompletion(
				partitionKey,
				expectedPartitionVersion,
				now,
				true);
		return IndexLifecycleTransitionResult.APPLIED;
	}

	IndexLifecycleTransitionResult completePreCutoverFailure(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String errorCode,
			Instant now
	) {
		Optional<LockedOperation> owned = findOwnedOperation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				now);
		if (owned.isEmpty()) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		IndexMaintenanceOperation operation = owned.orElseThrow().operation();
		if (operation.type() != IndexMaintenanceType.REBUILD
				|| operation.phase() != IndexMaintenancePhase.UNFREEZE_REQUESTED) {
			return IndexLifecycleTransitionResult.OWNERSHIP_LOST;
		}
		failBuildingGeneration(
				operation.buildingGenerationId(),
				partitionKey,
				errorCode,
				now);
		failOperation(
				operation.id(),
				expectedOperationVersion,
				errorCode,
				now);
		bumpPartitionAfterCompletion(
				partitionKey,
				expectedPartitionVersion,
				now,
				false);
		return IndexLifecycleTransitionResult.APPLIED;
	}

	Optional<IndexMaintenanceOperation> findRecoverableOperation(String partitionKey) {
		return findOpenOperation(partitionKey);
	}

	Optional<IndexPartition> findPartition(String partitionKey) {
		return jdbcClient.sql(SELECT_PARTITION)
				.param("partitionKey", partitionKey)
				.query(JdbcIndexLifecycleRepository::mapPartition)
				.optional();
	}

	Optional<ActiveIndexTargets> findActiveTargets(String partitionKey) {
		return jdbcClient.sql("""
				select
				    p.partition_key,
				    p.state_version as partition_state_version,
				    active.id as generation_id,
				    active.generation_uuid,
				    active.event_index_name,
				    active.event_index_uuid,
				    active.mention_index_name,
				    active.mention_index_uuid
				from index_logical_partitions p
				join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where p.partition_key = :partitionKey
				""")
				.param("partitionKey", partitionKey)
				.query((resultSet, rowNumber) -> new ActiveIndexTargets(
						resultSet.getString("partition_key"),
						resultSet.getLong("partition_state_version"),
						resultSet.getLong("generation_id"),
						resultSet.getObject("generation_uuid", UUID.class),
						new ExactIndexTarget(
								resultSet.getString("event_index_name"),
								resultSet.getString("event_index_uuid")),
						new ExactIndexTarget(
								resultSet.getString("mention_index_name"),
								resultSet.getString("mention_index_uuid"))))
				.optional();
	}

	List<IndexGeneration> findGenerations(String partitionKey) {
		return jdbcClient.sql("""
				select
				    id,
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid,
				    state_version
				from index_generations
				where partition_key = :partitionKey
				order by generation_number
				""")
				.param("partitionKey", partitionKey)
				.query(JdbcIndexLifecycleRepository::mapGeneration)
				.list();
	}

	private Optional<IndexGeneration> findGeneration(
			String partitionKey,
			long generationId
	) {
		return jdbcClient.sql("""
				select
				    id,
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid,
				    state_version
				from index_generations
				where partition_key = :partitionKey
				  and id = :generationId
				""")
				.param("partitionKey", partitionKey)
				.param("generationId", generationId)
				.query(JdbcIndexLifecycleRepository::mapGeneration)
				.optional();
	}

	private Optional<IndexPartition> findPartitionForUpdate(String partitionKey) {
		return jdbcClient.sql(SELECT_PARTITION + "for update of p")
				.param("partitionKey", partitionKey)
				.query(JdbcIndexLifecycleRepository::mapPartition)
				.optional();
	}

	private Optional<IndexMaintenanceOperation> findOpenOperation(String partitionKey) {
		return jdbcClient.sql(SELECT_OPERATION)
				.param("partitionKey", partitionKey)
				.query(JdbcIndexLifecycleRepository::mapOperation)
				.optional();
	}

	private Optional<IndexMaintenanceOperation> findOpenOperationForUpdate(String partitionKey) {
		return jdbcClient.sql(SELECT_OPERATION + "for update")
				.param("partitionKey", partitionKey)
				.query(JdbcIndexLifecycleRepository::mapOperation)
				.optional();
	}

	private boolean hasOpenOperation(String partitionKey) {
		return jdbcClient.sql("""
				select count(*)
				from index_maintenance_operations
				where partition_key = :partitionKey
				  and phase not in ('COMPLETED', 'FAILED')
				""")
				.param("partitionKey", partitionKey)
				.query(Integer.class)
				.single() > 0;
	}

	private boolean hasBuildingGeneration(String partitionKey) {
		return jdbcClient.sql("""
				select count(*)
				from index_generations
				where partition_key = :partitionKey
				  and state = :buildingState
				""")
				.param("partitionKey", partitionKey)
				.param("buildingState", BUILDING)
				.query(Integer.class)
				.single() > 0;
	}

	private Optional<LockedOperation> findOwnedOperation(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			Instant now
	) {
		return jdbcClient.sql("""
				select
				    o.id,
				    o.operation_token,
				    o.partition_key,
				    o.operation_kind,
				    o.phase,
				    o.lease_expires_at,
				    o.expected_partition_state_version,
				    o.base_generation_id,
				    o.target_generation_id,
				    o.operation_version,
				    o.repair_cause,
				    o.plan_fingerprint,
				    o.plan_expires_at,
				    o.actor,
				    o.reason_code,
				    p.state_version as current_partition_version,
				    active.id as active_generation_id
				from index_maintenance_operations o
				join index_logical_partitions p on p.partition_key = o.partition_key
				left join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where o.partition_key = :partitionKey
				  and o.operation_token = :operationToken
				  and o.expected_partition_state_version = :expectedPartitionVersion
				  and o.operation_version = :expectedOperationVersion
				  and o.phase not in ('COMPLETED', 'FAILED')
				for update of o, p
				""")
				.param("partitionKey", partitionKey)
				.param("operationToken", operationToken)
				.param("expectedPartitionVersion", expectedPartitionVersion)
				.param("expectedOperationVersion", expectedOperationVersion)
				.query((resultSet, rowNumber) -> new LockedOperation(
						mapOperation(resultSet, rowNumber),
						resultSet.getLong("current_partition_version"),
						resultSet.getObject("active_generation_id", Long.class)))
				.optional()
				.filter(locked -> locked.currentPartitionVersion() == expectedPartitionVersion)
				.filter(locked -> locked.operation().leaseExpiresAt().isAfter(now));
	}

	private long insertBuildingGeneration(
			String partitionKey,
			int generationNumber,
			IndexGenerationNames names,
			Instant now
	) {
		return jdbcClient.sql("""
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
				returning id
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", partitionKey)
				.param("generationNumber", generationNumber)
				.param("state", BUILDING)
				.param("eventIndexName", names.eventIndexName())
				.param("mentionIndexName", names.mentionIndexName())
				.param("heartbeatAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(Long.class)
				.single();
	}

	private int nextGenerationNumber(String partitionKey) {
		return jdbcClient.sql("""
				select coalesce(max(generation_number), 0) + 1
				from index_generations
				where partition_key = :partitionKey
				""")
				.param("partitionKey", partitionKey)
				.query(Integer.class)
				.single();
	}

	private boolean hasExactGenerationUuids(IndexMaintenanceOperation operation) {
		return jdbcClient.sql("""
				select count(*)
				from index_generations
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = :state
				  and event_index_uuid is not null
				  and mention_index_uuid is not null
				""")
				.param("generationId", operation.buildingGenerationId())
				.param("partitionKey", operation.partitionKey())
				.param("state", BUILDING)
				.query(Integer.class)
				.single() == 1;
	}

	private void advanceOperationVersion(long operationId, long expectedOperationVersion, Instant now) {
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_version = :expectedOperationVersion
				""")
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operationId)
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		requireSingleUpdate(updated, "Operation version");
	}

	private void activateGeneration(long generationId, String partitionKey, Instant now) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = :activeState,
				    state_version = state_version + 1,
				    activated_at = :activatedAt,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = :buildingState
				  and event_index_uuid is not null
				  and mention_index_uuid is not null
				""")
				.param("activeState", ACTIVE)
				.param("activatedAt", Timestamp.from(now))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("generationId", generationId)
				.param("partitionKey", partitionKey)
				.param("buildingState", BUILDING)
				.update();
		requireSingleUpdate(updated, "Generation activation");
	}

	private void supersedeGeneration(long generationId, String partitionKey, Instant now) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = :supersededState,
				    state_version = state_version + 1,
				    superseded_at = :supersededAt,
				    updated_at = :updatedAt
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = :activeState
				""")
				.param("supersededState", IndexGenerationStatus.SUPERSEDED.name())
				.param("supersededAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("generationId", generationId)
				.param("partitionKey", partitionKey)
				.param("activeState", ACTIVE)
				.update();
		requireSingleUpdate(updated, "Generation supersede");
	}

	private void failActiveGeneration(long generationId, String partitionKey, Instant now) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = 'FAILED',
				    state_version = state_version + 1,
				    failure_origin = 'REBUILD_BASE_INVALID',
				    failed_at = :failedAt,
				    updated_at = :updatedAt
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = 'ACTIVE'
				""")
				.param("failedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("generationId", generationId)
				.param("partitionKey", partitionKey)
				.update();
		requireSingleUpdate(updated, "Generation failure");
	}

	private void failBuildingGeneration(
			long generationId,
			String partitionKey,
			String errorCode,
			Instant now
	) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = 'FAILED',
				    state_version = state_version + 1,
				    failure_origin = :failureOrigin,
				    failed_at = :failedAt,
				    updated_at = :updatedAt
				where id = :generationId
				  and partition_key = :partitionKey
				  and state = 'BUILDING'
				""")
				.param("failureOrigin", errorCode)
				.param("failedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("generationId", generationId)
				.param("partitionKey", partitionKey)
				.update();
		requireSingleUpdate(updated, "Building generation failure");
	}

	private void rebindReceipt(
			String partitionKey,
			long nextPartitionVersion,
			long baseGenerationId,
			IndexGeneration target,
			ArchiveReceiptBinding receipt,
			Instant now
	) {
		String indexName = receipt.kind() == com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT
				? target.names().eventIndexName()
				: target.names().mentionIndexName();
		String indexUuid = receipt.kind() == com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT
				? target.eventIndexUuid()
				: target.mentionIndexUuid();
		int updated = jdbcClient.sql("""
				update ingestion_archive_processing processing
				set status = 'INDEXED',
				    attempt_token = null,
				    lease_expires_at = null,
				    bound_partition_state_version = :partitionVersion,
				    bound_generation_id = :generationId,
				    bound_generation_uuid = :generationUuid,
				    bound_index_kind = :indexKind,
				    bound_index_name = :indexName,
				    bound_index_uuid = :indexUuid,
				    automatic_retries_used = 0,
				    consecutive_retryable_failures = 0,
				    retry_not_before = null,
				    verified_generation_id = :generationId,
				    verified_index_uuid = :indexUuid,
				    actual_document_count = expected_document_count,
				    actual_identity_digest = expected_identity_digest,
				    receipt_verified_at = :verifiedAt,
				    failed_at = null,
				    completed_at = :completedAt,
				    last_error_code = null,
				    last_error_retryable = null,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where processing.archive_idempotency_key = :archiveKey
				  and processing.logical_partition_key = :partitionKey
				  and processing.processing_fingerprint = :processingFingerprint
				  and processing.state_version = :expectedStateVersion
				  and processing.total_attempt_count = :expectedAttemptCount
				  and processing.status in ('INDEXED', 'FAILED')
				  and processing.attempt_token is null
				  and processing.expected_document_count = :expectedDocumentCount
				  and processing.expected_identity_digest = :expectedIdentityDigest
				  and processing.receipt_digest_algorithm = 'sha256-length-prefix-v1'
				  and processing.verified_generation_id = :baseGenerationId
				  and exists (
				      select 1
				      from ingestion_archives archive
				      where archive.idempotency_key = processing.archive_idempotency_key
				        and archive.archive_type = :archiveType
				  )
				""")
				.param("partitionVersion", nextPartitionVersion)
				.param("generationId", target.id())
				.param("generationUuid", target.generationUuid())
				.param("indexKind", receipt.kind().name())
				.param("indexName", indexName)
				.param("indexUuid", indexUuid)
				.param("verifiedAt", Timestamp.from(now))
				.param("completedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("archiveKey", receipt.archiveKey())
				.param("partitionKey", partitionKey)
				.param("processingFingerprint", receipt.processingFingerprint())
				.param("expectedStateVersion", receipt.expectedStateVersion())
				.param("expectedAttemptCount", receipt.expectedAttemptCount())
				.param("expectedDocumentCount", receipt.expectedDocumentCount())
				.param("expectedIdentityDigest", receipt.expectedIdentityDigest().value())
				.param("baseGenerationId", baseGenerationId)
				.param("archiveType", receipt.kind() == com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT
						? "TRANSLATION_EVENTS"
						: "TRANSLATION_MENTIONS")
				.update();
		requireSingleUpdate(updated, "Archive receipt rebind");
	}

	private void completeOperation(long operationId, long expectedOperationVersion, Instant now) {
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set phase = :completedPhase,
				    operation_version = operation_version + 1,
				    completed_at = :completedAt,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and phase = :observedPhase
				  and operation_version = :expectedOperationVersion
				""")
				.param("completedPhase", COMPLETED)
				.param("completedAt", Timestamp.from(now))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operationId)
				.param("observedPhase", IndexMaintenancePhase.CUTOVER_OBSERVED.name())
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		requireSingleUpdate(updated, "Operation completion");
	}

	private void failOperation(
			long operationId,
			long expectedOperationVersion,
			String errorCode,
			Instant now
	) {
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set phase = 'FAILED',
				    operation_version = operation_version + 1,
				    last_error_code = :errorCode,
				    failed_at = :failedAt,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and phase = 'UNFREEZE_REQUESTED'
				  and operation_version = :expectedOperationVersion
				""")
				.param("errorCode", errorCode)
				.param("failedAt", Timestamp.from(now))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operationId)
				.param("expectedOperationVersion", expectedOperationVersion)
				.update();
		requireSingleUpdate(updated, "Operation failure");
	}

	private void bumpPartitionAfterCompletion(
			String partitionKey,
			long expectedPartitionVersion,
			Instant now
	) {
		bumpPartitionAfterCompletion(
				partitionKey,
				expectedPartitionVersion,
				now,
				false);
	}

	private void bumpPartitionAfterCompletion(
			String partitionKey,
			long expectedPartitionVersion,
			Instant now,
			boolean clearRepairCause
	) {
		int updated = jdbcClient.sql("""
				update index_logical_partitions
				set state_version = state_version + 1,
				    repair_cause = case when :clearRepairCause then null else repair_cause end,
				    repair_requested_at = case
				        when :clearRepairCause then null
				        else repair_requested_at
				    end,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and state_version = :expectedPartitionVersion
				""")
				.param("clearRepairCause", clearRepairCause)
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", partitionKey)
				.param("expectedPartitionVersion", expectedPartitionVersion)
				.update();
		requireSingleUpdate(updated, "Partition completion fence");
	}

	private static void requireNamesMatchGeneration(
			IndexGenerationNames names,
			String partitionKey,
			int generationNumber
	) {
		String suffix = "-" + partitionKey + "-g"
				+ String.format(Locale.ROOT, "%04d", generationNumber);
		if (!names.eventIndexName().endsWith(suffix)
				|| !names.mentionIndexName().endsWith(suffix)) {
			throw new IllegalArgumentException(
					"Physical index names must match partition and generation number");
		}
		String eventVersion = names.eventIndexName().substring(
				"gdelt-events-".length(),
				names.eventIndexName().length() - suffix.length());
		String mentionVersion = names.mentionIndexName().substring(
				"gdelt-mentions-".length(),
				names.mentionIndexName().length() - suffix.length());
		if (!eventVersion.equals(mentionVersion)) {
			throw new IllegalArgumentException("Event and Mention index schema versions must match");
		}
	}

	private static boolean isAllowedForType(
			IndexMaintenanceType type,
			IndexMaintenancePhase expected,
			IndexMaintenancePhase next
	) {
		if (!expected.canAdvanceTo(next)) {
			return false;
		}
		if (expected == IndexMaintenancePhase.PLANNED) {
			return type == IndexMaintenanceType.INITIAL_PROMOTION
					? next == IndexMaintenancePhase.BUILDING
					: type == IndexMaintenanceType.REBUILD
							&& next == IndexMaintenancePhase.FREEZE_REQUESTED;
		}
		return type != IndexMaintenanceType.CLEANUP;
	}

	private static int buildWriteOutcomeRank(CleanupBuildWriteOutcome outcome) {
		return switch (outcome) {
			case NONE -> 0;
			case UNKNOWN -> 1;
			case PARTIAL -> 2;
			case COMPLETED -> 3;
		};
	}

	private static void requireSingleUpdate(int updated, String transition) {
		if (updated != 1) {
			throw new IllegalStateException(transition + " changed an unexpected row count: " + updated);
		}
	}

	private static IndexLifecycleTransitionResult transitionResult(int updated) {
		return switch (updated) {
			case 0 -> IndexLifecycleTransitionResult.OWNERSHIP_LOST;
			case 1 -> IndexLifecycleTransitionResult.APPLIED;
			default -> throw new IllegalStateException(
					"Lifecycle transition changed an unexpected row count: " + updated);
		};
	}

	private static IndexPartition mapPartition(ResultSet resultSet, int rowNumber) throws SQLException {
		IndexPartitionDefinition definition = new IndexPartitionDefinition(
				resultSet.getString("partition_key"),
				resultSet.getTimestamp("partition_start_at").toInstant(),
				resultSet.getTimestamp("partition_end_at").toInstant(),
				Period.parse(resultSet.getString("partition_interval")));
		return new IndexPartition(
				definition,
				resultSet.getLong("state_version"),
				resultSet.getObject("active_generation_id", Long.class),
				resultSet.getString("repair_cause") == null
						? null
						: IndexRepairCause.valueOf(resultSet.getString("repair_cause")),
				resultSet.getTimestamp("repair_requested_at") == null
						? null
						: resultSet.getTimestamp("repair_requested_at").toInstant());
	}

	private static IndexGeneration mapGeneration(ResultSet resultSet, int rowNumber) throws SQLException {
		return new IndexGeneration(
				resultSet.getLong("id"),
				resultSet.getObject("generation_uuid", UUID.class),
				resultSet.getString("partition_key"),
				resultSet.getInt("generation_number"),
				IndexGenerationStatus.valueOf(resultSet.getString("state")),
				new IndexGenerationNames(
						resultSet.getString("event_index_name"),
						resultSet.getString("mention_index_name")),
				resultSet.getString("event_index_uuid"),
				resultSet.getString("mention_index_uuid"),
				resultSet.getLong("state_version"));
	}

	private static IndexMaintenanceOperation mapOperation(ResultSet resultSet, int rowNumber)
			throws SQLException {
		return new IndexMaintenanceOperation(
				resultSet.getLong("id"),
				resultSet.getString("partition_key"),
				IndexMaintenanceType.valueOf(resultSet.getString("operation_kind")),
				IndexMaintenancePhase.valueOf(resultSet.getString("phase")),
				resultSet.getObject("operation_token", UUID.class),
				resultSet.getTimestamp("lease_expires_at").toInstant(),
				resultSet.getLong("expected_partition_state_version"),
				resultSet.getObject("base_generation_id", Long.class),
				resultSet.getLong("target_generation_id"),
				resultSet.getLong("operation_version"),
				resultSet.getString("repair_cause") == null
						? null
						: IndexRepairCause.valueOf(resultSet.getString("repair_cause")),
				resultSet.getString("plan_fingerprint"),
				resultSet.getTimestamp("plan_expires_at") == null
						? null
						: resultSet.getTimestamp("plan_expires_at").toInstant(),
				resultSet.getString("actor"),
				resultSet.getString("reason_code"));
	}

	private record LockedOperation(
			IndexMaintenanceOperation operation,
			long currentPartitionVersion,
			Long activeGenerationId
	) {
	}
}
