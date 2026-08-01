package com.neighbor.eventmosaic.indexing.state;

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
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Выполняет SQL-переходы cleanup с CAS по operation, partition и generation. */
@Repository
class JdbcIndexCleanupRepository {

	private static final String CANDIDATE_COLUMNS = """
			p.partition_key,
			p.state_version as partition_version,
			p.repair_cause,
			g.id as generation_id,
			g.generation_uuid,
			g.generation_number,
			g.state as generation_state,
			g.state_version as generation_version,
			g.event_index_name,
			g.event_index_uuid,
			g.mention_index_name,
			g.mention_index_uuid,
			g.failure_origin,
			g.heartbeat_at as generation_heartbeat_at,
			g.activated_at,
			g.superseded_at,
			g.failed_at as generation_failed_at,
			owner.id as owner_operation_id,
			owner.operation_kind as owner_operation_kind,
			owner.phase as owner_phase,
			owner.operation_token as owner_operation_token,
			owner.operation_version as owner_operation_version,
			owner.lease_expires_at as owner_lease_expires_at,
			owner.heartbeat_at as owner_heartbeat_at,
			owner.build_write_outcome as owner_build_write_outcome,
			active.id as active_generation_id,
			active.generation_uuid as active_generation_uuid,
			active.state_version as active_generation_version,
			active.event_index_name as active_event_index_name,
			active.event_index_uuid as active_event_index_uuid,
			active.mention_index_name as active_mention_index_name,
			active.mention_index_uuid as active_mention_index_uuid,
			exists (
			    select 1
			    from ingestion_archive_processing processing
			    where processing.logical_partition_key = p.partition_key
			      and processing.status = 'PROCESSING'
			      and processing.attempt_token is not null
			) as active_processing
			""";

	private static final String CLEANUP_OPERATION_COLUMNS = """
			o.id,
			o.partition_key,
			o.phase,
			o.operation_token,
			o.operation_version,
			o.lease_expires_at,
			o.expected_partition_state_version,
			o.cleanup_generation_id,
			o.cleanup_generation_state_version,
			o.base_generation_id,
			o.cleanup_protected_generation_state_version,
			o.plan_fingerprint,
			o.plan_expires_at,
			o.actor,
			o.reason_code
			""";

	private final JdbcClient jdbcClient;

	JdbcIndexCleanupRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	List<CleanupCandidateSnapshot> findCandidates(
			String partitionKey,
			Instant orphanHeartbeatCutoff,
			Instant supersededCutoff,
			Instant now
	) {
		return jdbcClient.sql("""
				select
				""" + CANDIDATE_COLUMNS + """
				from index_logical_partitions p
				join index_generations g on g.partition_key = p.partition_key
				left join index_maintenance_operations owner
				  on owner.partition_key = g.partition_key
				 and owner.target_generation_id = g.id
				 and owner.operation_kind in ('INITIAL_PROMOTION', 'REBUILD')
				left join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where p.partition_key = :partitionKey
				  and (
				      g.state = 'FAILED'
				      or (
				          g.state = 'SUPERSEDED'
				          and g.superseded_at <= :supersededCutoff
				          and active.id is not null
				      )
				      or (
				          g.state = 'BUILDING'
				          and g.heartbeat_at <= :orphanHeartbeatCutoff
				          and owner.id is not null
				          and owner.phase not in ('COMPLETED', 'FAILED')
				          and owner.lease_expires_at <= :now
				      )
				  )
				order by g.generation_number
				""")
				.param("partitionKey", partitionKey)
				.param("orphanHeartbeatCutoff", Timestamp.from(orphanHeartbeatCutoff))
				.param("supersededCutoff", Timestamp.from(supersededCutoff))
				.param("now", Timestamp.from(now))
				.query(JdbcIndexCleanupRepository::mapCandidate)
				.list();
	}

	Optional<CleanupOperation> claim(CleanupClaim claim, Duration leaseDuration, Instant now) {
		CleanupCandidateSnapshot expected = claim.candidate();
		Optional<LockedCandidate> locked = expected.status() == IndexGenerationStatus.BUILDING
				? lockOrphanCandidate(expected)
				: lockRegularCandidate(expected.partitionKey(), expected.generationId());
		if (locked.isEmpty() || !matches(expected, locked.orElseThrow())) {
			return Optional.empty();
		}
		LockedCandidate current = locked.orElseThrow();
		if (current.repairCause() != null
				|| hasActiveProcessingClaim(expected.partitionKey())
				|| !matchesProtectedActive(expected.protectedActive(), current.protectedActive())) {
			return Optional.empty();
		}
		if (!claim.aliasesConfirmedAbsent()
				|| !claim.replaySourcesConfirmed()
				|| ((expected.status() == IndexGenerationStatus.FAILED
						|| expected.status() == IndexGenerationStatus.BUILDING)
						&& (current.buildWriteOutcome() != CleanupBuildWriteOutcome.NONE
								|| "REBUILD_BASE_INVALID".equals(current.generation().failureOrigin())))
				|| (expected.status() == IndexGenerationStatus.SUPERSEDED
						&& !claim.currentReceiptsConfirmed())) {
			return Optional.empty();
		}

		if (expected.status() == IndexGenerationStatus.BUILDING) {
			if (current.owner() == null
					|| !isSafeOrphanPhase(current.owner())
					|| current.owner().leaseExpiresAt().isAfter(now)
					|| hasOtherOpenMaintenance(expected.partitionKey(), current.owner().operationId())) {
				return Optional.empty();
			}
			failOrphanOwner(current.owner(), now);
			markOrphanFailed(current.generation(), now);
			markCleanupPending(
					expected.partitionKey(),
					expected.generationId(),
					IndexGenerationStatus.FAILED,
					expected.generationVersion() + 1,
					now);
		} else {
			if (hasOpenMaintenance(expected.partitionKey())) {
				return Optional.empty();
			}
			markCleanupPending(
					expected.partitionKey(),
					expected.generationId(),
					expected.status(),
					expected.generationVersion(),
					now);
		}

		long cleanupGenerationVersion = expected.generationVersion()
				+ (expected.status() == IndexGenerationStatus.BUILDING ? 2 : 1);
		long partitionVersion = bumpPartitionVersion(expected.partitionKey(), expected.partitionVersion(), now);
		return Optional.of(insertCleanupOperation(
				claim,
				partitionVersion,
				cleanupGenerationVersion,
				leaseDuration,
				now));
	}

	Optional<CleanupOperation> resume(
			String partitionKey,
			String planFingerprint,
			Duration leaseDuration,
			Instant now
	) {
		Optional<LockedCleanupOperation> locked = lockCleanupOperation(partitionKey, planFingerprint);
		if (locked.isEmpty()
				|| locked.orElseThrow().operation().leaseExpiresAt().isAfter(now)
				|| !isInternallyConsistent(locked.orElseThrow())) {
			return Optional.empty();
		}
		return Optional.of(refreshLease(locked.orElseThrow(), leaseDuration, now));
	}

	Optional<CleanupOperation> renew(CleanupOwnership ownership, Duration leaseDuration, Instant now) {
		Optional<LockedCleanupOperation> locked = lockCleanupOperation(
				ownership.partitionKey(),
				ownership.operationToken());
		if (locked.isEmpty()
				|| !locked.orElseThrow().operation().leaseExpiresAt().isAfter(now)
				|| !matches(ownership, locked.orElseThrow())
				|| !isInternallyConsistent(locked.orElseThrow())) {
			return Optional.empty();
		}
		return Optional.of(refreshLease(locked.orElseThrow(), leaseDuration, now));
	}

	CleanupTransitionResult requestDelete(
			CleanupOwnership ownership,
			CleanupDeleteEvidence evidence,
			Instant now
	) {
		Optional<LockedCleanupOperation> locked = lockCleanupOperation(
				ownership.partitionKey(),
				ownership.operationToken());
		if (locked.isEmpty()) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}
		LockedCleanupOperation current = locked.orElseThrow();
		if (!current.operation().leaseExpiresAt().isAfter(now)
				|| current.operation().phase() != IndexMaintenancePhase.CLEANUP_PENDING
				|| !evidence.aliasesConfirmedAbsent()
				|| !evidence.replaySourcesConfirmed()
				|| !matches(ownership, current)
				|| !isInternallyConsistent(current)
				|| current.repairCause() != null
				|| hasActiveProcessingClaim(ownership.partitionKey())
				|| !matchesEvidence(current.generation(), evidence)) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}
		if (isSupersededOrigin(current.generation()) && !evidence.currentReceiptsConfirmed()) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}

		long generationVersion = transitionGeneration(
				current.generation(),
				IndexGenerationStatus.CLEANUP_PENDING,
				IndexGenerationStatus.DELETE_REQUESTED,
				now);
		advanceCleanupOperation(
				current.operation(),
				IndexMaintenancePhase.CLEANUP_PENDING,
				IndexMaintenancePhase.DELETE_REQUESTED,
				generationVersion,
				now);
		return CleanupTransitionResult.APPLIED;
	}

	CleanupTransitionResult complete(
			CleanupOwnership ownership,
			CleanupCompletionEvidence evidence,
			Instant now
	) {
		Optional<LockedCleanupOperation> locked = lockCleanupOperation(
				ownership.partitionKey(),
				ownership.operationToken());
		if (locked.isEmpty()) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}
		LockedCleanupOperation current = locked.orElseThrow();
		if (!current.operation().leaseExpiresAt().isAfter(now)
				|| current.operation().phase() != IndexMaintenancePhase.DELETE_REQUESTED
				|| !evidence.eventIndexAbsent()
				|| !evidence.mentionIndexAbsent()
				|| !matches(ownership, current)
				|| !isInternallyConsistent(current)
				|| hasActiveProcessingClaim(ownership.partitionKey())
				|| !matchesEvidence(current.generation(), evidence)) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}

		long generationVersion = transitionGeneration(
				current.generation(),
				IndexGenerationStatus.DELETE_REQUESTED,
				IndexGenerationStatus.CLEANED,
				now);
		advanceCleanupOperation(
				current.operation(),
				IndexMaintenancePhase.DELETE_REQUESTED,
				IndexMaintenancePhase.COMPLETED,
				generationVersion,
				now);
		return CleanupTransitionResult.APPLIED;
	}

	Optional<CleanupOperation> findRecoverable(String partitionKey) {
		return jdbcClient.sql("""
				select
				""" + CLEANUP_OPERATION_COLUMNS + """
				from index_maintenance_operations o
				where o.partition_key = :partitionKey
				  and o.operation_kind = 'CLEANUP'
				  and o.phase in ('CLEANUP_PENDING', 'DELETE_REQUESTED')
				""")
				.param("partitionKey", partitionKey)
				.query(JdbcIndexCleanupRepository::mapCleanupOperation)
				.optional();
	}

	boolean hasOpenMaintenance(String partitionKey) {
		return jdbcClient.sql("""
				select exists (
				    select 1
				    from index_maintenance_operations
				    where partition_key = :partitionKey
				      and phase not in ('COMPLETED', 'FAILED')
				)
				""")
				.param("partitionKey", partitionKey)
				.query(Boolean.class)
				.single();
	}

	private Optional<LockedCandidate> lockRegularCandidate(String partitionKey, long generationId) {
		return jdbcClient.sql("""
				select
				""" + CANDIDATE_COLUMNS + """
				from index_logical_partitions p
				join index_generations g
				  on g.partition_key = p.partition_key
				 and g.id = :generationId
				left join index_maintenance_operations owner
				  on owner.partition_key = g.partition_key
				 and owner.target_generation_id = g.id
				 and owner.operation_kind in ('INITIAL_PROMOTION', 'REBUILD')
				left join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where p.partition_key = :partitionKey
				for update of p, g
				""")
				.param("partitionKey", partitionKey)
				.param("generationId", generationId)
				.query(JdbcIndexCleanupRepository::mapLockedCandidate)
				.optional();
	}

	private Optional<LockedCandidate> lockOrphanCandidate(CleanupCandidateSnapshot candidate) {
		CleanupOrphanOwner owner = candidate.orphanOwner();
		if (owner == null) {
			return Optional.empty();
		}
		return jdbcClient.sql("""
				select
				""" + CANDIDATE_COLUMNS + """
				from index_maintenance_operations owner
				join index_logical_partitions p on p.partition_key = owner.partition_key
				join index_generations g
				  on g.partition_key = owner.partition_key
				 and g.id = owner.target_generation_id
				left join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where owner.id = :ownerOperationId
				  and owner.partition_key = :partitionKey
				  and g.id = :generationId
				  and owner.operation_kind in ('INITIAL_PROMOTION', 'REBUILD')
				  and owner.phase not in ('COMPLETED', 'FAILED')
				for update of owner, p, g
				""")
				.param("ownerOperationId", owner.operationId())
				.param("partitionKey", candidate.partitionKey())
				.param("generationId", candidate.generationId())
				.query(JdbcIndexCleanupRepository::mapLockedCandidate)
				.optional();
	}

	private Optional<LockedCleanupOperation> lockCleanupOperation(String partitionKey, UUID operationToken) {
		return lockCleanupOperation(partitionKey, "o.operation_token = :operationToken", specification ->
				specification.param("operationToken", operationToken));
	}

	private Optional<LockedCleanupOperation> lockCleanupOperation(String partitionKey, String planFingerprint) {
		return lockCleanupOperation(partitionKey, "o.plan_fingerprint = :planFingerprint", specification ->
				specification.param("planFingerprint", planFingerprint));
	}

	private Optional<LockedCleanupOperation> lockCleanupOperation(
			String partitionKey,
			String selector,
			java.util.function.UnaryOperator<JdbcClient.StatementSpec> binder
	) {
		String sql = """
				select
				%s,
				p.state_version as current_partition_version,
				p.repair_cause,
				g.generation_uuid,
				g.generation_number,
				g.state as generation_state,
				g.state_version as current_generation_version,
				g.event_index_name,
				g.event_index_uuid,
				g.mention_index_name,
				g.mention_index_uuid,
				g.failure_origin,
				g.heartbeat_at as generation_heartbeat_at,
				g.activated_at,
				g.superseded_at,
				g.failed_at as generation_failed_at,
				active.id as active_generation_id,
				active.state_version as active_generation_version
				from index_maintenance_operations o
				join index_logical_partitions p on p.partition_key = o.partition_key
				join index_generations g
				  on g.partition_key = o.partition_key
				 and g.id = o.cleanup_generation_id
				left join index_generations active
				  on active.partition_key = p.partition_key
				 and active.state = 'ACTIVE'
				where o.partition_key = :partitionKey
				  and o.operation_kind = 'CLEANUP'
				  and o.phase in ('CLEANUP_PENDING', 'DELETE_REQUESTED')
				  and %s
				for update of o, p, g
				""".formatted(CLEANUP_OPERATION_COLUMNS, selector);
		JdbcClient.StatementSpec statement = jdbcClient.sql(sql)
				.param("partitionKey", partitionKey);
		return binder.apply(statement)
				.query(JdbcIndexCleanupRepository::mapLockedCleanupOperation)
				.optional();
	}

	private void failOrphanOwner(CleanupOrphanOwner owner, Instant now) {
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set phase = 'FAILED',
				    operation_version = operation_version + 1,
				    last_error_code = 'ORPHANED_BUILD',
				    failed_at = :failedAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_token = :operationToken
				  and operation_version = :operationVersion
				  and phase = :phase
				""")
				.param("failedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", owner.operationId())
				.param("operationToken", owner.operationToken())
				.param("operationVersion", owner.operationVersion())
				.param("phase", owner.phase().name())
				.update();
		requireSingleUpdate(updated, "Orphan owner changed while locked");
	}

	private void markOrphanFailed(GenerationRow generation, Instant now) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = 'FAILED',
				    failure_origin = 'ORPHANED_BUILD',
				    failed_at = :failedAt,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and id = :generationId
				  and state = 'BUILDING'
				  and state_version = :generationVersion
				""")
				.param("failedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", generation.partitionKey())
				.param("generationId", generation.id())
				.param("generationVersion", generation.stateVersion())
				.update();
		requireSingleUpdate(updated, "Orphan generation changed while locked");
	}

	private void markCleanupPending(
			String partitionKey,
			long generationId,
			IndexGenerationStatus expectedState,
			long expectedVersion,
			Instant now
	) {
		int updated = jdbcClient.sql("""
				update index_generations
				set state = 'CLEANUP_PENDING',
				    cleanup_requested_at = :cleanupRequestedAt,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and id = :generationId
				  and state = :expectedState
				  and state_version = :expectedVersion
				""")
				.param("cleanupRequestedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", partitionKey)
				.param("generationId", generationId)
				.param("expectedState", expectedState.name())
				.param("expectedVersion", expectedVersion)
				.update();
		requireSingleUpdate(updated, "Cleanup candidate changed while locked");
	}

	private long bumpPartitionVersion(String partitionKey, long expectedVersion, Instant now) {
		int updated = jdbcClient.sql("""
				update index_logical_partitions
				set state_version = state_version + 1,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and state_version = :expectedVersion
				  and repair_cause is null
				""")
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", partitionKey)
				.param("expectedVersion", expectedVersion)
				.update();
		requireSingleUpdate(updated, "Partition changed while locked");
		return expectedVersion + 1;
	}

	private CleanupOperation insertCleanupOperation(
			CleanupClaim claim,
			long partitionVersion,
			long generationVersion,
			Duration leaseDuration,
			Instant now
	) {
		UUID operationToken = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		Long protectedActiveGenerationId = claim.candidate().protectedActive() == null
				? null
				: claim.candidate().protectedActive().generationId();
		long operationId = jdbcClient.sql("""
				insert into index_maintenance_operations (
				    operation_token,
				    partition_key,
				    operation_kind,
				    phase,
				    expected_partition_state_version,
				    base_generation_id,
				    cleanup_protected_generation_state_version,
				    cleanup_generation_id,
				    cleanup_generation_state_version,
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
				    'CLEANUP',
				    'CLEANUP_PENDING',
				    :partitionVersion,
				    :baseGenerationId,
				    :protectedActiveGenerationVersion,
				    :cleanupGenerationId,
				    :cleanupGenerationVersion,
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
				.param("partitionKey", claim.candidate().partitionKey())
				.param("partitionVersion", partitionVersion)
				.param("baseGenerationId", protectedActiveGenerationId, java.sql.Types.BIGINT)
				.param(
						"protectedActiveGenerationVersion",
						claim.candidate().protectedActive() == null
								? null
								: claim.candidate().protectedActive().generationVersion(),
						java.sql.Types.BIGINT)
				.param("cleanupGenerationId", claim.candidate().generationId())
				.param("cleanupGenerationVersion", generationVersion)
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
		return new CleanupOperation(
				operationId,
				claim.candidate().partitionKey(),
				IndexMaintenancePhase.CLEANUP_PENDING,
				operationToken,
				0,
				leaseExpiresAt,
				partitionVersion,
				claim.candidate().generationId(),
				generationVersion,
				protectedActiveGenerationId,
				claim.candidate().protectedActive() == null
						? null
						: claim.candidate().protectedActive().generationVersion(),
				claim.planFingerprint(),
				claim.planExpiresAt(),
				claim.actor(),
				claim.reasonCode());
	}

	private CleanupOperation refreshLease(
			LockedCleanupOperation locked,
			Duration leaseDuration,
			Instant now
	) {
		CleanupOperation operation = locked.operation();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set operation_version = operation_version + 1,
				    lease_expires_at = :leaseExpiresAt,
				    heartbeat_at = :heartbeatAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_token = :operationToken
				  and operation_version = :operationVersion
				  and phase = :phase
				""")
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("heartbeatAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.operationId())
				.param("operationToken", operation.operationToken())
				.param("operationVersion", operation.operationVersion())
				.param("phase", operation.phase().name())
				.update();
		requireSingleUpdate(updated, "Cleanup ownership changed while locked");
		return new CleanupOperation(
				operation.operationId(),
				operation.partitionKey(),
				operation.phase(),
				operation.operationToken(),
				operation.operationVersion() + 1,
				leaseExpiresAt,
				operation.partitionVersion(),
				operation.generationId(),
				operation.generationVersion(),
				operation.protectedActiveGenerationId(),
				operation.protectedActiveGenerationVersion(),
				operation.planFingerprint(),
				operation.planExpiresAt(),
				operation.actor(),
				operation.reasonCode());
	}

	private long transitionGeneration(
			GenerationRow generation,
			IndexGenerationStatus expected,
			IndexGenerationStatus next,
			Instant now
	) {
		String timestampColumn = next == IndexGenerationStatus.DELETE_REQUESTED
				? "delete_requested_at"
				: "cleaned_at";
		String sql = """
				update index_generations
				set state = :nextState,
				    %s = :transitionAt,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				  and id = :generationId
				  and state = :expectedState
				  and state_version = :generationVersion
				""".formatted(timestampColumn);
		int updated = jdbcClient.sql(sql)
				.param("nextState", next.name())
				.param("transitionAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("partitionKey", generation.partitionKey())
				.param("generationId", generation.id())
				.param("expectedState", expected.name())
				.param("generationVersion", generation.stateVersion())
				.update();
		requireSingleUpdate(updated, "Cleanup generation transition lost ownership");
		return generation.stateVersion() + 1;
	}

	private void advanceCleanupOperation(
			CleanupOperation operation,
			IndexMaintenancePhase expected,
			IndexMaintenancePhase next,
			long generationVersion,
			Instant now
	) {
		boolean completed = next == IndexMaintenancePhase.COMPLETED;
		int updated = jdbcClient.sql("""
				update index_maintenance_operations
				set phase = :nextPhase,
				    cleanup_generation_state_version = :generationVersion,
				    operation_version = operation_version + 1,
				    heartbeat_at = :heartbeatAt,
				    completed_at = :completedAt,
				    updated_at = :updatedAt
				where id = :operationId
				  and operation_token = :operationToken
				  and operation_version = :operationVersion
				  and phase = :expectedPhase
				""")
				.param("nextPhase", next.name())
				.param("generationVersion", generationVersion)
				.param("heartbeatAt", Timestamp.from(now))
				.param("completedAt", completed ? Timestamp.from(now) : null, java.sql.Types.TIMESTAMP)
				.param("updatedAt", Timestamp.from(now))
				.param("operationId", operation.operationId())
				.param("operationToken", operation.operationToken())
				.param("operationVersion", operation.operationVersion())
				.param("expectedPhase", expected.name())
				.update();
		requireSingleUpdate(updated, "Cleanup operation transition lost ownership");
	}

	private boolean hasOtherOpenMaintenance(String partitionKey, long operationId) {
		return jdbcClient.sql("""
				select exists (
				    select 1
				    from index_maintenance_operations
				    where partition_key = :partitionKey
				      and id <> :operationId
				      and phase not in ('COMPLETED', 'FAILED')
				)
				""")
				.param("partitionKey", partitionKey)
				.param("operationId", operationId)
				.query(Boolean.class)
				.single();
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

	private static boolean matches(CleanupCandidateSnapshot expected, LockedCandidate current) {
		GenerationRow generation = current.generation();
		return expected.partitionKey().equals(generation.partitionKey())
				&& expected.partitionVersion() == current.partitionVersion()
				&& expected.repairOpen() == (current.repairCause() != null)
				&& expected.generationId() == generation.id()
				&& expected.generationUuid().equals(generation.generationUuid())
				&& expected.generationNumber() == generation.generationNumber()
				&& expected.status() == generation.state()
				&& expected.generationVersion() == generation.stateVersion()
				&& expected.names().equals(generation.names())
				&& Objects.equals(expected.eventIndexUuid(), generation.eventIndexUuid())
				&& Objects.equals(expected.mentionIndexUuid(), generation.mentionIndexUuid())
				&& Objects.equals(expected.failureOrigin(), generation.failureOrigin())
				&& expected.heartbeatAt().equals(generation.heartbeatAt())
				&& Objects.equals(expected.supersededAt(), generation.supersededAt())
				&& Objects.equals(expected.failedAt(), generation.failedAt())
				&& Objects.equals(expected.orphanOwner(), current.owner())
				&& expected.buildWriteOutcome() == current.buildWriteOutcome()
				&& expected.activeProcessing() == current.activeProcessing();
	}

	private static boolean matches(CleanupOwnership ownership, LockedCleanupOperation current) {
		CleanupOperation operation = current.operation();
		return ownership.partitionKey().equals(operation.partitionKey())
				&& ownership.operationToken().equals(operation.operationToken())
				&& ownership.operationVersion() == operation.operationVersion()
				&& ownership.partitionVersion() == operation.partitionVersion()
				&& ownership.generationId() == operation.generationId()
				&& ownership.generationVersion() == operation.generationVersion()
				&& ownership.planFingerprint().equals(operation.planFingerprint());
	}

	private static boolean isInternallyConsistent(LockedCleanupOperation current) {
		CleanupOperation operation = current.operation();
		IndexGenerationStatus expectedGenerationState = operation.phase() == IndexMaintenancePhase.CLEANUP_PENDING
				? IndexGenerationStatus.CLEANUP_PENDING
				: IndexGenerationStatus.DELETE_REQUESTED;
		return operation.partitionVersion() == current.currentPartitionVersion()
				&& operation.generationVersion() == current.generation().stateVersion()
				&& current.generation().state() == expectedGenerationState
				&& Objects.equals(operation.protectedActiveGenerationId(), current.activeGenerationId())
				&& Objects.equals(
						operation.protectedActiveGenerationVersion(),
						current.activeGenerationVersion());
	}

	private static boolean matchesProtectedActive(
			CleanupProtectedActive expected,
			CleanupProtectedActive current
	) {
		return Objects.equals(expected, current);
	}

	private static boolean matchesEvidence(GenerationRow generation, CleanupDeleteEvidence evidence) {
		return generation.names().equals(evidence.names())
				&& Objects.equals(generation.eventIndexUuid(), evidence.eventIndexUuid())
				&& Objects.equals(generation.mentionIndexUuid(), evidence.mentionIndexUuid());
	}

	private static boolean matchesEvidence(GenerationRow generation, CleanupCompletionEvidence evidence) {
		return generation.names().equals(evidence.names())
				&& Objects.equals(generation.eventIndexUuid(), evidence.eventIndexUuid())
				&& Objects.equals(generation.mentionIndexUuid(), evidence.mentionIndexUuid());
	}

	private static boolean isSupersededOrigin(GenerationRow generation) {
		return generation.supersededAt() != null
				&& generation.failedAt() == null
				&& generation.failureOrigin() == null;
	}

	private static boolean isSafeOrphanPhase(CleanupOrphanOwner owner) {
		return switch (owner.type()) {
			case INITIAL_PROMOTION -> owner.phase() == IndexMaintenancePhase.PLANNED
					|| owner.phase() == IndexMaintenancePhase.BUILDING;
			case REBUILD -> owner.phase() == IndexMaintenancePhase.PLANNED;
			case CLEANUP -> false;
		};
	}

	private static CleanupCandidateSnapshot mapCandidate(ResultSet resultSet, int rowNumber) throws SQLException {
		LockedCandidate candidate = mapLockedCandidate(resultSet, rowNumber);
		GenerationRow generation = candidate.generation();
		return new CleanupCandidateSnapshot(
				generation.partitionKey(),
				candidate.partitionVersion(),
				candidate.repairCause() != null,
				generation.id(),
				generation.generationUuid(),
				generation.generationNumber(),
				generation.state(),
				generation.stateVersion(),
				generation.names(),
				generation.eventIndexUuid(),
				generation.mentionIndexUuid(),
				generation.failureOrigin(),
				generation.heartbeatAt(),
				generation.supersededAt(),
				generation.failedAt(),
				candidate.owner(),
				candidate.buildWriteOutcome(),
				candidate.activeProcessing(),
				candidate.protectedActive());
	}

	private static LockedCandidate mapLockedCandidate(ResultSet resultSet, int rowNumber) throws SQLException {
		GenerationRow generation = mapGeneration(resultSet);
		CleanupOrphanOwner owner = generation.state() == IndexGenerationStatus.BUILDING
				? mapOwner(resultSet)
				: null;
		String rawWriteOutcome = resultSet.getString("owner_build_write_outcome");
		return new LockedCandidate(
				resultSet.getLong("partition_version"),
				resultSet.getString("repair_cause"),
				generation,
				owner,
				mapProtectedActive(resultSet),
				rawWriteOutcome == null
						? CleanupBuildWriteOutcome.UNKNOWN
						: CleanupBuildWriteOutcome.valueOf(rawWriteOutcome),
				resultSet.getBoolean("active_processing"));
	}

	private static GenerationRow mapGeneration(ResultSet resultSet) throws SQLException {
		return new GenerationRow(
				resultSet.getLong("generation_id"),
				resultSet.getObject("generation_uuid", UUID.class),
				resultSet.getString("partition_key"),
				resultSet.getInt("generation_number"),
				IndexGenerationStatus.valueOf(resultSet.getString("generation_state")),
				resultSet.getLong("generation_version"),
				new IndexGenerationNames(
						resultSet.getString("event_index_name"),
						resultSet.getString("mention_index_name")),
				resultSet.getString("event_index_uuid"),
				resultSet.getString("mention_index_uuid"),
				resultSet.getString("failure_origin"),
				toInstant(resultSet, "generation_heartbeat_at"),
				toInstant(resultSet, "activated_at"),
				toInstant(resultSet, "superseded_at"),
				toInstant(resultSet, "generation_failed_at"));
	}

	private static CleanupOrphanOwner mapOwner(ResultSet resultSet) throws SQLException {
		Long operationId = nullableLong(resultSet, "owner_operation_id");
		if (operationId == null) {
			return null;
		}
		return new CleanupOrphanOwner(
				operationId,
				IndexMaintenanceType.valueOf(resultSet.getString("owner_operation_kind")),
				IndexMaintenancePhase.valueOf(resultSet.getString("owner_phase")),
				resultSet.getObject("owner_operation_token", UUID.class),
				resultSet.getLong("owner_operation_version"),
				toInstant(resultSet, "owner_lease_expires_at"),
				toInstant(resultSet, "owner_heartbeat_at"));
	}

	private static CleanupProtectedActive mapProtectedActive(ResultSet resultSet) throws SQLException {
		Long generationId = nullableLong(resultSet, "active_generation_id");
		if (generationId == null) {
			return null;
		}
		return new CleanupProtectedActive(
				generationId,
				resultSet.getObject("active_generation_uuid", UUID.class),
				resultSet.getLong("active_generation_version"),
				new IndexGenerationNames(
						resultSet.getString("active_event_index_name"),
						resultSet.getString("active_mention_index_name")),
				resultSet.getString("active_event_index_uuid"),
				resultSet.getString("active_mention_index_uuid"));
	}

	private static CleanupOperation mapCleanupOperation(ResultSet resultSet, int rowNumber) throws SQLException {
		return new CleanupOperation(
				resultSet.getLong("id"),
				resultSet.getString("partition_key"),
				IndexMaintenancePhase.valueOf(resultSet.getString("phase")),
				resultSet.getObject("operation_token", UUID.class),
				resultSet.getLong("operation_version"),
				toInstant(resultSet, "lease_expires_at"),
				resultSet.getLong("expected_partition_state_version"),
				resultSet.getLong("cleanup_generation_id"),
				resultSet.getLong("cleanup_generation_state_version"),
				nullableLong(resultSet, "base_generation_id"),
				nullableLong(resultSet, "cleanup_protected_generation_state_version"),
				resultSet.getString("plan_fingerprint"),
				toInstant(resultSet, "plan_expires_at"),
				resultSet.getString("actor"),
				resultSet.getString("reason_code"));
	}

	private static LockedCleanupOperation mapLockedCleanupOperation(ResultSet resultSet, int rowNumber)
			throws SQLException {
		CleanupOperation operation = mapCleanupOperation(resultSet, rowNumber);
		GenerationRow generation = new GenerationRow(
				operation.generationId(),
				resultSet.getObject("generation_uuid", UUID.class),
				operation.partitionKey(),
				resultSet.getInt("generation_number"),
				IndexGenerationStatus.valueOf(resultSet.getString("generation_state")),
				resultSet.getLong("current_generation_version"),
				new IndexGenerationNames(
						resultSet.getString("event_index_name"),
						resultSet.getString("mention_index_name")),
				resultSet.getString("event_index_uuid"),
				resultSet.getString("mention_index_uuid"),
				resultSet.getString("failure_origin"),
				toInstant(resultSet, "generation_heartbeat_at"),
				toInstant(resultSet, "activated_at"),
				toInstant(resultSet, "superseded_at"),
				toInstant(resultSet, "generation_failed_at"));
		return new LockedCleanupOperation(
				operation,
				resultSet.getLong("current_partition_version"),
				resultSet.getString("repair_cause"),
				generation,
				nullableLong(resultSet, "active_generation_id"),
				nullableLong(resultSet, "active_generation_version"));
	}

	private static Instant toInstant(ResultSet resultSet, String column) throws SQLException {
		Timestamp timestamp = resultSet.getTimestamp(column);
		return timestamp == null ? null : timestamp.toInstant();
	}

	private static Long nullableLong(ResultSet resultSet, String column) throws SQLException {
		long value = resultSet.getLong(column);
		return resultSet.wasNull() ? null : value;
	}

	private static void requireSingleUpdate(int updated, String message) {
		if (updated != 1) {
			throw new IllegalStateException(message);
		}
	}

	private record GenerationRow(
			long id,
			UUID generationUuid,
			String partitionKey,
			int generationNumber,
			IndexGenerationStatus state,
			long stateVersion,
			IndexGenerationNames names,
			String eventIndexUuid,
			String mentionIndexUuid,
			String failureOrigin,
			Instant heartbeatAt,
			Instant activatedAt,
			Instant supersededAt,
			Instant failedAt
	) {
	}

	private record LockedCandidate(
			long partitionVersion,
			String repairCause,
			GenerationRow generation,
			CleanupOrphanOwner owner,
			CleanupProtectedActive protectedActive,
			CleanupBuildWriteOutcome buildWriteOutcome,
			boolean activeProcessing
	) {
	}

	private record LockedCleanupOperation(
			CleanupOperation operation,
			long currentPartitionVersion,
			String repairCause,
			GenerationRow generation,
			Long activeGenerationId,
			Long activeGenerationVersion
	) {
	}
}
