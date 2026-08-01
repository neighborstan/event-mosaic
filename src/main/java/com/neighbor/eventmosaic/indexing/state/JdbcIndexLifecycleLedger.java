package com.neighbor.eventmosaic.indexing.state;

import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Транзакционный facade fenced PostgreSQL lifecycle индексов GDELT. */
@Repository
public class JdbcIndexLifecycleLedger implements IndexLifecycleLedger {

	private static final Pattern INDEX_UUID = Pattern.compile("^[^*?,\\s]{1,128}$");

	private final JdbcIndexLifecycleRepository repository;
	private final Clock clock;

	/**
	 * Создает facade с единым application Clock.
	 *
	 * @param repository JDBC state machine
	 * @param clock UTC source текущего времени
	 */
	public JdbcIndexLifecycleLedger(JdbcIndexLifecycleRepository repository, Clock clock) {
		this.repository = repository;
		this.clock = clock;
	}

	@Override
	@Transactional
	public IndexPartition registerPartition(IndexPartitionDefinition definition) {
		Objects.requireNonNull(definition, "definition must not be null");
		return repository.registerPartition(definition, clock.instant());
	}

	@Override
	@Transactional
	public Optional<IndexMaintenanceOperation> startMaintenance(
			String partitionKey,
			IndexMaintenanceType type,
			IndexGenerationNames names,
			Duration leaseDuration
	) {
		requirePartitionKey(partitionKey);
		Objects.requireNonNull(type, "type must not be null");
		if (type == IndexMaintenanceType.CLEANUP) {
			throw new IllegalArgumentException("Cleanup uses a separate lifecycle operation");
		}
		Objects.requireNonNull(names, "names must not be null");
		requirePositive(leaseDuration);
		return repository.startMaintenance(
				partitionKey,
				type,
				names,
				leaseDuration,
				clock.instant());
	}

	@Override
	@Transactional
	public Optional<IndexMaintenanceOperation> startRebuild(
			IndexRebuildClaim claim,
			Duration leaseDuration
	) {
		Objects.requireNonNull(claim, "claim must not be null");
		requirePositive(leaseDuration);
		return repository.startRebuild(claim, leaseDuration, clock.instant());
	}

	@Override
	@Transactional
	public Optional<IndexMaintenanceOperation> reclaimExpiredMaintenance(
			String partitionKey,
			Duration leaseDuration
	) {
		requirePartitionKey(partitionKey);
		requirePositive(leaseDuration);
		return repository.reclaimExpiredMaintenance(
				partitionKey,
				leaseDuration,
				clock.instant());
	}

	@Override
	@Transactional
	public Optional<IndexMaintenanceOperation> renewMaintenanceLease(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			Duration leaseDuration
	) {
		requireOwnership(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion);
		requirePositive(leaseDuration);
		return repository.renewMaintenanceLease(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				leaseDuration,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult recordGenerationUuids(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String eventIndexUuid,
			String mentionIndexUuid
	) {
		requireOwnership(partitionKey, operationToken, expectedPartitionVersion, expectedOperationVersion);
		if (eventIndexUuid == null && mentionIndexUuid == null) {
			throw new IllegalArgumentException("At least one index UUID must be present");
		}
		requireOptionalIndexUuid(eventIndexUuid, "eventIndexUuid");
		requireOptionalIndexUuid(mentionIndexUuid, "mentionIndexUuid");
		if (eventIndexUuid != null && eventIndexUuid.equals(mentionIndexUuid)) {
			throw new IllegalArgumentException("Event and Mention index UUIDs must differ");
		}
		return repository.recordGenerationUuids(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				eventIndexUuid,
				mentionIndexUuid,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult recordBuildWriteOutcome(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			CleanupBuildWriteOutcome minimumOutcome
	) {
		requireOwnership(partitionKey, operationToken, expectedPartitionVersion, expectedOperationVersion);
		Objects.requireNonNull(minimumOutcome, "minimumOutcome must not be null");
		return repository.recordBuildWriteOutcome(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				minimumOutcome,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult advancePhase(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			IndexMaintenancePhase expectedPhase,
			IndexMaintenancePhase nextPhase
	) {
		requireOwnership(partitionKey, operationToken, expectedPartitionVersion, expectedOperationVersion);
		Objects.requireNonNull(expectedPhase, "expectedPhase must not be null");
		Objects.requireNonNull(nextPhase, "nextPhase must not be null");
		if (!expectedPhase.canAdvanceTo(nextPhase)) {
			throw new IllegalArgumentException("Unsupported maintenance phase transition");
		}
		return repository.advancePhase(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				expectedPhase,
				nextPhase,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult completeInitialActivation(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion
	) {
		requireOwnership(partitionKey, operationToken, expectedPartitionVersion, expectedOperationVersion);
		return repository.completeInitialActivation(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion
	) {
		requireOwnership(partitionKey, operationToken, expectedPartitionVersion, expectedOperationVersion);
		return repository.completeObservedCutover(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			BaseGenerationDisposition baseDisposition,
			List<ArchiveReceiptBinding> receipts
	) {
		requireOwnership(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion);
		Objects.requireNonNull(baseDisposition, "baseDisposition must not be null");
		List<ArchiveReceiptBinding> immutableReceipts = List.copyOf(
				Objects.requireNonNull(receipts, "receipts must not be null"));
		if (immutableReceipts.isEmpty()) {
			throw new IllegalArgumentException("receipts must not be empty");
		}
		return repository.completeObservedCutover(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				baseDisposition,
				immutableReceipts,
				clock.instant());
	}

	@Override
	@Transactional
	public IndexLifecycleTransitionResult completePreCutoverFailure(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String errorCode
	) {
		requireOwnership(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion);
		if (errorCode == null || !errorCode.matches("[A-Z][A-Z0-9_]{0,63}")) {
			throw new IllegalArgumentException("errorCode has invalid format");
		}
		return repository.completePreCutoverFailure(
				partitionKey,
				operationToken,
				expectedPartitionVersion,
				expectedOperationVersion,
				errorCode,
				clock.instant());
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<IndexMaintenanceOperation> findRecoverableOperation(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.findRecoverableOperation(partitionKey);
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<IndexPartition> findPartition(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.findPartition(partitionKey);
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<ActiveIndexTargets> findActiveTargets(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.findActiveTargets(partitionKey);
	}

	@Override
	@Transactional(readOnly = true)
	public List<IndexGeneration> findGenerations(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.findGenerations(partitionKey);
	}

	private static void requireOwnership(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion
	) {
		requirePartitionKey(partitionKey);
		Objects.requireNonNull(operationToken, "operationToken must not be null");
		if (expectedPartitionVersion < 0) {
			throw new IllegalArgumentException("expectedPartitionVersion must not be negative");
		}
		if (expectedOperationVersion < 0) {
			throw new IllegalArgumentException("expectedOperationVersion must not be negative");
		}
	}

	private static void requirePartitionKey(String partitionKey) {
		Objects.requireNonNull(partitionKey, "partitionKey must not be null");
		if (partitionKey.isBlank()) {
			throw new IllegalArgumentException("partitionKey must not be blank");
		}
	}

	private static void requirePositive(Duration duration) {
		Objects.requireNonNull(duration, "leaseDuration must not be null");
		if (duration.isZero() || duration.isNegative()) {
			throw new IllegalArgumentException("leaseDuration must be positive");
		}
	}

	private static void requireOptionalIndexUuid(String value, String field) {
		if (value != null && !INDEX_UUID.matcher(value).matches()) {
			throw new IllegalArgumentException(field + " must be a bounded exact Elasticsearch UUID");
		}
	}
}
