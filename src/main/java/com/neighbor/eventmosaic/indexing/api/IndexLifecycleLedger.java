package com.neighbor.eventmosaic.indexing.api;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Транзакционная граница PostgreSQL state logical partitions и generations. */
public interface IndexLifecycleLedger {

	/**
	 * Идемпотентно регистрирует logical partition без current generation.
	 *
	 * @param definition exact P7D key и boundaries
	 * @return созданная или существующая partition
	 */
	IndexPartition registerPartition(IndexPartitionDefinition definition);

	/**
	 * Атомарно захватывает partition и создает единственную BUILDING generation.
	 *
	 * @param partitionKey logical partition
	 * @param type initial promotion либо rebuild
	 * @param names exact physical names нового поколения
	 * @param leaseDuration положительный срок ownership
	 * @return operation либо empty при конфликтующем state
	 */
	Optional<IndexMaintenanceOperation> startMaintenance(
			String partitionKey,
			IndexMaintenanceType type,
			IndexGenerationNames names,
			Duration leaseDuration
	);

	/**
	 * Условно сохраняет один или оба exact Elasticsearch UUID BUILDING generation.
	 *
	 * @return APPLIED либо OWNERSHIP_LOST для stale token/version
	 */
	IndexLifecycleTransitionResult recordGenerationUuids(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String eventIndexUuid,
			String mentionIndexUuid
	);

	/**
	 * Условно продвигает resumable operation на одну допустимую фазу.
	 *
	 * @return APPLIED либо OWNERSHIP_LOST для stale phase/token/version
	 */
	IndexLifecycleTransitionResult advancePhase(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			IndexMaintenancePhase expectedPhase,
			IndexMaintenancePhase nextPhase
	);

	/** Условно завершает initial promotion и делает BUILDING generation ACTIVE. */
	IndexLifecycleTransitionResult completeInitialActivation(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion
	);

	/** Условно завершает observed cutover одной атомарной SQL transaction. */
	IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion
	);

	/** Возвращает незавершенную operation для recovery после restart. */
	Optional<IndexMaintenanceOperation> findRecoverableOperation(String partitionKey);

	/** Возвращает current logical partition state. */
	Optional<IndexPartition> findPartition(String partitionKey);

	/** Возвращает generations logical partition в порядке generation number. */
	List<IndexGeneration> findGenerations(String partitionKey);
}
