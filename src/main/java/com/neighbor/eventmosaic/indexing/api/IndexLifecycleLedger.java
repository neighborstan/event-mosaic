package com.neighbor.eventmosaic.indexing.api;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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
	 * Атомарно проверяет подтвержденный plan, закрывает processing claims и
	 * создает единственную BUILDING generation для rebuild.
	 *
	 * @param claim exact plan identity и audit
	 * @param leaseDuration положительный срок ownership
	 * @return operation либо empty при stale plan или competing maintenance
	 */
	Optional<IndexMaintenanceOperation> startRebuild(
			IndexRebuildClaim claim,
			Duration leaseDuration
	);

	/**
	 * Условно передает expired незавершенную operation новому owner без новой generation.
	 *
	 * @return operation с новым token/lease либо empty, если lease еще принадлежит owner
	 */
	Optional<IndexMaintenanceOperation> reclaimExpiredMaintenance(
			String partitionKey,
			Duration leaseDuration
	);

	/** Продлевает lease текущего owner и возвращает обновленную operation. */
	Optional<IndexMaintenanceOperation> renewMaintenanceLease(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
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

	/**
	 * Завершает observed rebuild cutover и атомарно переносит диагностические
	 * receipt bindings на новую ACTIVE generation.
	 */
	IndexLifecycleTransitionResult completeObservedCutover(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			BaseGenerationDisposition baseDisposition,
			List<ArchiveReceiptBinding> receipts
	);

	/**
	 * Закрывает pre-cutover failure после наблюдаемого unfreeze и оставляет
	 * repair cause открытой.
	 */
	IndexLifecycleTransitionResult completePreCutoverFailure(
			String partitionKey,
			UUID operationToken,
			long expectedPartitionVersion,
			long expectedOperationVersion,
			String errorCode
	);

	/** Возвращает незавершенную operation для recovery после restart. */
	Optional<IndexMaintenanceOperation> findRecoverableOperation(String partitionKey);

	/** Возвращает current logical partition state. */
	Optional<IndexPartition> findPartition(String partitionKey);

	/** Одним coherent SQL read возвращает exact ACTIVE generation binding. */
	Optional<ActiveIndexTargets> findActiveTargets(String partitionKey);

	/** Возвращает generations logical partition в порядке generation number. */
	List<IndexGeneration> findGenerations(String partitionKey);

	/** Immutable fingerprinted claim rebuild operation. */
	record IndexRebuildClaim(
			String partitionKey,
			long expectedPartitionVersion,
			long expectedBaseGenerationId,
			UUID expectedBaseGenerationUuid,
			IndexRepairCause repairCause,
			IndexGenerationNames targetNames,
			String planFingerprint,
			Instant planExpiresAt,
			String actor,
			String reasonCode
	) {

		/** Проверяет bounded exact plan identity. */
		public IndexRebuildClaim {
			requireText(partitionKey, "partitionKey");
			if (expectedPartitionVersion < 0 || expectedBaseGenerationId <= 0) {
				throw new IllegalArgumentException("expected state identities are invalid");
			}
			Objects.requireNonNull(
					expectedBaseGenerationUuid,
					"expectedBaseGenerationUuid must not be null");
			Objects.requireNonNull(repairCause, "repairCause must not be null");
			Objects.requireNonNull(targetNames, "targetNames must not be null");
			requirePattern(planFingerprint, "[0-9a-f]{64}", "planFingerprint");
			Objects.requireNonNull(planExpiresAt, "planExpiresAt must not be null");
			requirePattern(actor, "[A-Za-z0-9._-]{1,64}", "actor");
			requirePattern(reasonCode, "[A-Z][A-Z0-9_]{0,63}", "reasonCode");
		}
	}

	/** Immutable CAS evidence одного durable archive receipt. */
	record ArchiveReceiptBinding(
			String archiveKey,
			String processingFingerprint,
			long expectedStateVersion,
			int expectedAttemptCount,
			GdeltIndexKind kind,
			long expectedDocumentCount,
			ArchiveIdentityDigest expectedIdentityDigest
	) {

		/** Проверяет fingerprint, counters и canonical digest. */
		public ArchiveReceiptBinding {
			requireText(archiveKey, "archiveKey");
			requirePattern(
					processingFingerprint,
					"[0-9a-f]{64}",
					"processingFingerprint");
			if (expectedStateVersion < 0
					|| expectedAttemptCount <= 0
					|| expectedDocumentCount < 0) {
				throw new IllegalArgumentException("receipt CAS values are invalid");
			}
			Objects.requireNonNull(kind, "kind must not be null");
			Objects.requireNonNull(
					expectedIdentityDigest,
					"expectedIdentityDigest must not be null");
		}
	}

	/** Итоговая диагностическая судьба прежней ACTIVE generation. */
	enum BaseGenerationDisposition {
		SUPERSEDED,
		FAILED
	}

	private static void requireText(String value, String field) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.isBlank()) {
			throw new IllegalArgumentException(field + " must not be blank");
		}
	}

	private static void requirePattern(String value, String pattern, String field) {
		requireText(value, field);
		if (!value.matches(pattern)) {
			throw new IllegalArgumentException(field + " has invalid format");
		}
	}
}
