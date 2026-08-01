package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Описывает read-only inspect и подтвержденную очистку одной generation. */
public interface GenerationCleanupService {

	/** Строит fingerprinted plan без mutation PostgreSQL или Elasticsearch. */
	GenerationCleanupPlan inspect(String partitionKey, UUID generationUuid);

	/** Выполняет либо возобновляет только сохраненный immutable plan. */
	GenerationCleanupResult execute(GenerationCleanupCommand command);

	/** Полный сериализуемый plan exact generation и ее recovery evidence. */
	record GenerationCleanupPlan(
			String partitionKey,
			Instant partitionStartAt,
			Instant partitionEndAt,
			long partitionStateVersion,
			long generationId,
			UUID generationUuid,
			int generationNumber,
			IndexGenerationStatus generationStatus,
			long generationStateVersion,
			IndexGenerationNames generationNames,
			String expectedEventIndexUuid,
			String expectedMentionIndexUuid,
			String failureOrigin,
			GenerationWriteOutcome writeOutcome,
			boolean repairOpen,
			boolean activeProcessing,
			Instant generationHeartbeatAt,
			Instant candidateSinceAt,
			OwnerEvidence ownerEvidence,
			String observedEventIndexUuid,
			Set<String> eventAliases,
			String observedMentionIndexUuid,
			Set<String> mentionAliases,
			long observedStoreBytes,
			ProtectedActiveEvidence protectedActive,
			List<ReplaySourcePlan> replaySources,
			List<ReceiptPlan> currentReceipts,
			Instant expiresAt,
			String fingerprint
	) {

		/** Проверяет exact identities и сохраняет immutable evidence collections. */
		public GenerationCleanupPlan {
			requireText(partitionKey, "partitionKey");
			Objects.requireNonNull(partitionStartAt, "partitionStartAt must not be null");
			Objects.requireNonNull(partitionEndAt, "partitionEndAt must not be null");
			if (!partitionStartAt.isBefore(partitionEndAt)
					|| partitionStateVersion < 0
					|| generationId <= 0
					|| generationNumber <= 0
					|| generationStateVersion < 0) {
				throw new IllegalArgumentException("cleanup plan state is invalid");
			}
			Objects.requireNonNull(generationUuid, "generationUuid must not be null");
			Objects.requireNonNull(generationStatus, "generationStatus must not be null");
			if (generationStatus != IndexGenerationStatus.FAILED
					&& generationStatus != IndexGenerationStatus.SUPERSEDED
					&& generationStatus != IndexGenerationStatus.BUILDING) {
				throw new IllegalArgumentException("generationStatus is not a cleanup candidate");
			}
			Objects.requireNonNull(generationNames, "generationNames must not be null");
			requireNullableExact(expectedEventIndexUuid, "expectedEventIndexUuid");
			requireNullableExact(expectedMentionIndexUuid, "expectedMentionIndexUuid");
			if (failureOrigin != null) {
				requirePattern(failureOrigin, "[A-Z][A-Z0-9_]{0,63}", "failureOrigin");
			}
			Objects.requireNonNull(writeOutcome, "writeOutcome must not be null");
			Objects.requireNonNull(
					generationHeartbeatAt, "generationHeartbeatAt must not be null");
			Objects.requireNonNull(candidateSinceAt, "candidateSinceAt must not be null");
			requireNullableExact(observedEventIndexUuid, "observedEventIndexUuid");
			requireNullableExact(observedMentionIndexUuid, "observedMentionIndexUuid");
			eventAliases = immutableExactSet(eventAliases, "eventAliases");
			mentionAliases = immutableExactSet(mentionAliases, "mentionAliases");
			if (observedStoreBytes < 0) {
				throw new IllegalArgumentException("observedStoreBytes must not be negative");
			}
			if (observedEventIndexUuid == null && !eventAliases.isEmpty()
					|| observedMentionIndexUuid == null && !mentionAliases.isEmpty()) {
				throw new IllegalArgumentException("absent index cannot contain aliases");
			}
			if (generationStatus == IndexGenerationStatus.BUILDING
					&& ownerEvidence == null) {
				throw new IllegalArgumentException("BUILDING candidate requires owner evidence");
			}
			if (generationStatus == IndexGenerationStatus.SUPERSEDED
					&& protectedActive == null) {
				throw new IllegalArgumentException("SUPERSEDED candidate requires current ACTIVE evidence");
			}
			replaySources = List.copyOf(Objects.requireNonNull(
					replaySources, "replaySources must not be null"));
			if (replaySources.isEmpty()) {
				throw new IllegalArgumentException("replaySources must not be empty");
			}
			currentReceipts = List.copyOf(Objects.requireNonNull(
					currentReceipts, "currentReceipts must not be null"));
			Objects.requireNonNull(expiresAt, "expiresAt must not be null");
			requirePattern(fingerprint, "[0-9a-f]{64}", "fingerprint");
		}
	}

	/** Durable evidence прежней owning operation orphan BUILDING. */
	record OwnerEvidence(
			long operationId,
			IndexMaintenanceType type,
			UUID operationToken,
			IndexMaintenancePhase phase,
			long operationVersion,
			Instant leaseExpiresAt,
			Instant heartbeatAt
	) {

		/** Проверяет captured owner identity и timestamps. */
		public OwnerEvidence {
			if (operationId <= 0 || operationVersion < 0) {
				throw new IllegalArgumentException("owner evidence state is invalid");
			}
			Objects.requireNonNull(type, "type must not be null");
			Objects.requireNonNull(operationToken, "operationToken must not be null");
			Objects.requireNonNull(phase, "phase must not be null");
			Objects.requireNonNull(leaseExpiresAt, "leaseExpiresAt must not be null");
			Objects.requireNonNull(heartbeatAt, "heartbeatAt must not be null");
		}
	}

	/** Exact current ACTIVE, которую SUPERSEDED cleanup обязан сохранить. */
	record ProtectedActiveEvidence(
			long generationId,
			UUID generationUuid,
			long generationStateVersion,
			IndexGenerationNames names,
			String eventIndexUuid,
			String mentionIndexUuid
	) {

		/** Проверяет coherent current generation pair. */
		public ProtectedActiveEvidence {
			if (generationId <= 0 || generationStateVersion < 0) {
				throw new IllegalArgumentException("protected generation state is invalid");
			}
			Objects.requireNonNull(generationUuid, "generationUuid must not be null");
			Objects.requireNonNull(names, "names must not be null");
			requireExact(eventIndexUuid, "eventIndexUuid");
			requireExact(mentionIndexUuid, "mentionIndexUuid");
		}
	}

	/** Immutable verified ZIP/CSV evidence, достаточная для recovery generation. */
	record ReplaySourcePlan(
			String archiveKey,
			Instant sourceUpdateTime,
			String archiveName,
			long expectedArchiveSizeBytes,
			String expectedArchiveMd5,
			String stagedArchivePath,
			String stagedCsvPath
	) {

		/** Проверяет stable source identity и bounded paths. */
		public ReplaySourcePlan {
			requireText(archiveKey, "archiveKey");
			Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
			requireText(archiveName, "archiveName");
			if (expectedArchiveSizeBytes <= 0) {
				throw new IllegalArgumentException("expectedArchiveSizeBytes must be positive");
			}
			requirePattern(expectedArchiveMd5, "[0-9a-f]{32}", "expectedArchiveMd5");
			requireText(stagedArchivePath, "stagedArchivePath");
			requireText(stagedCsvPath, "stagedCsvPath");
		}
	}

	/** Immutable receipt current ACTIVE для SUPERSEDED cleanup guard. */
	record ReceiptPlan(
			String archiveKey,
			String processingFingerprint,
			long processingStateVersion,
			int attemptCount,
			GdeltIndexKind kind,
			long expectedDocumentCount,
			String expectedIdentityDigest
	) {

		/** Проверяет receipt CAS identity и canonical digest. */
		public ReceiptPlan {
			requireText(archiveKey, "archiveKey");
			requirePattern(
					processingFingerprint,
					"[0-9a-f]{64}",
					"processingFingerprint");
			if (processingStateVersion < 0
					|| attemptCount <= 0
					|| expectedDocumentCount < 0) {
				throw new IllegalArgumentException("receipt evidence counters are invalid");
			}
			Objects.requireNonNull(kind, "kind must not be null");
			requirePattern(
					expectedIdentityDigest,
					"[0-9a-f]{64}",
					"expectedIdentityDigest");
		}
	}

	/** Recorded build-write evidence, которая запрещает догадки о пустом build. */
	enum GenerationWriteOutcome {
		NONE,
		PARTIAL,
		COMPLETED,
		UNKNOWN
	}

	/** Подтверждение сохраненного plan с bounded audit полями. */
	record GenerationCleanupCommand(
			GenerationCleanupPlan plan,
			String actor,
			String reasonCode
	) {

		/** Проверяет формат durable audit. */
		public GenerationCleanupCommand {
			Objects.requireNonNull(plan, "plan must not be null");
			requirePattern(actor, "[A-Za-z0-9._-]{1,64}", "actor");
			requirePattern(reasonCode, "[A-Z][A-Z0-9_]{0,63}", "reasonCode");
		}
	}

	/** Typed завершение локальной команды без внешних exception details. */
	record GenerationCleanupResult(
			GenerationCleanupOutcome outcome,
			String partitionKey,
			UUID generationUuid,
			IndexMaintenancePhase phase,
			UUID operationToken
	) {

		/** Проверяет обязательную generation и operation identity. */
		public GenerationCleanupResult {
			Objects.requireNonNull(outcome, "outcome must not be null");
			requireText(partitionKey, "partitionKey");
			Objects.requireNonNull(generationUuid, "generationUuid must not be null");
			Objects.requireNonNull(phase, "phase must not be null");
			Objects.requireNonNull(operationToken, "operationToken must not be null");
		}
	}

	/** Bounded результаты owning cleanup attempt. */
	enum GenerationCleanupOutcome {
		COMPLETED,
		MAINTENANCE_DEFERRED,
		OWNERSHIP_LOST
	}

	private static Set<String> immutableExactSet(Set<String> values, String field) {
		Set<String> result = Set.copyOf(Objects.requireNonNull(
				values, field + " must not be null"));
		result.forEach(value -> requireExact(value, field));
		return result;
	}

	private static void requireNullableExact(String value, String field) {
		if (value != null) {
			requireExact(value, field);
		}
	}

	private static void requireExact(String value, String field) {
		requireText(value, field);
		if (value.indexOf('*') >= 0
				|| value.indexOf('?') >= 0
				|| value.indexOf(',') >= 0
				|| value.chars().anyMatch(Character::isWhitespace)) {
			throw new IllegalArgumentException(field + " must be exact");
		}
	}

	private static void requirePattern(String value, String pattern, String field) {
		requireText(value, field);
		if (!value.matches(pattern)) {
			throw new IllegalArgumentException(field + " has invalid format");
		}
	}

	private static void requireText(String value, String field) {
		Objects.requireNonNull(value, field + " must not be null");
		if (value.isBlank()) {
			throw new IllegalArgumentException(field + " must not be blank");
		}
	}
}
