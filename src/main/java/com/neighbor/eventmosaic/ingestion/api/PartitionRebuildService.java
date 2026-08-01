package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Описывает локальную проверку и подтвержденное перестроение одной partition. */
public interface PartitionRebuildService {

	/** Строит read-only exact plan без mutation PostgreSQL или Elasticsearch. */
	PartitionRebuildPlan inspect(String partitionKey);

	/** Выполняет либо возобновляет только подтвержденный immutable plan. */
	PartitionRebuildResult execute(PartitionRebuildCommand command);

	/** Полный сериализуемый plan, который пользователь сохраняет вне targets. */
	record PartitionRebuildPlan(
			String partitionKey,
			Instant partitionStartAt,
			Instant partitionEndAt,
			IndexRepairCause repairCause,
			long partitionStateVersion,
			long baseGenerationId,
			UUID baseGenerationUuid,
			IndexGenerationNames baseNames,
			String expectedEventIndexUuid,
			String expectedMentionIndexUuid,
			String observedEventIndexUuid,
			boolean observedEventWriteBlocked,
			String observedMentionIndexUuid,
			boolean observedMentionWriteBlocked,
			Set<String> eventAliasMembership,
			Set<String> mentionAliasMembership,
			int targetGenerationNumber,
			IndexGenerationNames targetNames,
			List<ArchivePlan> archives,
			long stagingAvailableDiskBytes,
			long elasticsearchAvailableDiskBytes,
			Instant expiresAt,
			String fingerprint
	) {

		/** Проверяет обязательные exact identities и immutable collections. */
		public PartitionRebuildPlan {
			requireText(partitionKey, "partitionKey");
			Objects.requireNonNull(partitionStartAt, "partitionStartAt must not be null");
			Objects.requireNonNull(partitionEndAt, "partitionEndAt must not be null");
			if (!partitionStartAt.isBefore(partitionEndAt)
					|| partitionStateVersion < 0
					|| baseGenerationId <= 0
					|| targetGenerationNumber <= 0) {
				throw new IllegalArgumentException("partition plan state is invalid");
			}
			Objects.requireNonNull(repairCause, "repairCause must not be null");
			Objects.requireNonNull(baseGenerationUuid, "baseGenerationUuid must not be null");
			Objects.requireNonNull(baseNames, "baseNames must not be null");
			requireExact(expectedEventIndexUuid, "expectedEventIndexUuid");
			requireExact(expectedMentionIndexUuid, "expectedMentionIndexUuid");
			requireNullableExact(observedEventIndexUuid, "observedEventIndexUuid");
			requireNullableExact(observedMentionIndexUuid, "observedMentionIndexUuid");
			if (observedEventIndexUuid == null && observedEventWriteBlocked
					|| observedMentionIndexUuid == null && observedMentionWriteBlocked) {
				throw new IllegalArgumentException("missing index cannot be write-blocked");
			}
			eventAliasMembership = Set.copyOf(Objects.requireNonNull(
					eventAliasMembership, "eventAliasMembership must not be null"));
			mentionAliasMembership = Set.copyOf(Objects.requireNonNull(
					mentionAliasMembership, "mentionAliasMembership must not be null"));
			Objects.requireNonNull(targetNames, "targetNames must not be null");
			archives = List.copyOf(Objects.requireNonNull(archives, "archives must not be null"));
			if (archives.isEmpty()
					|| stagingAvailableDiskBytes < 0
					|| elasticsearchAvailableDiskBytes < 0) {
				throw new IllegalArgumentException("archive set or disk observation is invalid");
			}
			Objects.requireNonNull(expiresAt, "expiresAt must not be null");
			requirePattern(fingerprint, "[0-9a-f]{64}", "fingerprint");
		}
	}

	/** Immutable replay identity и expected receipt одного archive. */
	record ArchivePlan(
			String archiveKey,
			Instant sourceUpdateTime,
			GdeltArchiveKind kind,
			String archiveName,
			long expectedArchiveSizeBytes,
			String expectedArchiveMd5,
			String stagedArchivePath,
			String stagedCsvPath,
			String processingFingerprint,
			ArchiveProcessingStatus processingStatus,
			long processingStateVersion,
			int attemptCount,
			long expectedDocumentCount,
			String expectedIdentityDigest
	) {

		/** Проверяет bounded paths, fingerprints и receipt counters. */
		public ArchivePlan {
			requireText(archiveKey, "archiveKey");
			Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
			Objects.requireNonNull(kind, "kind must not be null");
			requireText(archiveName, "archiveName");
			if (expectedArchiveSizeBytes <= 0
					|| processingStateVersion < 0
					|| attemptCount <= 0
					|| expectedDocumentCount < 0) {
				throw new IllegalArgumentException("archive plan counters are invalid");
			}
			requirePattern(expectedArchiveMd5, "[0-9a-f]{32}", "expectedArchiveMd5");
			requireText(stagedArchivePath, "stagedArchivePath");
			requireText(stagedCsvPath, "stagedCsvPath");
			requirePattern(
					processingFingerprint,
					"[0-9a-f]{64}",
					"processingFingerprint");
			Objects.requireNonNull(processingStatus, "processingStatus must not be null");
			if (processingStatus != ArchiveProcessingStatus.INDEXED
					&& processingStatus != ArchiveProcessingStatus.FAILED) {
				throw new IllegalArgumentException("processingStatus must contain receipt evidence");
			}
			requirePattern(
					expectedIdentityDigest,
					"[0-9a-f]{64}",
					"expectedIdentityDigest");
		}
	}

	/** Подтверждение сохраненного plan с bounded audit полями. */
	record PartitionRebuildCommand(
			PartitionRebuildPlan plan,
			String actor,
			String reasonCode
	) {

		/** Проверяет формат durable audit. */
		public PartitionRebuildCommand {
			Objects.requireNonNull(plan, "plan must not be null");
			requirePattern(actor, "[A-Za-z0-9._-]{1,64}", "actor");
			requirePattern(reasonCode, "[A-Z][A-Z0-9_]{0,63}", "reasonCode");
		}
	}

	/** Typed завершение local command без exception details. */
	record PartitionRebuildResult(
			PartitionRebuildOutcome outcome,
			String partitionKey,
			IndexMaintenancePhase phase,
			UUID operationToken
	) {

		/** Проверяет обязательную operation identity. */
		public PartitionRebuildResult {
			Objects.requireNonNull(outcome, "outcome must not be null");
			requireText(partitionKey, "partitionKey");
			Objects.requireNonNull(phase, "phase must not be null");
			Objects.requireNonNull(operationToken, "operationToken must not be null");
		}
	}

	/** Bounded результаты owning attempt. */
	enum PartitionRebuildOutcome {
		COMPLETED,
		MAINTENANCE_DEFERRED,
		OWNERSHIP_LOST
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
