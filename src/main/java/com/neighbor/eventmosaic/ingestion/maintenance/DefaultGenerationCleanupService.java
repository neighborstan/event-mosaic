package com.neighbor.eventmosaic.ingestion.maintenance;

import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
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
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexCleanupLedger;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ExactIndexAliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ObservedIndex;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupErrorCode;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupRejectedException;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupUnavailableException;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;

/**
 * Координирует явную очистку одной technical generation через durable fencing
 * и повторное наблюдение каждого destructive шага.
 */
@Service
final class DefaultGenerationCleanupService implements GenerationCleanupService {

	private static final String EMPTY_FINGERPRINT = "0".repeat(64);
	private static final String FAILED_ACTIVE_ORIGIN = "REBUILD_BASE_INVALID";
	private static final Pattern EVENT_INDEX = Pattern.compile(
			"^gdelt-events-v1-(p[0-9]{8})-g[0-9]{4,}$");
	private static final Pattern MENTION_INDEX = Pattern.compile(
			"^gdelt-mentions-v1-(p[0-9]{8})-g[0-9]{4,}$");

	private final IndexCleanupLedger cleanupLedger;
	private final IndexLifecycleLedger lifecycleLedger;
	private final IndexMaintenanceGateway elasticsearch;
	private final IngestionArchiveLedger archiveLedger;
	private final ArchiveProcessingLedger processingLedger;
	private final ZipArchiveStager archiveStager;
	private final BackendDataProperties properties;
	private final Clock clock;

	/** Создает cleanup coordinator из transactional и external exact adapters. */
	DefaultGenerationCleanupService(
			IndexCleanupLedger cleanupLedger,
			IndexLifecycleLedger lifecycleLedger,
			IndexMaintenanceGateway elasticsearch,
			IngestionArchiveLedger archiveLedger,
			ArchiveProcessingLedger processingLedger,
			ZipArchiveStager archiveStager,
			BackendDataProperties properties,
			Clock clock
	) {
		this.cleanupLedger = Objects.requireNonNull(
				cleanupLedger, "cleanupLedger must not be null");
		this.lifecycleLedger = Objects.requireNonNull(
				lifecycleLedger, "lifecycleLedger must not be null");
		this.elasticsearch = Objects.requireNonNull(
				elasticsearch, "elasticsearch must not be null");
		this.archiveLedger = Objects.requireNonNull(
				archiveLedger, "archiveLedger must not be null");
		this.processingLedger = Objects.requireNonNull(
				processingLedger, "processingLedger must not be null");
		this.archiveStager = Objects.requireNonNull(
				archiveStager, "archiveStager must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	@Override
	public GenerationCleanupPlan inspect(String partitionKey, UUID generationUuid) {
		try {
			Instant expiresAt = clock.instant().plus(properties.cleanup().planTtl());
			return inspectAt(partitionKey, generationUuid, expiresAt, true).plan();
		}
		catch (GenerationCleanupRejectedException
				| GenerationCleanupUnavailableException exception) {
			throw exception;
		}
		catch (ApplicationException | DataAccessException exception) {
			throw unavailable(exception);
		}
	}

	@Override
	public GenerationCleanupResult execute(GenerationCleanupCommand command) {
		Objects.requireNonNull(command, "command must not be null");
		try {
			GenerationCleanupPlan plan = command.plan();
			validateFingerprint(plan);
			Optional<CleanupOperation> existing = cleanupLedger.findRecoverable(
					plan.partitionKey());
			CleanupOperation operation;
			if (existing.isEmpty()) {
				operation = claim(command);
			}
			else {
				operation = resume(command, existing.orElseThrow());
				if (operation == null) {
					CleanupOperation open = existing.orElseThrow();
					return result(
							GenerationCleanupOutcome.MAINTENANCE_DEFERRED,
							plan,
							open.phase(),
							open.operationToken());
				}
			}
			return runOwned(plan, operation);
		}
		catch (CleanupOwnershipLostException exception) {
			GenerationCleanupPlan plan = command.plan();
			return result(
					GenerationCleanupOutcome.OWNERSHIP_LOST,
					plan,
					exception.phase,
					exception.operationToken);
		}
		catch (GenerationCleanupRejectedException
				| GenerationCleanupUnavailableException exception) {
			throw exception;
		}
		catch (ApplicationException | DataAccessException exception) {
			throw unavailable(exception);
		}
	}

	private InspectedCleanup inspectAt(
			String partitionKey,
			UUID generationUuid,
			Instant expiresAt,
			boolean rejectOpenMaintenance
	) {
		requirePartitionKey(partitionKey);
		Objects.requireNonNull(generationUuid, "generationUuid must not be null");
		IndexPartition partition = lifecycleLedger.findPartition(partitionKey)
				.orElseThrow(() -> failure(GenerationCleanupErrorCode.UNKNOWN_PARTITION));
		CleanupCandidateSnapshot candidate = cleanupLedger.findCandidates(
				partitionKey,
				properties.cleanup().orphanBuildingAge(),
				properties.cleanup().supersededAge()).stream()
				.filter(item -> item.generationUuid().equals(generationUuid))
				.findFirst()
				.orElseThrow(() -> failure(
						GenerationCleanupErrorCode.CLEANUP_CANDIDATE_NOT_FOUND));
		if (candidate.partitionVersion() != partition.stateVersion()) {
			throw failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		}
		if (rejectOpenMaintenance
				&& cleanupLedger.hasOpenMaintenance(partitionKey)
				&& candidate.status() != IndexGenerationStatus.BUILDING) {
			throw new GenerationCleanupUnavailableException(
					GenerationCleanupErrorCode.CLEANUP_MAINTENANCE_BUSY);
		}
		Optional<ExactIndexAliasMembership> event = elasticsearch
				.observeAllAliasesForExactIndex(candidate.names().eventIndexName());
		Optional<ExactIndexAliasMembership> mention = elasticsearch
				.observeAllAliasesForExactIndex(candidate.names().mentionIndexName());
		long observedStoreBytes = Math.addExact(
				event.map(value -> elasticsearch.exactIndexStoreBytes(
						new ExactIndexTarget(value.indexName(), value.indexUuid())))
						.orElse(0L),
				mention.map(value -> elasticsearch.exactIndexStoreBytes(
						new ExactIndexTarget(value.indexName(), value.indexUuid())))
						.orElse(0L));
		List<ReplaySourcePlan> replaySources = loadReplaySources(partition, true);
		ProtectedActiveEvidence protectedActive = protectedActive(candidate.protectedActive());
		List<ReceiptPlan> currentReceipts = candidate.status()
				== IndexGenerationStatus.SUPERSEDED
				? loadCurrentReceiptPlans(
						partition,
						candidate.protectedActive(),
						replaySources,
						true)
				: List.of();
		GenerationCleanupPlan material = new GenerationCleanupPlan(
				partitionKey,
				partition.definition().startAt(),
				partition.definition().endAt(),
				candidate.partitionVersion(),
				candidate.generationId(),
				candidate.generationUuid(),
				Math.toIntExact(candidate.generationNumber()),
				candidate.status(),
				candidate.generationVersion(),
				candidate.names(),
				candidate.eventIndexUuid(),
				candidate.mentionIndexUuid(),
				candidate.failureOrigin(),
				GenerationWriteOutcome.valueOf(candidate.buildWriteOutcome().name()),
				candidate.repairOpen(),
				candidate.activeProcessing(),
				candidate.heartbeatAt(),
				candidateSince(candidate),
				ownerEvidence(candidate.orphanOwner()),
				event.map(ExactIndexAliasMembership::indexUuid).orElse(null),
				event.map(ExactIndexAliasMembership::aliases).orElseGet(Set::of),
				mention.map(ExactIndexAliasMembership::indexUuid).orElse(null),
				mention.map(ExactIndexAliasMembership::aliases).orElseGet(Set::of),
				observedStoreBytes,
				protectedActive,
				replaySources,
				currentReceipts,
				expiresAt,
				EMPTY_FINGERPRINT);
		GenerationCleanupPlan plan = copyWithFingerprint(
				material,
				GenerationCleanupFingerprint.calculate(material));
		return new InspectedCleanup(candidate, plan);
	}

	private CleanupOperation claim(GenerationCleanupCommand command) {
		GenerationCleanupPlan plan = command.plan();
		if (!plan.expiresAt().isAfter(clock.instant())) {
			throw failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		}
		InspectedCleanup current;
		try {
			current = inspectAt(
					plan.partitionKey(),
					plan.generationUuid(),
					plan.expiresAt(),
					false);
		}
		catch (GenerationCleanupRejectedException
				| GenerationCleanupUnavailableException exception) {
			if (exception.errorCode()
					== GenerationCleanupErrorCode.CLEANUP_OPERATION_UNAVAILABLE) {
				throw exception;
			}
			throw failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		}
		if (!current.plan().fingerprint().equals(plan.fingerprint())) {
			throw failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		}
		validateExecuteEligibility(current.plan());
		CleanupClaim claim = new CleanupClaim(
				current.candidate(),
				plan.fingerprint(),
				plan.expiresAt(),
				command.actor(),
				command.reasonCode(),
				true,
				plan.generationStatus() != IndexGenerationStatus.SUPERSEDED
						|| !plan.currentReceipts().isEmpty(),
				true);
		return cleanupLedger.claim(claim, properties.cleanup().ownershipLease())
				.orElseThrow(() -> failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN));
	}

	private CleanupOperation resume(
			GenerationCleanupCommand command,
			CleanupOperation existing
	) {
		GenerationCleanupPlan plan = command.plan();
		Long protectedActiveId = plan.protectedActive() == null
				? null
				: plan.protectedActive().generationId();
		Long protectedActiveVersion = plan.protectedActive() == null
				? null
				: plan.protectedActive().generationStateVersion();
		if (!existing.planFingerprint().equals(plan.fingerprint())
				|| existing.generationId() != plan.generationId()
				|| !existing.planExpiresAt().equals(plan.expiresAt())
				|| !existing.actor().equals(command.actor())
				|| !existing.reasonCode().equals(command.reasonCode())
				|| !Objects.equals(
						existing.protectedActiveGenerationId(), protectedActiveId)
				|| !Objects.equals(
						existing.protectedActiveGenerationVersion(), protectedActiveVersion)) {
			throw new GenerationCleanupUnavailableException(
					GenerationCleanupErrorCode.CLEANUP_MAINTENANCE_BUSY);
		}
		if (existing.leaseExpiresAt().isAfter(clock.instant())) {
			return null;
		}
		return cleanupLedger.resume(
				plan.partitionKey(),
				plan.fingerprint(),
				properties.cleanup().ownershipLease()).orElse(null);
	}

	private GenerationCleanupResult runOwned(
			GenerationCleanupPlan plan,
			CleanupOperation operation
	) {
		CleanupOwner owner = new CleanupOwner(operation);
		while (true) {
			switch (owner.operation().phase()) {
				case CLEANUP_PENDING -> requestDelete(plan, owner);
				case DELETE_REQUESTED -> {
					deleteAndComplete(plan, owner);
					return result(
							GenerationCleanupOutcome.COMPLETED,
							plan,
							IndexMaintenancePhase.COMPLETED,
							owner.operation().operationToken());
				}
				default -> throw new GenerationCleanupUnavailableException(
						GenerationCleanupErrorCode.CLEANUP_OWNERSHIP_LOST);
			}
		}
	}

	private void requestDelete(GenerationCleanupPlan plan, CleanupOwner owner) {
		owner.renew();
		verifyReplaySources(plan, owner);
		boolean receiptsConfirmed = plan.generationStatus()
				!= IndexGenerationStatus.SUPERSEDED
				|| verifyCurrentReceipts(plan, owner);
		ExactEvidence exact = observeDeletableExactPair(plan);
		owner.renew();
		CleanupTransitionResult transition = cleanupLedger.requestDelete(
				CleanupOwnership.from(owner.operation()),
				new CleanupDeleteEvidence(
						plan.generationNames(),
						plan.expectedEventIndexUuid(),
						plan.expectedMentionIndexUuid(),
						exact.aliasesAbsent(),
						receiptsConfirmed,
						true));
		if (transition != CleanupTransitionResult.APPLIED) {
			owner.lost();
		}
		owner.reload(IndexMaintenancePhase.DELETE_REQUESTED);
	}

	private void deleteAndComplete(GenerationCleanupPlan plan, CleanupOwner owner) {
		owner.renew();
		deleteTarget(
				plan.generationNames().eventIndexName(),
				plan.expectedEventIndexUuid());
		owner.renew();
		deleteTarget(
				plan.generationNames().mentionIndexName(),
				plan.expectedMentionIndexUuid());
		owner.renew();
		boolean eventAbsent = requireAbsent(
				plan.generationNames().eventIndexName(),
				plan.expectedEventIndexUuid());
		boolean mentionAbsent = requireAbsent(
				plan.generationNames().mentionIndexName(),
				plan.expectedMentionIndexUuid());
		if (!eventAbsent || !mentionAbsent) {
			throw new GenerationCleanupUnavailableException(
					GenerationCleanupErrorCode.CLEANUP_OPERATION_UNAVAILABLE);
		}
		CleanupTransitionResult transition = cleanupLedger.complete(
				CleanupOwnership.from(owner.operation()),
				new CleanupCompletionEvidence(
						plan.generationNames(),
						plan.expectedEventIndexUuid(),
						plan.expectedMentionIndexUuid(),
						true,
						true));
		if (transition != CleanupTransitionResult.APPLIED) {
			owner.lost();
		}
	}

	private void deleteTarget(String indexName, String expectedIndexUuid) {
		Optional<ExactIndexAliasMembership> observed = elasticsearch
				.observeAllAliasesForExactIndex(indexName);
		if (observed.isEmpty()) {
			return;
		}
		ExactIndexAliasMembership present = observed.orElseThrow();
		if (expectedIndexUuid == null
				|| !expectedIndexUuid.equals(present.indexUuid())) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_EXACT_TARGET_CONFLICT);
		}
		if (!present.aliases().isEmpty()) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_ALIAS_CONFLICT);
		}
		elasticsearch.deleteExactIndex(new ExactIndexTarget(indexName, expectedIndexUuid));
	}

	private boolean requireAbsent(String indexName, String expectedIndexUuid) {
		Optional<ExactIndexAliasMembership> observed = elasticsearch
				.observeAllAliasesForExactIndex(indexName);
		if (observed.isEmpty()) {
			return true;
		}
		ExactIndexAliasMembership present = observed.orElseThrow();
		if (expectedIndexUuid == null
				|| !expectedIndexUuid.equals(present.indexUuid())) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_EXACT_TARGET_CONFLICT);
		}
		if (!present.aliases().isEmpty()) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_ALIAS_CONFLICT);
		}
		return false;
	}

	private void validateExecuteEligibility(GenerationCleanupPlan plan) {
		if (plan.repairOpen() || plan.activeProcessing()) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_NOT_ELIGIBLE);
		}
		ExactEvidence exact = validateExactEvidence(plan);
		if (!exact.aliasesAbsent()) {
			GenerationCleanupErrorCode code = plan.generationStatus()
					== IndexGenerationStatus.BUILDING
					? GenerationCleanupErrorCode.CLEANUP_RECONCILIATION_REQUIRED
					: GenerationCleanupErrorCode.CLEANUP_ALIAS_CONFLICT;
			throw failure(code);
		}
		if (plan.generationStatus() == IndexGenerationStatus.BUILDING) {
			if (plan.writeOutcome() != GenerationWriteOutcome.NONE
					|| plan.ownerEvidence() == null
					|| !directOrphanCleanupIsSafe(plan.ownerEvidence())) {
				throw failure(GenerationCleanupErrorCode.CLEANUP_RECONCILIATION_REQUIRED);
			}
		}
		if (plan.generationStatus() == IndexGenerationStatus.FAILED
				&& (plan.writeOutcome() != GenerationWriteOutcome.NONE
					|| FAILED_ACTIVE_ORIGIN.equals(plan.failureOrigin()))) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_NOT_ELIGIBLE);
		}
		if (plan.generationStatus() == IndexGenerationStatus.SUPERSEDED
				&& (plan.protectedActive() == null || plan.currentReceipts().isEmpty())) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
	}

	private ExactEvidence validateExactEvidence(GenerationCleanupPlan plan) {
		validateObservedUuid(
				plan.expectedEventIndexUuid(),
				plan.observedEventIndexUuid());
		validateObservedUuid(
				plan.expectedMentionIndexUuid(),
				plan.observedMentionIndexUuid());
		return new ExactEvidence(
				plan.eventAliases().isEmpty() && plan.mentionAliases().isEmpty());
	}

	private ExactEvidence observeDeletableExactPair(GenerationCleanupPlan plan) {
		Optional<ExactIndexAliasMembership> event = elasticsearch
				.observeAllAliasesForExactIndex(plan.generationNames().eventIndexName());
		Optional<ExactIndexAliasMembership> mention = elasticsearch
				.observeAllAliasesForExactIndex(plan.generationNames().mentionIndexName());
		validateObservedUuid(
				plan.expectedEventIndexUuid(),
				event.map(ExactIndexAliasMembership::indexUuid).orElse(null));
		validateObservedUuid(
				plan.expectedMentionIndexUuid(),
				mention.map(ExactIndexAliasMembership::indexUuid).orElse(null));
		boolean aliasesAbsent = event.map(ExactIndexAliasMembership::aliases)
				.orElseGet(Set::of).isEmpty()
				&& mention.map(ExactIndexAliasMembership::aliases)
						.orElseGet(Set::of).isEmpty();
		if (!aliasesAbsent) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_ALIAS_CONFLICT);
		}
		return new ExactEvidence(true);
	}

	private static void validateObservedUuid(String expected, String observed) {
		if (observed != null && !observed.equals(expected)) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_EXACT_TARGET_CONFLICT);
		}
	}

	private List<ReplaySourcePlan> loadReplaySources(
			IndexPartition partition,
			boolean verify
	) {
		List<IngestionArchiveState> states = archiveLedger.findStagedBetween(
				partition.definition().startAt(),
				partition.definition().endAt());
		if (states.isEmpty()) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_SOURCE_INVALID);
		}
		List<ReplaySourcePlan> result = new ArrayList<>(states.size());
		Set<String> keys = new HashSet<>();
		for (IngestionArchiveState state : states) {
			if (state.status() != IngestionArchiveStatus.STAGED
					|| state.stagedArchive() == null
					|| !keys.add(state.archive().idempotencyKey())) {
				throw failure(GenerationCleanupErrorCode.CLEANUP_SOURCE_INVALID);
			}
			if (verify) {
				verifyReplaySource(state);
			}
			result.add(new ReplaySourcePlan(
					state.archive().idempotencyKey(),
					state.archive().sourceUpdateTime(),
					state.archive().archiveName(),
					state.archive().expectedSizeBytes(),
					state.archive().expectedMd5(),
					state.stagedArchive().archivePath().toString(),
					state.stagedArchive().csvPath().toString()));
		}
		result.sort(Comparator
				.comparing(ReplaySourcePlan::sourceUpdateTime)
				.thenComparing(ReplaySourcePlan::archiveKey));
		return List.copyOf(result);
	}

	private void verifyReplaySources(GenerationCleanupPlan plan, CleanupOwner owner) {
		IndexPartition partition = requirePartition(plan.partitionKey());
		List<ReplaySourcePlan> current = loadReplaySources(partition, false);
		if (!current.equals(plan.replaySources())) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_SOURCE_INVALID);
		}
		for (ReplaySourcePlan source : plan.replaySources()) {
			owner.renew();
			IngestionArchiveState state = archiveLedger.findByIdempotencyKey(
					source.archiveKey()).orElseThrow(() -> failure(
						GenerationCleanupErrorCode.CLEANUP_SOURCE_INVALID));
			verifyReplaySource(state);
		}
	}

	private void verifyReplaySource(IngestionArchiveState state) {
		try {
			archiveStager.verifyReplaySource(
					state,
					OperationBudget.start(properties.operationDeadline()));
		}
		catch (ApplicationException exception) {
			throw new GenerationCleanupRejectedException(
					GenerationCleanupErrorCode.CLEANUP_SOURCE_INVALID,
					exception);
		}
	}

	private List<ReceiptPlan> loadCurrentReceiptPlans(
			IndexPartition partition,
			CleanupProtectedActive active,
			List<ReplaySourcePlan> sources,
			boolean verifyLive
	) {
		if (active == null
				|| partition.activeGenerationId() == null
				|| partition.activeGenerationId() != active.generationId()
				|| partition.repairCause() != null) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
		IndexGeneration generation = lifecycleLedger.findGenerations(
				partition.definition().partitionKey()).stream()
				.filter(item -> item.id() == active.generationId())
				.filter(item -> item.status() == IndexGenerationStatus.ACTIVE)
				.findFirst()
				.orElseThrow(() -> failure(
						GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));
		if (!matches(active, generation)) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
		requireProtectedActiveTargets(active);
		requireProtectedActiveAliases(partition.definition().partitionKey(), active);
		Map<String, IngestionArchiveState> archives = archiveLedger.findStagedBetween(
				partition.definition().startAt(),
				partition.definition().endAt()).stream()
				.collect(Collectors.toMap(
						state -> state.archive().idempotencyKey(),
						Function.identity()));
		Map<String, ArchiveProcessingState> processing = processingLedger
				.findByPartition(partition.definition().partitionKey()).stream()
				.collect(Collectors.toMap(
						ArchiveProcessingState::archiveIdempotencyKey,
						Function.identity()));
		Set<String> sourceKeys = sources.stream()
				.map(ReplaySourcePlan::archiveKey)
				.collect(Collectors.toSet());
		if (!archives.keySet().equals(sourceKeys)
				|| !processing.keySet().equals(sourceKeys)) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
		List<ReceiptPlan> result = new ArrayList<>(sources.size());
		for (ReplaySourcePlan source : sources) {
			IngestionArchiveState archive = archives.get(source.archiveKey());
			ArchiveProcessingState state = processing.get(source.archiveKey());
			GdeltIndexKind kind = toIndexKind(archive.archive().archiveType());
			ExactIndexTarget target = exactTarget(active, kind);
			if (!healthyReceiptState(
					state,
					partition.definition().partitionKey(),
					active,
					kind,
					target)) {
				throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
			}
			ArchiveProcessingReceipt receipt = state.receipt();
			ReceiptPlan plan = new ReceiptPlan(
					source.archiveKey(),
					state.fingerprint().processingFingerprint(),
					state.stateVersion(),
					state.attempt().count(),
					kind,
					receipt.expectedDocumentCount(),
					receipt.expectedIdentityDigest());
			if (verifyLive && !verifyReceipt(plan, target).matched()) {
				throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
			}
			result.add(plan);
		}
		result.sort(Comparator
				.comparing(ReceiptPlan::archiveKey)
				.thenComparing(receipt -> receipt.kind().name()));
		return List.copyOf(result);
	}

	private boolean verifyCurrentReceipts(
			GenerationCleanupPlan plan,
			CleanupOwner owner
	) {
		owner.renew();
		IndexPartition partition = requirePartition(plan.partitionKey());
		CleanupProtectedActive active = toCleanupProtectedActive(plan.protectedActive());
		List<ReceiptPlan> current = loadCurrentReceiptPlans(
				partition,
				active,
				plan.replaySources(),
				true);
		if (!current.equals(plan.currentReceipts())) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
		return true;
	}

	private ArchiveReceiptVerification verifyReceipt(
			ReceiptPlan receipt,
			ExactIndexTarget target
	) {
		return elasticsearch.verifyReceipt(new ArchiveReceiptQuery(
				receipt.kind(),
				target,
				receipt.archiveKey(),
				receipt.processingFingerprint(),
				receipt.expectedDocumentCount(),
				new ArchiveIdentityDigest(receipt.expectedIdentityDigest()),
				properties.receiptPageSize()));
	}

	private static boolean healthyReceiptState(
			ArchiveProcessingState state,
			String partitionKey,
			CleanupProtectedActive active,
			GdeltIndexKind kind,
			ExactIndexTarget target
	) {
		if (state == null
				|| state.status() != ArchiveProcessingStatus.INDEXED
				|| state.attempt().token() != null
				|| state.receipt() == null
				|| !state.receipt().matched()
				|| state.failure() != null) {
			return false;
		}
		ArchiveProcessingTargetBinding binding = state.targetBinding();
		return binding != null
				&& binding.indexKind() == kind
				&& binding.partitionKey().equals(partitionKey)
				&& binding.generationId() == active.generationId()
				&& binding.generationUuid().equals(active.generationUuid())
				&& binding.indexName().equals(target.indexName())
				&& binding.indexUuid().equals(target.indexUuid())
				&& state.receipt().verifiedGenerationId() == active.generationId()
				&& state.receipt().verifiedIndexUuid().equals(target.indexUuid());
	}

	private void requireProtectedActiveAliases(
			String partitionKey,
			CleanupProtectedActive active
	) {
		AliasMembership aliases = elasticsearch.readAliases();
		Set<String> eventMembers = partitionMembers(
				aliases.eventIndices(), partitionKey, EVENT_INDEX);
		Set<String> mentionMembers = partitionMembers(
				aliases.mentionIndices(), partitionKey, MENTION_INDEX);
		if (!eventMembers.equals(Set.of(active.names().eventIndexName()))
				|| !mentionMembers.equals(Set.of(active.names().mentionIndexName()))) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
	}

	private void requireProtectedActiveTargets(CleanupProtectedActive active) {
		ObservedIndex event = elasticsearch.observeExactIndex(
				active.names().eventIndexName()).orElseThrow(() -> failure(
						GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));
		ObservedIndex mention = elasticsearch.observeExactIndex(
				active.names().mentionIndexName()).orElseThrow(() -> failure(
						GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH));
		if (!event.indexUuid().equals(active.eventIndexUuid())
				|| event.writeBlocked()
				|| !mention.indexUuid().equals(active.mentionIndexUuid())
				|| mention.writeBlocked()) {
			throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
		}
	}

	private static Set<String> partitionMembers(
			Set<String> indices,
			String partitionKey,
			Pattern pattern
	) {
		Set<String> members = new HashSet<>();
		for (String index : indices) {
			Matcher matcher = pattern.matcher(index);
			if (!matcher.matches()) {
				throw failure(GenerationCleanupErrorCode.CLEANUP_RECEIPT_MISMATCH);
			}
			if (partitionKey.equals(matcher.group(1))) {
				members.add(index);
			}
		}
		return Set.copyOf(members);
	}

	private static boolean matches(
			CleanupProtectedActive active,
			IndexGeneration generation
	) {
		return generation.generationUuid().equals(active.generationUuid())
				&& generation.stateVersion() == active.generationVersion()
				&& generation.names().equals(active.names())
				&& Objects.equals(generation.eventIndexUuid(), active.eventIndexUuid())
				&& Objects.equals(generation.mentionIndexUuid(), active.mentionIndexUuid());
	}

	private static ExactIndexTarget exactTarget(
			CleanupProtectedActive active,
			GdeltIndexKind kind
	) {
		return kind == GdeltIndexKind.EVENT
				? new ExactIndexTarget(
						active.names().eventIndexName(), active.eventIndexUuid())
				: new ExactIndexTarget(
						active.names().mentionIndexName(), active.mentionIndexUuid());
	}

	private IndexPartition requirePartition(String partitionKey) {
		return lifecycleLedger.findPartition(partitionKey)
				.orElseThrow(() -> failure(GenerationCleanupErrorCode.UNKNOWN_PARTITION));
	}

	private static GdeltIndexKind toIndexKind(ArchiveType archiveType) {
		return switch (archiveType) {
			case TRANSLATION_EVENTS -> GdeltIndexKind.EVENT;
			case TRANSLATION_MENTIONS -> GdeltIndexKind.MENTION;
		};
	}

	private static OwnerEvidence ownerEvidence(CleanupOrphanOwner owner) {
		return owner == null
				? null
				: new OwnerEvidence(
						owner.operationId(),
						owner.type(),
						owner.operationToken(),
						owner.phase(),
						owner.operationVersion(),
						owner.leaseExpiresAt(),
						owner.heartbeatAt());
	}

	private static ProtectedActiveEvidence protectedActive(CleanupProtectedActive active) {
		return active == null
				? null
				: new ProtectedActiveEvidence(
						active.generationId(),
						active.generationUuid(),
						active.generationVersion(),
						active.names(),
						active.eventIndexUuid(),
						active.mentionIndexUuid());
	}

	private static CleanupProtectedActive toCleanupProtectedActive(
			ProtectedActiveEvidence active
	) {
		return active == null
				? null
				: new CleanupProtectedActive(
						active.generationId(),
						active.generationUuid(),
						active.generationStateVersion(),
						active.names(),
						active.eventIndexUuid(),
						active.mentionIndexUuid());
	}

	private static Instant candidateSince(CleanupCandidateSnapshot candidate) {
		return switch (candidate.status()) {
			case BUILDING -> candidate.heartbeatAt();
			case FAILED -> Objects.requireNonNull(
					candidate.failedAt(), "failedAt must not be null");
			case SUPERSEDED -> Objects.requireNonNull(
					candidate.supersededAt(), "supersededAt must not be null");
			default -> throw new IllegalArgumentException("Unsupported cleanup status");
		};
	}

	private static boolean directOrphanCleanupIsSafe(OwnerEvidence owner) {
		return switch (owner.type()) {
			case INITIAL_PROMOTION -> owner.phase() == IndexMaintenancePhase.PLANNED
					|| owner.phase() == IndexMaintenancePhase.BUILDING;
			case REBUILD -> owner.phase() == IndexMaintenancePhase.PLANNED;
			case CLEANUP -> false;
		};
	}

	private static GenerationCleanupPlan copyWithFingerprint(
			GenerationCleanupPlan plan,
			String fingerprint
	) {
		return new GenerationCleanupPlan(
				plan.partitionKey(),
				plan.partitionStartAt(),
				plan.partitionEndAt(),
				plan.partitionStateVersion(),
				plan.generationId(),
				plan.generationUuid(),
				plan.generationNumber(),
				plan.generationStatus(),
				plan.generationStateVersion(),
				plan.generationNames(),
				plan.expectedEventIndexUuid(),
				plan.expectedMentionIndexUuid(),
				plan.failureOrigin(),
				plan.writeOutcome(),
				plan.repairOpen(),
				plan.activeProcessing(),
				plan.generationHeartbeatAt(),
				plan.candidateSinceAt(),
				plan.ownerEvidence(),
				plan.observedEventIndexUuid(),
				plan.eventAliases(),
				plan.observedMentionIndexUuid(),
				plan.mentionAliases(),
				plan.observedStoreBytes(),
				plan.protectedActive(),
				plan.replaySources(),
				plan.currentReceipts(),
				plan.expiresAt(),
				fingerprint);
	}

	private static void validateFingerprint(GenerationCleanupPlan plan) {
		if (!GenerationCleanupFingerprint.calculate(plan).equals(plan.fingerprint())) {
			throw failure(GenerationCleanupErrorCode.STALE_CLEANUP_PLAN);
		}
	}

	private static GenerationCleanupResult result(
			GenerationCleanupOutcome outcome,
			GenerationCleanupPlan plan,
			IndexMaintenancePhase phase,
			UUID operationToken
	) {
		return new GenerationCleanupResult(
				outcome,
				plan.partitionKey(),
				plan.generationUuid(),
				phase,
				operationToken);
	}

	private static void requirePartitionKey(String partitionKey) {
		if (partitionKey == null || !partitionKey.matches("p[0-9]{8}")) {
			throw new IllegalArgumentException("partitionKey must be an exact P7D key");
		}
	}

	private static GenerationCleanupRejectedException failure(
			GenerationCleanupErrorCode code
	) {
		return new GenerationCleanupRejectedException(code);
	}

	private static GenerationCleanupUnavailableException unavailable(
			RuntimeException cause
	) {
		return new GenerationCleanupUnavailableException(
				GenerationCleanupErrorCode.CLEANUP_OPERATION_UNAVAILABLE,
				cause);
	}

	private record InspectedCleanup(
			CleanupCandidateSnapshot candidate,
			GenerationCleanupPlan plan
	) {
	}

	private record ExactEvidence(boolean aliasesAbsent) {
	}

	private final class CleanupOwner {

		private CleanupOperation operation;

		private CleanupOwner(CleanupOperation operation) {
			this.operation = Objects.requireNonNull(operation, "operation must not be null");
		}

		private CleanupOperation operation() {
			return operation;
		}

		private void renew() {
			operation = cleanupLedger.renew(
					CleanupOwnership.from(operation),
					properties.cleanup().ownershipLease()).orElseThrow(() ->
						new CleanupOwnershipLostException(
								operation.phase(), operation.operationToken()));
		}

		private void reload(IndexMaintenancePhase expectedPhase) {
			CleanupOperation current = cleanupLedger.findRecoverable(
					operation.partitionKey()).orElseThrow(() ->
						new CleanupOwnershipLostException(
								operation.phase(), operation.operationToken()));
			if (!current.operationToken().equals(operation.operationToken())
					|| !current.planFingerprint().equals(operation.planFingerprint())
					|| current.phase() != expectedPhase) {
				lost();
			}
			operation = current;
		}

		private void lost() {
			throw new CleanupOwnershipLostException(
					operation.phase(), operation.operationToken());
		}
	}

	private static final class CleanupOwnershipLostException extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private final IndexMaintenancePhase phase;
		private final UUID operationToken;

		private CleanupOwnershipLostException(
				IndexMaintenancePhase phase,
				UUID operationToken
		) {
			this.phase = phase;
			this.operationToken = operationToken;
		}
	}
}
