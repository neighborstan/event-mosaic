package com.neighbor.eventmosaic.ingestion.maintenance;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.ArchiveReceiptBinding;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.BaseGenerationDisposition;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger.IndexRebuildClaim;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasCutover;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.AliasMembership;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceGateway.ObservedIndex;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolution;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolver;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import com.neighbor.eventmosaic.indexing.api.IndexWriteMode;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildErrorCode;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildException;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.staging.ZipArchiveStager;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

/** Координирует восстанавливаемое полное перестроение partition без удаления indices. */
@Service
final class DefaultPartitionRebuildService implements PartitionRebuildService {

	private static final String EMPTY_FINGERPRINT = "0".repeat(64);
	private static final String PRE_CUTOVER_FAILURE = "PARTITION_REBUILD_PRE_CUTOVER_FAILED";
	private static final Pattern EVENT_INDEX = Pattern.compile(
			"^gdelt-events-v[0-9]+-(p[0-9]{8})-g[0-9]+$");
	private static final Pattern MENTION_INDEX = Pattern.compile(
			"^gdelt-mentions-v[0-9]+-(p[0-9]{8})-g[0-9]+$");

	private final IndexLifecycleLedger lifecycleLedger;
	private final IndexPartitionGenerationResolver generationResolver;
	private final IndexMaintenanceGateway elasticsearch;
	private final IngestionArchiveLedger archiveLedger;
	private final ArchiveProcessingLedger processingLedger;
	private final ZipArchiveStager archiveStager;
	private final GdeltArchiveProcessor archiveProcessor;
	private final BackendDataProperties properties;
	private final Clock clock;

	DefaultPartitionRebuildService(
			IndexLifecycleLedger lifecycleLedger,
			IndexPartitionGenerationResolver generationResolver,
			IndexMaintenanceGateway elasticsearch,
			IngestionArchiveLedger archiveLedger,
			ArchiveProcessingLedger processingLedger,
			ZipArchiveStager archiveStager,
			GdeltArchiveProcessor archiveProcessor,
			BackendDataProperties properties,
			Clock clock
	) {
		this.lifecycleLedger = Objects.requireNonNull(
				lifecycleLedger, "lifecycleLedger must not be null");
		this.generationResolver = Objects.requireNonNull(
				generationResolver, "generationResolver must not be null");
		this.elasticsearch = Objects.requireNonNull(
				elasticsearch, "elasticsearch must not be null");
		this.archiveLedger = Objects.requireNonNull(
				archiveLedger, "archiveLedger must not be null");
		this.processingLedger = Objects.requireNonNull(
				processingLedger, "processingLedger must not be null");
		this.archiveStager = Objects.requireNonNull(
				archiveStager, "archiveStager must not be null");
		this.archiveProcessor = Objects.requireNonNull(
				archiveProcessor, "archiveProcessor must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	@Override
	public PartitionRebuildPlan inspect(String partitionKey) {
		try {
			Instant expiresAt = clock.instant().plus(properties.rebuild().planTtl());
			return inspectAt(partitionKey, expiresAt, true);
		}
		catch (PartitionRebuildException exception) {
			throw exception;
		}
		catch (RuntimeException exception) {
			throw unavailable(exception);
		}
	}

	@Override
	public PartitionRebuildResult execute(PartitionRebuildCommand command) {
		Objects.requireNonNull(command, "command must not be null");
		try {
			PartitionRebuildPlan plan = command.plan();
			validatePlanFingerprint(plan);
			Optional<IndexMaintenanceOperation> existing =
					lifecycleLedger.findRecoverableOperation(plan.partitionKey());
			IndexMaintenanceOperation operation;
			if (existing.isEmpty()) {
				operation = start(command);
			}
			else {
				operation = reclaim(command, existing.orElseThrow());
				if (operation == null) {
					IndexMaintenanceOperation open = existing.orElseThrow();
					return new PartitionRebuildResult(
							PartitionRebuildOutcome.MAINTENANCE_DEFERRED,
							plan.partitionKey(),
							open.phase(),
							open.token());
				}
			}
			return runOwned(command, operation);
		}
		catch (PartitionRebuildException exception) {
			throw exception;
		}
		catch (RuntimeException exception) {
			throw unavailable(exception);
		}
	}

	private PartitionRebuildPlan inspectAt(
			String partitionKey,
			Instant expiresAt,
			boolean rejectOpenMaintenance
	) {
		requirePartitionKey(partitionKey);
		if (rejectOpenMaintenance
				&& lifecycleLedger.findRecoverableOperation(partitionKey).isPresent()) {
			throw failure(PartitionRebuildErrorCode.REBUILD_MAINTENANCE_BUSY);
		}
		IndexPartition partition = lifecycleLedger.findPartition(partitionKey)
				.orElseThrow(() -> failure(PartitionRebuildErrorCode.UNKNOWN_PARTITION));
		IndexGeneration base = findBaseGeneration(partition);
		Optional<ObservedIndex> observedEvent = elasticsearch.observeExactIndex(
				base.names().eventIndexName());
		Optional<ObservedIndex> observedMention = elasticsearch.observeExactIndex(
				base.names().mentionIndexName());
		AliasMembership aliases = elasticsearch.readAliases();
		Set<String> eventMembership = partitionMembers(
				aliases.eventIndices(),
				partitionKey,
				EVENT_INDEX);
		Set<String> mentionMembership = partitionMembers(
				aliases.mentionIndices(),
				partitionKey,
				MENTION_INDEX);
		requireNoUnexpectedMembers(base, eventMembership, mentionMembership);
		List<ArchivePlan> archives = loadArchivePlans(partition);
		IndexRepairCause repairCause = determineRepairCause(
				base,
				observedEvent,
				observedMention,
				eventMembership,
				mentionMembership,
				archives);
		int nextGenerationNumber = lifecycleLedger.findGenerations(partitionKey).stream()
				.mapToInt(IndexGeneration::generationNumber)
				.max()
				.orElseThrow() + 1;
		IndexPartitionGenerationResolution target = generationResolver.resolve(
				partition.definition().startAt(),
				nextGenerationNumber);
		if (!target.partition().partitionKey().equals(partitionKey)) {
			throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
		}
		long stagingAvailable = minimumStagingAvailableBytes(archives);
		long elasticsearchAvailable = elasticsearch.minimumAvailableDiskBytes();
		PartitionRebuildPlan material = new PartitionRebuildPlan(
				partitionKey,
				partition.definition().startAt(),
				partition.definition().endAt(),
				repairCause,
				partition.stateVersion(),
				base.id(),
				base.generationUuid(),
				base.names(),
				base.eventIndexUuid(),
				base.mentionIndexUuid(),
				observedEvent.map(ObservedIndex::indexUuid).orElse(null),
				observedEvent.map(ObservedIndex::writeBlocked).orElse(false),
				observedMention.map(ObservedIndex::indexUuid).orElse(null),
				observedMention.map(ObservedIndex::writeBlocked).orElse(false),
				eventMembership,
				mentionMembership,
				nextGenerationNumber,
				target.names(),
				archives,
				stagingAvailable,
				elasticsearchAvailable,
				expiresAt,
				EMPTY_FINGERPRINT);
		String fingerprint = PartitionRebuildFingerprint.calculate(material);
		return copyWithFingerprint(material, fingerprint);
	}

	private IndexGeneration findBaseGeneration(IndexPartition partition) {
		Long activeGenerationId = partition.activeGenerationId();
		if (activeGenerationId == null) {
			throw failure(PartitionRebuildErrorCode.REBUILD_NOT_REQUIRED);
		}
		return lifecycleLedger.findGenerations(partition.definition().partitionKey()).stream()
				.filter(generation -> generation.id() == activeGenerationId)
				.filter(generation -> generation.status() == IndexGenerationStatus.ACTIVE)
				.findFirst()
				.orElseThrow(() -> failure(
						PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT));
	}

	private List<ArchivePlan> loadArchivePlans(IndexPartition partition) {
		List<IngestionArchiveState> archives = archiveLedger.findStagedBetween(
				partition.definition().startAt(),
				partition.definition().endAt());
		Map<String, ArchiveProcessingState> processingByKey = processingLedger
				.findByPartition(partition.definition().partitionKey()).stream()
				.collect(Collectors.toMap(
						ArchiveProcessingState::archiveIdempotencyKey,
						Function.identity()));
		if (archives.isEmpty()) {
			throw failure(PartitionRebuildErrorCode.AUTHORITATIVE_SET_INCOMPLETE);
		}
		List<ArchivePlan> result = new ArrayList<>(archives.size());
		Set<String> archiveKeys = new HashSet<>();
		for (IngestionArchiveState archive : archives) {
			String archiveKey = archive.archive().idempotencyKey();
			archiveKeys.add(archiveKey);
			ArchiveProcessingState processing = processingByKey.get(archiveKey);
			if (processing == null
					|| processing.attempt().token() != null
					|| processing.receipt() == null
					|| processing.receipt().verifiedGenerationId()
							!= partition.activeGenerationId()
					|| processing.status() != ArchiveProcessingStatus.INDEXED
							&& processing.status() != ArchiveProcessingStatus.FAILED
					|| archive.stagedArchive() == null) {
				throw failure(PartitionRebuildErrorCode.AUTHORITATIVE_SET_INCOMPLETE);
			}
			ArchiveProcessingReceipt receipt = processing.receipt();
			result.add(new ArchivePlan(
					archiveKey,
					archive.archive().sourceUpdateTime(),
					toGdeltKind(archive.archive().archiveType()),
					archive.archive().archiveName(),
					archive.archive().expectedSizeBytes(),
					archive.archive().expectedMd5(),
					archive.stagedArchive().archivePath().toString(),
					archive.stagedArchive().csvPath().toString(),
					processing.fingerprint().processingFingerprint(),
					processing.status(),
					processing.stateVersion(),
					processing.attempt().count(),
					receipt.expectedDocumentCount(),
					receipt.expectedIdentityDigest()));
		}
		if (!archiveKeys.equals(processingByKey.keySet())) {
			throw failure(PartitionRebuildErrorCode.AUTHORITATIVE_SET_INCOMPLETE);
		}
		result.sort(Comparator
				.comparing(ArchivePlan::sourceUpdateTime)
				.thenComparing(archive -> archive.kind().name())
				.thenComparing(ArchivePlan::archiveKey));
		return List.copyOf(result);
	}

	private IndexRepairCause determineRepairCause(
			IndexGeneration base,
			Optional<ObservedIndex> observedEvent,
			Optional<ObservedIndex> observedMention,
			Set<String> eventMembership,
			Set<String> mentionMembership,
			List<ArchivePlan> archives
	) {
		boolean missing = observedEvent.isEmpty()
				|| observedMention.isEmpty()
				|| !eventMembership.contains(base.names().eventIndexName())
				|| !mentionMembership.contains(base.names().mentionIndexName());
		boolean corrupt = observedEvent.isPresent()
				&& !base.eventIndexUuid().equals(observedEvent.orElseThrow().indexUuid())
				|| observedMention.isPresent()
				&& !base.mentionIndexUuid().equals(observedMention.orElseThrow().indexUuid());
		boolean surplus = false;
		boolean receiptCorrupt = false;
		if (!missing && !corrupt) {
			for (ArchivePlan archive : archives) {
				ArchiveReceiptVerification verification = verifyReceipt(
						archive,
						baseTarget(base, archive.kind()));
				surplus |= verification.status() == ArchiveReceiptStatus.SURPLUS;
				receiptCorrupt |= verification.status() == ArchiveReceiptStatus.SHORTAGE
						|| verification.status() == ArchiveReceiptStatus.IDENTITY_MISMATCH;
			}
		}
		for (ArchivePlan archive : archives) {
			if (archive.processingStatus() == ArchiveProcessingStatus.FAILED) {
				ArchiveProcessingState state = processingLedger
						.findByArchiveIdempotencyKey(archive.archiveKey())
						.orElseThrow(() -> failure(
								PartitionRebuildErrorCode.AUTHORITATIVE_SET_INCOMPLETE));
				String code = state.failure() == null
						? null
						: state.failure().failure().errorCode();
				surplus |= "INDEX_RECEIPT_SURPLUS".equals(code);
				receiptCorrupt |= "INDEX_RECEIPT_MISMATCH".equals(code);
			}
		}
		if (missing) {
			return IndexRepairCause.MISSING_CURRENT;
		}
		if (corrupt || receiptCorrupt) {
			return IndexRepairCause.CORRUPT_CURRENT;
		}
		if (surplus) {
			return IndexRepairCause.SURPLUS;
		}
		throw failure(PartitionRebuildErrorCode.REBUILD_NOT_REQUIRED);
	}

	private IndexMaintenanceOperation start(PartitionRebuildCommand command) {
		PartitionRebuildPlan plan = command.plan();
		if (!plan.expiresAt().isAfter(clock.instant())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
		PartitionRebuildPlan current = inspectAt(
				plan.partitionKey(),
				plan.expiresAt(),
				false);
		if (!current.fingerprint().equals(plan.fingerprint())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
		requireDiskCapacity(current);
		IndexRebuildClaim claim = new IndexRebuildClaim(
				plan.partitionKey(),
				plan.partitionStateVersion(),
				plan.baseGenerationId(),
				plan.baseGenerationUuid(),
				plan.repairCause(),
				plan.targetNames(),
				plan.fingerprint(),
				plan.expiresAt(),
				command.actor(),
				command.reasonCode());
		return lifecycleLedger.startRebuild(
				claim,
				properties.rebuild().ownershipLease()).orElseThrow(() -> failure(
						PartitionRebuildErrorCode.STALE_REBUILD_PLAN));
	}

	private IndexMaintenanceOperation reclaim(
			PartitionRebuildCommand command,
			IndexMaintenanceOperation operation
	) {
		PartitionRebuildPlan plan = command.plan();
		validateStoredOperation(command, operation);
		if (operation.leaseExpiresAt().isAfter(clock.instant())) {
			return null;
		}
		IndexMaintenanceOperation reclaimed = lifecycleLedger
				.reclaimExpiredMaintenance(
						plan.partitionKey(),
						properties.rebuild().ownershipLease())
				.orElseThrow(() -> failure(
						PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST));
		validateStoredOperation(command, reclaimed);
		return reclaimed;
	}

	private void validateStoredOperation(
			PartitionRebuildCommand command,
			IndexMaintenanceOperation operation
	) {
		PartitionRebuildPlan plan = command.plan();
		if (operation.type() != IndexMaintenanceType.REBUILD
				|| !operation.partitionKey().equals(plan.partitionKey())
				|| !Objects.equals(operation.repairCause(), plan.repairCause())
				|| !Objects.equals(operation.planFingerprint(), plan.fingerprint())
				|| !Objects.equals(operation.planExpiresAt(), plan.expiresAt())
				|| !Objects.equals(operation.actor(), command.actor())
				|| !Objects.equals(operation.reasonCode(), command.reasonCode())
				|| !Objects.equals(operation.baseGenerationId(), plan.baseGenerationId())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
		IndexGeneration target = findGeneration(
				plan.partitionKey(),
				operation.buildingGenerationId());
		if (!target.names().equals(plan.targetNames())) {
			throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
		}
	}

	private PartitionRebuildResult runOwned(
			PartitionRebuildCommand command,
			IndexMaintenanceOperation initial
	) {
		MaintenanceOwner owner = new MaintenanceOwner(initial);
		try {
			while (true) {
				switch (owner.operation().phase()) {
					case PLANNED -> owner.advance(IndexMaintenancePhase.FREEZE_REQUESTED);
					case FREEZE_REQUESTED -> {
						elasticsearch.addWriteBlock(survivingOldTargets(command.plan()));
						owner.advance(IndexMaintenancePhase.FROZEN);
					}
					case FROZEN -> {
						freezeAuthoritativeSet(command.plan(), owner);
						owner.advance(IndexMaintenancePhase.BUILDING);
					}
					case BUILDING -> {
						ActiveIndexTargets targets = rebuildShadow(command.plan(), owner);
						verifyAllReceipts(command.plan(), targets);
						owner.advance(IndexMaintenancePhase.VERIFIED);
					}
					case VERIFIED -> {
						owner.renew();
						owner.advance(IndexMaintenancePhase.CUTOVER_REQUESTED);
					}
					case CUTOVER_REQUESTED -> reconcileCutover(command.plan(), owner);
					case CUTOVER_OBSERVED -> {
						return completeCutover(command.plan(), owner);
					}
					case UNFREEZE_REQUESTED -> {
						completeFailedUnfreeze(command.plan(), owner);
						throw failure(PartitionRebuildErrorCode.REBUILD_PROCESSING_FAILED);
					}
					default -> throw failure(
							PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST);
				}
			}
		}
		catch (PartitionRebuildException exception) {
			abortBeforeCutover(command.plan(), owner, exception);
			throw exception;
		}
		catch (RuntimeException exception) {
			PartitionRebuildException translated = unavailable(exception);
			abortBeforeCutover(command.plan(), owner, translated);
			throw translated;
		}
	}

	private void freezeAuthoritativeSet(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner
	) {
		List<ArchivePlan> current = loadArchivePlans(requirePartition(plan));
		if (!current.equals(plan.archives())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
		for (ArchivePlan archive : plan.archives()) {
			owner.renew();
			IngestionArchiveState state = archiveLedger.findByIdempotencyKey(
					archive.archiveKey()).orElseThrow(() -> failure(
							PartitionRebuildErrorCode.REBUILD_SOURCE_INVALID));
			try {
				archiveStager.verifyReplaySource(
						state,
						OperationBudget.start(properties.operationDeadline()));
			}
			catch (RuntimeException exception) {
				throw new PartitionRebuildException(
						PartitionRebuildErrorCode.REBUILD_SOURCE_INVALID,
						exception);
			}
		}
		validateRepairCauseAfterFreeze(plan);
	}

	private void validateRepairCauseAfterFreeze(PartitionRebuildPlan plan) {
		IndexGeneration base = findGeneration(plan.partitionKey(), plan.baseGenerationId());
		List<ExactIndexTarget> surviving = survivingOldTargets(plan);
		for (ExactIndexTarget target : surviving) {
			GdeltIndexKind kind = target.indexName().startsWith("gdelt-events-")
					? GdeltIndexKind.EVENT
					: GdeltIndexKind.MENTION;
			elasticsearch.refreshExact(kind, target);
		}
		Optional<ObservedIndex> event = elasticsearch.observeExactIndex(
				base.names().eventIndexName());
		Optional<ObservedIndex> mention = elasticsearch.observeExactIndex(
				base.names().mentionIndexName());
		AliasMembership aliases = elasticsearch.readAliases();
		Set<String> eventMembers = partitionMembers(
				aliases.eventIndices(), plan.partitionKey(), EVENT_INDEX);
		Set<String> mentionMembers = partitionMembers(
				aliases.mentionIndices(), plan.partitionKey(), MENTION_INDEX);
		requireNoUnexpectedMembers(base, eventMembers, mentionMembers);
		IndexRepairCause current = determineRepairCause(
				base,
				event,
				mention,
				eventMembers,
				mentionMembers,
				plan.archives());
		if (current != plan.repairCause()) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
	}

	private ActiveIndexTargets rebuildShadow(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner
	) {
		elasticsearch.installTemplates();
		IndexGeneration target = findGeneration(
				plan.partitionKey(),
				owner.operation().buildingGenerationId());
		ObservedIndex event = observeOrCreateTarget(
				target.names().eventIndexName(),
				target.eventIndexUuid());
		ObservedIndex mention = observeOrCreateTarget(
				target.names().mentionIndexName(),
				target.mentionIndexUuid());
		if (!event.indexUuid().equals(target.eventIndexUuid())
				|| !mention.indexUuid().equals(target.mentionIndexUuid())) {
			owner.recordGenerationUuids(event.indexUuid(), mention.indexUuid());
			target = findGeneration(
					plan.partitionKey(),
					owner.operation().buildingGenerationId());
		}
		ActiveIndexTargets targets = new ActiveIndexTargets(
				plan.partitionKey(),
				owner.operation().partitionVersion(),
				target.id(),
				target.generationUuid(),
				new ExactIndexTarget(target.names().eventIndexName(), event.indexUuid()),
				new ExactIndexTarget(target.names().mentionIndexName(), mention.indexUuid()));
		for (ArchivePlan archive : plan.archives()) {
			owner.renew();
			ArchiveProcessingResult result = archiveProcessor.process(
					new ArchiveProcessingRequest(
							archive.kind(),
							archive.sourceUpdateTime(),
							archive.archiveKey(),
							archive.processingFingerprint(),
							targets,
							IndexWriteMode.REBUILD,
							Path.of(archive.stagedCsvPath()),
							properties.receiptPageSize(),
							OperationBudget.start(properties.operationDeadline())),
					progress -> owner.stillOwns());
			if (result.outcome() != ArchiveProcessingOutcome.COMPLETED
					|| result.receipt() == null
					|| result.receipt().expectedDocumentCount()
							!= archive.expectedDocumentCount()
					|| !result.receipt().expectedDigest().value().equals(
							archive.expectedIdentityDigest())) {
				throw failure(PartitionRebuildErrorCode.REBUILD_PROCESSING_FAILED);
			}
		}
		return targets;
	}

	private ObservedIndex observeOrCreateTarget(String indexName, String recordedUuid) {
		Optional<ObservedIndex> observed = elasticsearch.observeExactIndex(indexName);
		if (observed.isEmpty()) {
			if (recordedUuid != null) {
				throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
			}
			elasticsearch.createExactIndex(indexName);
			observed = elasticsearch.observeExactIndex(indexName);
		}
		ObservedIndex target = observed.orElseThrow(() -> failure(
				PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT));
		if (recordedUuid != null && !recordedUuid.equals(target.indexUuid())
				|| target.writeBlocked()) {
			throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
		}
		return target;
	}

	private void verifyAllReceipts(
			PartitionRebuildPlan plan,
			ActiveIndexTargets targets
	) {
		elasticsearch.refreshExact(GdeltIndexKind.EVENT, targets.event());
		elasticsearch.refreshExact(GdeltIndexKind.MENTION, targets.mention());
		for (ArchivePlan archive : plan.archives()) {
			ArchiveReceiptVerification verification = verifyReceipt(
					archive,
					targets.target(toIndexKind(archive.kind())));
			if (!verification.matched()) {
				throw failure(PartitionRebuildErrorCode.REBUILD_RECEIPT_MISMATCH);
			}
		}
	}

	private void reconcileCutover(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner
	) {
		IndexGeneration target = findGeneration(
				plan.partitionKey(),
				owner.operation().buildingGenerationId());
		ExactIndexTarget newEvent = exactTarget(target, GdeltIndexKind.EVENT);
		ExactIndexTarget newMention = exactTarget(target, GdeltIndexKind.MENTION);
		for (int attempt = 0; attempt < 3; attempt++) {
			AliasState state = aliasState(plan, target, elasticsearch.readAliases());
			if (state.unexpected() || state.mixed()) {
				throw failure(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT);
			}
			if (state.completeNew()) {
				owner.advance(IndexMaintenancePhase.CUTOVER_OBSERVED);
				return;
			}
			owner.renew();
			try {
				if (state.newOnly()) {
					elasticsearch.addStableAliases(
							newEvent.indexName(),
							!state.eventNew(),
							newMention.indexName(),
							!state.mentionNew());
				}
				else {
					elasticsearch.cutoverAliases(new AliasCutover(
							state.eventOld() ? observedOldTarget(plan, true) : null,
							state.mentionOld() ? observedOldTarget(plan, false) : null,
							newEvent,
							newMention));
				}
			}
			catch (RuntimeException unknownOutcome) {
				AliasState observed = aliasState(plan, target, elasticsearch.readAliases());
				if (observed.completeNew()) {
					owner.advance(IndexMaintenancePhase.CUTOVER_OBSERVED);
					return;
				}
				if (observed.unexpected() || observed.mixed()) {
					throw failure(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT);
				}
				if (!observed.newOnly() || attempt == 2) {
					throw unknownOutcome;
				}
			}
		}
		throw failure(PartitionRebuildErrorCode.REBUILD_OPERATION_UNAVAILABLE);
	}

	private PartitionRebuildResult completeCutover(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner
	) {
		IndexGeneration target = findGeneration(
				plan.partitionKey(),
				owner.operation().buildingGenerationId());
		ActiveIndexTargets targets = activeTargets(owner, target);
		verifyAllReceipts(plan, targets);
		BaseGenerationDisposition disposition = validRollbackCopy(plan)
				? BaseGenerationDisposition.SUPERSEDED
				: BaseGenerationDisposition.FAILED;
		AliasState finalAliases = aliasState(
				plan,
				target,
				elasticsearch.readAliases());
		if (!finalAliases.completeNew()) {
			throw failure(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT);
		}
		owner.renew();
		IndexLifecycleTransitionResult result = lifecycleLedger.completeObservedCutover(
				plan.partitionKey(),
				owner.operation().token(),
				owner.operation().partitionVersion(),
				owner.operation().operationVersion(),
				disposition,
				plan.archives().stream().map(DefaultPartitionRebuildService::toReceiptBinding)
						.toList());
		if (result != IndexLifecycleTransitionResult.APPLIED) {
			throw failure(PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST);
		}
		return new PartitionRebuildResult(
				PartitionRebuildOutcome.COMPLETED,
				plan.partitionKey(),
				IndexMaintenancePhase.COMPLETED,
				owner.operation().token());
	}

	private boolean validRollbackCopy(PartitionRebuildPlan plan) {
		if (!Objects.equals(plan.expectedEventIndexUuid(), plan.observedEventIndexUuid())
				|| !Objects.equals(
						plan.expectedMentionIndexUuid(),
						plan.observedMentionIndexUuid())) {
			return false;
		}
		ExactIndexTarget event = observedOldTarget(plan, true);
		ExactIndexTarget mention = observedOldTarget(plan, false);
		Optional<ObservedIndex> observedEvent = elasticsearch.observeExactIndex(
				event.indexName());
		Optional<ObservedIndex> observedMention = elasticsearch.observeExactIndex(
				mention.indexName());
		if (observedEvent.isEmpty()
				|| observedMention.isEmpty()
				|| !event.indexUuid().equals(observedEvent.orElseThrow().indexUuid())
				|| !mention.indexUuid().equals(observedMention.orElseThrow().indexUuid())
				|| !observedEvent.orElseThrow().writeBlocked()
				|| !observedMention.orElseThrow().writeBlocked()) {
			return false;
		}
		for (ArchivePlan archive : plan.archives()) {
			ExactIndexTarget target = archive.kind() == GdeltArchiveKind.TRANSLATION_EVENTS
					? event
					: mention;
			if (!verifyReceipt(archive, target).matched()) {
				return false;
			}
		}
		return true;
	}

	private void abortBeforeCutover(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner,
			PartitionRebuildException original
	) {
		IndexMaintenancePhase phase = owner.operation().phase();
		if (phase != IndexMaintenancePhase.FROZEN
				&& phase != IndexMaintenancePhase.BUILDING
				&& phase != IndexMaintenancePhase.VERIFIED) {
			return;
		}
		try {
			owner.advance(IndexMaintenancePhase.UNFREEZE_REQUESTED);
			completeFailedUnfreeze(plan, owner);
		}
		catch (RuntimeException cleanupFailure) {
			original.addSuppressed(cleanupFailure);
		}
	}

	private void completeFailedUnfreeze(
			PartitionRebuildPlan plan,
			MaintenanceOwner owner
	) {
		List<ExactIndexTarget> oldTargets = plannedOldTargets(plan);
		elasticsearch.removeWriteBlock(oldTargets);
		for (ExactIndexTarget target : oldTargets) {
			Optional<ObservedIndex> observed = elasticsearch.observeExactIndex(
					target.indexName());
			if (observed.isPresent()
					&& (!target.indexUuid().equals(observed.orElseThrow().indexUuid())
						|| observed.orElseThrow().writeBlocked())) {
				throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
			}
		}
		IndexLifecycleTransitionResult failed = lifecycleLedger.completePreCutoverFailure(
				plan.partitionKey(),
				owner.operation().token(),
				owner.operation().partitionVersion(),
				owner.operation().operationVersion(),
				PRE_CUTOVER_FAILURE);
		if (failed != IndexLifecycleTransitionResult.APPLIED) {
			throw failure(PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST);
		}
	}

	private List<ExactIndexTarget> survivingOldTargets(PartitionRebuildPlan plan) {
		List<ExactIndexTarget> targets = plannedOldTargets(plan);
		for (ExactIndexTarget target : targets) {
			Optional<ObservedIndex> observed = elasticsearch.observeExactIndex(
					target.indexName());
			if (observed.isEmpty()
					|| !target.indexUuid().equals(observed.orElseThrow().indexUuid())) {
				throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
			}
		}
		return targets;
	}

	private static List<ExactIndexTarget> plannedOldTargets(PartitionRebuildPlan plan) {
		List<ExactIndexTarget> targets = new ArrayList<>(2);
		addSurvivingTarget(
				targets,
				plan.baseNames().eventIndexName(),
				plan.observedEventIndexUuid());
		addSurvivingTarget(
				targets,
				plan.baseNames().mentionIndexName(),
				plan.observedMentionIndexUuid());
		return List.copyOf(targets);
	}

	private static void addSurvivingTarget(
			List<ExactIndexTarget> targets,
			String indexName,
			String observedUuid
	) {
		if (observedUuid != null) {
			targets.add(new ExactIndexTarget(indexName, observedUuid));
		}
	}

	private static ExactIndexTarget observedOldTarget(
			PartitionRebuildPlan plan,
			boolean event
	) {
		String name = event
				? plan.baseNames().eventIndexName()
				: plan.baseNames().mentionIndexName();
		String uuid = event
				? plan.observedEventIndexUuid()
				: plan.observedMentionIndexUuid();
		if (uuid == null) {
			throw failure(PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT);
		}
		return new ExactIndexTarget(name, uuid);
	}

	private AliasState aliasState(
			PartitionRebuildPlan plan,
			IndexGeneration target,
			AliasMembership membership
	) {
		Set<String> eventMembers = partitionMembers(
				membership.eventIndices(), plan.partitionKey(), EVENT_INDEX);
		Set<String> mentionMembers = partitionMembers(
				membership.mentionIndices(), plan.partitionKey(), MENTION_INDEX);
		String oldEvent = plan.baseNames().eventIndexName();
		String oldMention = plan.baseNames().mentionIndexName();
		String newEvent = target.names().eventIndexName();
		String newMention = target.names().mentionIndexName();
		boolean unexpected = eventMembers.stream().anyMatch(
				name -> !name.equals(oldEvent) && !name.equals(newEvent))
				|| mentionMembers.stream().anyMatch(
						name -> !name.equals(oldMention) && !name.equals(newMention));
		return new AliasState(
				eventMembers.contains(oldEvent),
				mentionMembers.contains(oldMention),
				eventMembers.contains(newEvent),
				mentionMembers.contains(newMention),
				unexpected,
				eventMembers.size(),
				mentionMembers.size());
	}

	private void requireDiskCapacity(PartitionRebuildPlan plan) {
		long stagingAvailable = minimumStagingAvailableBytes(plan.archives());
		long elasticsearchAvailable = elasticsearch.minimumAvailableDiskBytes();
		if (stagingAvailable < properties.diskPressure().stagingMinFreeBytes()
				|| elasticsearchAvailable
						< properties.diskPressure().elasticsearchMinFreeBytes()) {
			throw failure(PartitionRebuildErrorCode.REBUILD_DISK_PRESSURE);
		}
	}

	private static long minimumStagingAvailableBytes(List<ArchivePlan> archives) {
		long available = Long.MAX_VALUE;
		try {
			for (ArchivePlan archive : archives) {
				Path path = Path.of(archive.stagedArchivePath());
				available = Math.min(available, Files.getFileStore(path).getUsableSpace());
			}
		}
		catch (IOException | RuntimeException exception) {
			throw new PartitionRebuildException(
					PartitionRebuildErrorCode.REBUILD_SOURCE_INVALID,
					exception);
		}
		if (available == Long.MAX_VALUE) {
			throw failure(PartitionRebuildErrorCode.AUTHORITATIVE_SET_INCOMPLETE);
		}
		return available;
	}

	private ArchiveReceiptVerification verifyReceipt(
			ArchivePlan archive,
			ExactIndexTarget target
	) {
		return elasticsearch.verifyReceipt(new ArchiveReceiptQuery(
				toIndexKind(archive.kind()),
				target,
				archive.archiveKey(),
				archive.processingFingerprint(),
				archive.expectedDocumentCount(),
				new ArchiveIdentityDigest(archive.expectedIdentityDigest()),
				properties.receiptPageSize()));
	}

	private static ExactIndexTarget baseTarget(
			IndexGeneration base,
			GdeltArchiveKind kind
	) {
		return kind == GdeltArchiveKind.TRANSLATION_EVENTS
				? new ExactIndexTarget(base.names().eventIndexName(), base.eventIndexUuid())
				: new ExactIndexTarget(base.names().mentionIndexName(), base.mentionIndexUuid());
	}

	private static ExactIndexTarget exactTarget(
			IndexGeneration generation,
			GdeltIndexKind kind
	) {
		return kind == GdeltIndexKind.EVENT
				? new ExactIndexTarget(
						generation.names().eventIndexName(),
						generation.eventIndexUuid())
				: new ExactIndexTarget(
						generation.names().mentionIndexName(),
						generation.mentionIndexUuid());
	}

	private ActiveIndexTargets activeTargets(
			MaintenanceOwner owner,
			IndexGeneration target
	) {
		return new ActiveIndexTargets(
				owner.operation().partitionKey(),
				owner.operation().partitionVersion(),
				target.id(),
				target.generationUuid(),
				exactTarget(target, GdeltIndexKind.EVENT),
				exactTarget(target, GdeltIndexKind.MENTION));
	}

	private static ArchiveReceiptBinding toReceiptBinding(ArchivePlan archive) {
		return new ArchiveReceiptBinding(
				archive.archiveKey(),
				archive.processingFingerprint(),
				archive.processingStateVersion(),
				archive.attemptCount(),
				toIndexKind(archive.kind()),
				archive.expectedDocumentCount(),
				new ArchiveIdentityDigest(archive.expectedIdentityDigest()));
	}

	private IndexPartition requirePartition(PartitionRebuildPlan plan) {
		IndexPartition partition = lifecycleLedger.findPartition(plan.partitionKey())
				.orElseThrow(() -> failure(PartitionRebuildErrorCode.UNKNOWN_PARTITION));
		if (partition.stateVersion() != plan.partitionStateVersion() + 1
				|| !Objects.equals(partition.activeGenerationId(), plan.baseGenerationId())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
		return partition;
	}

	private IndexGeneration findGeneration(String partitionKey, long generationId) {
		return lifecycleLedger.findGenerations(partitionKey).stream()
				.filter(generation -> generation.id() == generationId)
				.findFirst()
				.orElseThrow(() -> failure(
						PartitionRebuildErrorCode.REBUILD_EXACT_TARGET_CONFLICT));
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
				throw failure(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT);
			}
			if (partitionKey.equals(matcher.group(1))) {
				members.add(index);
			}
		}
		return Set.copyOf(members);
	}

	private static void requireNoUnexpectedMembers(
			IndexGeneration base,
			Set<String> eventMembership,
			Set<String> mentionMembership
	) {
		if (eventMembership.stream().anyMatch(
				name -> !name.equals(base.names().eventIndexName()))
				|| mentionMembership.stream().anyMatch(
						name -> !name.equals(base.names().mentionIndexName()))) {
			throw failure(PartitionRebuildErrorCode.REBUILD_ALIAS_CONFLICT);
		}
	}

	private static PartitionRebuildPlan copyWithFingerprint(
			PartitionRebuildPlan plan,
			String fingerprint
	) {
		return new PartitionRebuildPlan(
				plan.partitionKey(),
				plan.partitionStartAt(),
				plan.partitionEndAt(),
				plan.repairCause(),
				plan.partitionStateVersion(),
				plan.baseGenerationId(),
				plan.baseGenerationUuid(),
				plan.baseNames(),
				plan.expectedEventIndexUuid(),
				plan.expectedMentionIndexUuid(),
				plan.observedEventIndexUuid(),
				plan.observedEventWriteBlocked(),
				plan.observedMentionIndexUuid(),
				plan.observedMentionWriteBlocked(),
				plan.eventAliasMembership(),
				plan.mentionAliasMembership(),
				plan.targetGenerationNumber(),
				plan.targetNames(),
				plan.archives(),
				plan.stagingAvailableDiskBytes(),
				plan.elasticsearchAvailableDiskBytes(),
				plan.expiresAt(),
				fingerprint);
	}

	private static void validatePlanFingerprint(PartitionRebuildPlan plan) {
		if (!PartitionRebuildFingerprint.calculate(plan).equals(plan.fingerprint())) {
			throw failure(PartitionRebuildErrorCode.STALE_REBUILD_PLAN);
		}
	}

	private static GdeltArchiveKind toGdeltKind(ArchiveType type) {
		return switch (type) {
			case TRANSLATION_EVENTS -> GdeltArchiveKind.TRANSLATION_EVENTS;
			case TRANSLATION_MENTIONS -> GdeltArchiveKind.TRANSLATION_MENTIONS;
		};
	}

	private static GdeltIndexKind toIndexKind(GdeltArchiveKind kind) {
		return switch (kind) {
			case TRANSLATION_EVENTS -> GdeltIndexKind.EVENT;
			case TRANSLATION_MENTIONS -> GdeltIndexKind.MENTION;
		};
	}

	private static void requirePartitionKey(String partitionKey) {
		if (partitionKey == null || !partitionKey.matches("p[0-9]{8}")) {
			throw new IllegalArgumentException("partitionKey must be an exact P7D key");
		}
	}

	private static PartitionRebuildException failure(PartitionRebuildErrorCode code) {
		return new PartitionRebuildException(code);
	}

	private static PartitionRebuildException unavailable(RuntimeException exception) {
		return new PartitionRebuildException(
				PartitionRebuildErrorCode.REBUILD_OPERATION_UNAVAILABLE,
				exception);
	}

	private final class MaintenanceOwner {

		private IndexMaintenanceOperation operation;

		private MaintenanceOwner(IndexMaintenanceOperation operation) {
			this.operation = operation;
		}

		private IndexMaintenanceOperation operation() {
			return operation;
		}

		private boolean stillOwns() {
			return operation.leaseExpiresAt().isAfter(clock.instant());
		}

		private void renew() {
			operation = lifecycleLedger.renewMaintenanceLease(
					operation.partitionKey(),
					operation.token(),
					operation.partitionVersion(),
					operation.operationVersion(),
					properties.rebuild().ownershipLease()).orElseThrow(() -> failure(
							PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST));
		}

		private void advance(IndexMaintenancePhase nextPhase) {
			IndexLifecycleTransitionResult result = lifecycleLedger.advancePhase(
					operation.partitionKey(),
					operation.token(),
					operation.partitionVersion(),
					operation.operationVersion(),
					operation.phase(),
					nextPhase);
			if (result != IndexLifecycleTransitionResult.APPLIED) {
				throw failure(PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST);
			}
			operation = copyOperation(
					operation,
					nextPhase,
					operation.operationVersion() + 1,
					operation.leaseExpiresAt());
		}

		private void recordGenerationUuids(String eventUuid, String mentionUuid) {
			IndexLifecycleTransitionResult result = lifecycleLedger.recordGenerationUuids(
					operation.partitionKey(),
					operation.token(),
					operation.partitionVersion(),
					operation.operationVersion(),
					eventUuid,
					mentionUuid);
			if (result != IndexLifecycleTransitionResult.APPLIED) {
				throw failure(PartitionRebuildErrorCode.REBUILD_OWNERSHIP_LOST);
			}
			operation = copyOperation(
					operation,
					operation.phase(),
					operation.operationVersion() + 1,
					operation.leaseExpiresAt());
		}
	}

	private static IndexMaintenanceOperation copyOperation(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase phase,
			long operationVersion,
			Instant leaseExpiresAt
	) {
		return new IndexMaintenanceOperation(
				operation.id(),
				operation.partitionKey(),
				operation.type(),
				phase,
				operation.token(),
				leaseExpiresAt,
				operation.partitionVersion(),
				operation.baseGenerationId(),
				operation.buildingGenerationId(),
				operationVersion,
				operation.repairCause(),
				operation.planFingerprint(),
				operation.planExpiresAt(),
				operation.actor(),
				operation.reasonCode());
	}

	private record AliasState(
			boolean eventOld,
			boolean mentionOld,
			boolean eventNew,
			boolean mentionNew,
			boolean unexpected,
			int eventCount,
			int mentionCount
	) {

		private boolean completeNew() {
			return eventNew && mentionNew && eventCount == 1 && mentionCount == 1;
		}

		private boolean mixed() {
			return eventOld && eventNew
					|| mentionOld && mentionNew
					|| eventOld && mentionNew
					|| mentionOld && eventNew;
		}

		private boolean newOnly() {
			return !eventOld
					&& !mentionOld
					&& (eventNew || mentionNew)
					&& eventCount <= 1
					&& mentionCount <= 1;
		}
	}
}
