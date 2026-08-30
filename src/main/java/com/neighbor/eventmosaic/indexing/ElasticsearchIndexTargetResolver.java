package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.neighbor.eventmosaic.indexing.api.ActiveIndexTargets;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolution;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolver;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolution;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolutionStatus;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolver;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/** Recoverable initial-promotion coordinator для exact schema-v1 generations. */
@Component
final class ElasticsearchIndexTargetResolver implements IndexTargetResolver {

	private static final Duration OWNERSHIP_LEASE = Duration.ofMinutes(15);
	private static final String INDEX_NOT_FOUND = "index_not_found_exception";
	private static final String CLUSTER_BLOCK = "cluster_block_exception";
	private static final Pattern EVENT_PHYSICAL_NAME = Pattern.compile(
			"^gdelt-events-v1-(p([0-9]{8}))-g[0-9]{4,}$");
	private static final Pattern MENTION_PHYSICAL_NAME = Pattern.compile(
			"^gdelt-mentions-v1-(p([0-9]{8}))-g[0-9]{4,}$");

	private final IndexPartitionGenerationResolver partitionResolver;
	private final IndexLifecycleLedger lifecycleLedger;
	private final IndexLifecycleElasticsearchGateway elasticsearch;
	private final Clock clock;

	ElasticsearchIndexTargetResolver(
			IndexPartitionGenerationResolver partitionResolver,
			IndexLifecycleLedger lifecycleLedger,
			IndexLifecycleElasticsearchGateway elasticsearch,
			Clock clock
	) {
		this.partitionResolver = Objects.requireNonNull(
				partitionResolver, "partitionResolver must not be null");
		this.lifecycleLedger = Objects.requireNonNull(
				lifecycleLedger, "lifecycleLedger must not be null");
		this.elasticsearch = Objects.requireNonNull(
				elasticsearch, "elasticsearch must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	@Override
	public IndexTargetResolution resolve(Instant sourceUpdateTime) {
		return resolve(sourceUpdateTime, new LifecycleCalls(null));
	}

	@Override
	public IndexTargetResolution resolve(
			Instant sourceUpdateTime,
			OperationBudget budget
	) {
		return resolve(
				sourceUpdateTime,
				new LifecycleCalls(Objects.requireNonNull(
						budget, "budget must not be null")));
	}

	private IndexTargetResolution resolve(
			Instant sourceUpdateTime,
			LifecycleCalls calls
	) {
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		IndexPartitionGenerationResolution firstGeneration = partitionResolver.resolve(
				sourceUpdateTime,
				1);
		String partitionKey = firstGeneration.partition().partitionKey();
		calls.registerPartition(firstGeneration.partition());

		try {
			Optional<IndexMaintenanceOperation> open =
					lifecycleLedger.findRecoverableOperation(partitionKey);
			Optional<ActiveIndexTargets> active = lifecycleLedger.findActiveTargets(partitionKey);
			if (active.isPresent()) {
				if (open.isPresent()) {
					return outcome(IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
				}
				return verifyActive(active.orElseThrow(), calls);
			}

			calls.installTemplates();
			MaintenanceAcquisition acquisition = acquireInitialOperation(
					sourceUpdateTime,
					partitionKey,
					calls);
			if (acquisition.outcome() != null) {
				return acquisition.outcome();
			}
			return runInitialPromotion(acquisition.operation(), calls);
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			if (hasType(exception, INDEX_NOT_FOUND)) {
				return outcome(IndexTargetResolutionStatus.MISSING);
			}
			if (hasType(exception, CLUSTER_BLOCK)) {
				return outcome(IndexTargetResolutionStatus.WRITE_BLOCKED);
			}
			throw classify(exception);
		}
	}

	private MaintenanceAcquisition acquireInitialOperation(
			Instant sourceUpdateTime,
			String partitionKey,
			LifecycleCalls calls
	) throws IOException {
		Optional<IndexMaintenanceOperation> open = lifecycleLedger.findRecoverableOperation(
				partitionKey);
		if (open.isPresent()) {
			IndexMaintenanceOperation existing = open.orElseThrow();
			if (existing.type() != IndexMaintenanceType.INITIAL_PROMOTION) {
				return MaintenanceAcquisition.outcome(
						IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
			}
			if (existing.leaseExpiresAt().isAfter(clock.instant())) {
				return MaintenanceAcquisition.outcome(
						IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
			}
			Optional<IndexMaintenanceOperation> reclaimed =
					calls.reclaimExpiredMaintenance(partitionKey, OWNERSHIP_LEASE);
			if (reclaimed.isEmpty()) {
				return MaintenanceAcquisition.outcome(
						IndexTargetResolutionStatus.OWNERSHIP_LOST);
			}
			return MaintenanceAcquisition.owned(reclaimed.orElseThrow());
		}

		List<IndexGeneration> generations = lifecycleLedger.findGenerations(partitionKey);
		int generationNumber = generations.stream()
				.mapToInt(IndexGeneration::generationNumber)
				.max()
				.orElse(0) + 1;
		IndexPartitionGenerationResolution generation = partitionResolver.resolve(
				sourceUpdateTime,
				generationNumber);
		Optional<IndexMaintenanceOperation> started = calls.startMaintenance(
				partitionKey,
				IndexMaintenanceType.INITIAL_PROMOTION,
				generation.names(),
				OWNERSHIP_LEASE);
		if (started.isPresent()) {
			return MaintenanceAcquisition.owned(started.orElseThrow());
		}
		Optional<ActiveIndexTargets> active = lifecycleLedger.findActiveTargets(partitionKey);
		if (active.isPresent()) {
			return MaintenanceAcquisition.ready(verifyActive(active.orElseThrow(), calls));
		}
		return MaintenanceAcquisition.outcome(
				IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
	}

	private IndexTargetResolution runInitialPromotion(
			IndexMaintenanceOperation initial,
			LifecycleCalls calls
	)
			throws IOException {
		IndexMaintenanceOperation operation = initial;
		while (true) {
			switch (operation.phase()) {
				case PLANNED -> {
					Optional<IndexMaintenanceOperation> advanced = advance(
							operation,
							IndexMaintenancePhase.BUILDING,
							calls);
					if (advanced.isEmpty()) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					operation = advanced.orElseThrow();
				}
				case BUILDING -> {
					BuildingPair pair = ensureBuildingPair(operation, calls);
					if (pair.outcome() != null) {
						return pair.outcome();
					}
					operation = pair.operation();
					Optional<IndexMaintenanceOperation> advanced = advance(
							operation,
							IndexMaintenancePhase.VERIFIED,
							calls);
					if (advanced.isEmpty()) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					operation = advanced.orElseThrow();
				}
				case VERIFIED -> {
					Optional<IndexMaintenanceOperation> advanced = advance(
							operation,
							IndexMaintenancePhase.CUTOVER_REQUESTED,
							calls);
					if (advanced.isEmpty()) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					operation = advanced.orElseThrow();
				}
				case CUTOVER_REQUESTED -> {
					IndexGeneration building = findBuildingGeneration(operation).orElse(null);
					if (building == null || !hasRecordedPair(building)) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					IndexTargetResolution identityOutcome = verifyRecordedBuildingPair(
							building,
							calls);
					if (identityOutcome != null) {
						return identityOutcome;
					}
					IndexTargetResolution aliasOutcome = reconcileInitialAliases(
							building,
							calls);
					if (aliasOutcome != null) {
						return aliasOutcome;
					}
					identityOutcome = verifyRecordedBuildingPair(building, calls);
					if (identityOutcome != null) {
						return identityOutcome;
					}
					Optional<IndexMaintenanceOperation> advanced = advance(
							operation,
							IndexMaintenancePhase.CUTOVER_OBSERVED,
							calls);
					if (advanced.isEmpty()) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					operation = advanced.orElseThrow();
				}
				case CUTOVER_OBSERVED -> {
					IndexLifecycleTransitionResult completed =
							calls.completeInitialActivation(
									operation.partitionKey(),
									operation.token(),
									operation.partitionVersion(),
									operation.operationVersion());
					if (completed != IndexLifecycleTransitionResult.APPLIED) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					Optional<ActiveIndexTargets> active = lifecycleLedger.findActiveTargets(
							operation.partitionKey());
					if (active.isEmpty()) {
						return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
					}
					return verifyActive(active.orElseThrow(), calls);
				}
				default -> {
					return outcome(IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
				}
			}
		}
	}

	private BuildingPair ensureBuildingPair(
			IndexMaintenanceOperation operation,
			LifecycleCalls calls
	)
			throws IOException {
		Optional<IndexGeneration> candidate = findBuildingGeneration(operation);
		if (candidate.isEmpty()) {
			return BuildingPair.outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		}
		IndexGeneration generation = candidate.orElseThrow();
		ObservedPair observed = observeOrCreatePair(generation, calls);
		if (observed.outcome() != null) {
			return BuildingPair.readyOutcome(observed.outcome());
		}
		boolean alreadyRecorded = observed.event().indexUuid().equals(generation.eventIndexUuid())
				&& observed.mention().indexUuid().equals(generation.mentionIndexUuid());
		if (alreadyRecorded) {
			return BuildingPair.owned(operation);
		}
		IndexLifecycleTransitionResult recorded = calls.recordGenerationUuids(
				operation.partitionKey(),
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				observed.event().indexUuid(),
				observed.mention().indexUuid());
		if (recorded != IndexLifecycleTransitionResult.APPLIED) {
			return BuildingPair.outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		}
		return BuildingPair.owned(withOperationVersion(operation));
	}

	private ObservedPair observeOrCreatePair(
			IndexGeneration generation,
			LifecycleCalls calls
	) throws IOException {
		ObservedIndexResult event = observeOrCreate(
				generation.names().eventIndexName(),
				generation.eventIndexUuid(),
				calls);
		if (event.outcome() != null) {
			return ObservedPair.readyOutcome(event.outcome());
		}
		ObservedIndexResult mention = observeOrCreate(
				generation.names().mentionIndexName(),
				generation.mentionIndexUuid(),
				calls);
		if (mention.outcome() != null) {
			return ObservedPair.readyOutcome(mention.outcome());
		}
		return ObservedPair.observed(event.index(), mention.index());
	}

	private ObservedIndexResult observeOrCreate(
			String indexName,
			String recordedUuid,
			LifecycleCalls calls
	)
			throws IOException {
		Optional<ObservedElasticsearchIndex> found = calls.findExactIndex(indexName);
		if (found.isEmpty()) {
			if (recordedUuid != null) {
				return ObservedIndexResult.outcome(IndexTargetResolutionStatus.MISSING);
			}
			calls.createExactIndex(indexName);
			found = calls.findExactIndex(indexName);
			if (found.isEmpty()) {
				return ObservedIndexResult.outcome(IndexTargetResolutionStatus.MISSING);
			}
		}
		ObservedElasticsearchIndex index = found.orElseThrow();
		if (recordedUuid != null && !recordedUuid.equals(index.indexUuid())) {
			return ObservedIndexResult.outcome(IndexTargetResolutionStatus.MISSING);
		}
		if (index.writeBlocked()) {
			return ObservedIndexResult.outcome(IndexTargetResolutionStatus.WRITE_BLOCKED);
		}
		return ObservedIndexResult.observed(index);
	}

	private IndexTargetResolution reconcileInitialAliases(
			IndexGeneration generation,
			LifecycleCalls calls
	)
			throws IOException {
		for (int attempt = 0; attempt < 2; attempt++) {
			AliasPairState state = inspectAliasPair(generation, calls.readStableAliases());
			if (state.unexpected()) {
				return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
			}
			if (state.complete()) {
				return null;
			}
			try {
				calls.addStableAliases(
						generation.names().eventIndexName(),
						!state.eventPresent(),
						generation.names().mentionIndexName(),
						!state.mentionPresent());
			}
			catch (IOException | IndexingAccessException unknownOutcome) {
				AliasPairState observed = inspectAliasPair(
						generation,
						calls.readStableAliases());
				if (observed.unexpected()) {
					return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
				}
				if (observed.complete()) {
					return null;
				}
				if (attempt == 1) {
					throw unknownOutcome;
				}
			}
		}
		AliasPairState finalState = inspectAliasPair(
				generation,
				calls.readStableAliases());
		if (finalState.unexpected()) {
			return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		}
		return finalState.complete()
				? null
				: outcome(IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
	}

	private static AliasPairState inspectAliasPair(
			IndexGeneration generation,
			IndexAliasMembership membership
	) {
		return inspectAliasPair(
				generation.partitionKey(),
				generation.names().eventIndexName(),
				generation.names().mentionIndexName(),
				membership);
	}

	private static AliasPairState inspectAliasPair(
			String partitionKey,
			String eventIndexName,
			String mentionIndexName,
			IndexAliasMembership membership
	) {
		boolean invalidTarget = membership.eventIndices().stream()
				.anyMatch(name -> physicalPartition(name, EVENT_PHYSICAL_NAME).isEmpty())
				|| membership.mentionIndices().stream()
				.anyMatch(name -> physicalPartition(name, MENTION_PHYSICAL_NAME).isEmpty());
		Set<String> eventPartitionTargets = membership.eventIndices().stream()
				.filter(name -> belongsToPartition(name, partitionKey, EVENT_PHYSICAL_NAME))
				.collect(java.util.stream.Collectors.toUnmodifiableSet());
		Set<String> mentionPartitionTargets = membership.mentionIndices().stream()
				.filter(name -> belongsToPartition(name, partitionKey, MENTION_PHYSICAL_NAME))
				.collect(java.util.stream.Collectors.toUnmodifiableSet());
		boolean eventPresent = eventPartitionTargets.contains(eventIndexName);
		boolean mentionPresent = mentionPartitionTargets.contains(mentionIndexName);
		boolean unexpected = invalidTarget || eventPartitionTargets.stream()
				.anyMatch(name -> !name.equals(eventIndexName))
				|| mentionPartitionTargets.stream()
				.anyMatch(name -> !name.equals(mentionIndexName));
		return new AliasPairState(eventPresent, mentionPresent, unexpected);
	}

	private IndexTargetResolution verifyRecordedBuildingPair(
			IndexGeneration generation,
			LifecycleCalls calls
	)
			throws IOException {
		ObservedIndexResult event = observeOrCreate(
				generation.names().eventIndexName(),
				generation.eventIndexUuid(),
				calls);
		if (event.outcome() != null) {
			return event.outcome();
		}
		ObservedIndexResult mention = observeOrCreate(
				generation.names().mentionIndexName(),
				generation.mentionIndexUuid(),
				calls);
		return mention.outcome();
	}

	private IndexTargetResolution verifyActive(
			ActiveIndexTargets active,
			LifecycleCalls calls
	) throws IOException {
		AliasPairState aliases = inspectAliasPair(
				active.partitionKey(),
				active.event().indexName(),
				active.mention().indexName(),
				calls.readStableAliases());
		if (aliases.unexpected()) {
			return outcome(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		}
		if (!aliases.complete()) {
			return outcome(IndexTargetResolutionStatus.MISSING);
		}
		IndexTargetResolution event = verifyExactTarget(active.event(), calls);
		if (event != null) {
			return event;
		}
		IndexTargetResolution mention = verifyExactTarget(active.mention(), calls);
		return mention == null ? IndexTargetResolution.ready(active) : mention;
	}

	private IndexTargetResolution verifyExactTarget(
			ExactIndexTarget target,
			LifecycleCalls calls
	) throws IOException {
		Optional<ObservedElasticsearchIndex> observed = calls.findExactIndex(
				target.indexName());
		if (observed.isEmpty()
				|| !target.indexUuid().equals(observed.orElseThrow().indexUuid())) {
			return outcome(IndexTargetResolutionStatus.MISSING);
		}
		return observed.orElseThrow().writeBlocked()
				? outcome(IndexTargetResolutionStatus.WRITE_BLOCKED)
				: null;
	}

	private Optional<IndexMaintenanceOperation> advance(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase nextPhase,
			LifecycleCalls calls
	) {
		IndexLifecycleTransitionResult result = calls.advancePhase(
				operation.partitionKey(),
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				operation.phase(),
				nextPhase);
		return result == IndexLifecycleTransitionResult.APPLIED
				? Optional.of(withPhase(operation, nextPhase))
				: Optional.empty();
	}

	private Optional<IndexGeneration> findBuildingGeneration(
			IndexMaintenanceOperation operation
	) {
		return lifecycleLedger.findGenerations(operation.partitionKey()).stream()
				.filter(generation -> generation.id() == operation.buildingGenerationId())
				.filter(generation -> generation.status() == IndexGenerationStatus.BUILDING)
				.findFirst();
	}

	private static boolean hasRecordedPair(IndexGeneration generation) {
		return generation.eventIndexUuid() != null && generation.mentionIndexUuid() != null;
	}

	private static boolean belongsToPartition(
			String indexName,
			String partitionKey,
			Pattern physicalName
	) {
		return physicalPartition(indexName, physicalName)
				.filter(partitionKey::equals)
				.isPresent();
	}

	private static Optional<String> physicalPartition(String indexName, Pattern physicalName) {
		var matcher = physicalName.matcher(indexName);
		if (!matcher.matches()) {
			return Optional.empty();
		}
		try {
			LocalDate start = LocalDate.parse(
					matcher.group(2),
					DateTimeFormatter.BASIC_ISO_DATE);
			return start.getDayOfWeek() == DayOfWeek.MONDAY
					? Optional.of(matcher.group(1))
					: Optional.empty();
		}
		catch (DateTimeParseException exception) {
			return Optional.empty();
		}
	}

	private static IndexMaintenanceOperation withPhase(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase phase
	) {
		return new IndexMaintenanceOperation(
				operation.id(),
				operation.partitionKey(),
				operation.type(),
				phase,
				operation.token(),
				operation.leaseExpiresAt(),
				operation.partitionVersion(),
				operation.baseGenerationId(),
				operation.buildingGenerationId(),
				operation.operationVersion() + 1);
	}

	private static IndexMaintenanceOperation withOperationVersion(
			IndexMaintenanceOperation operation
	) {
		return withPhase(operation, operation.phase());
	}

	private static IndexTargetResolution outcome(IndexTargetResolutionStatus status) {
		return IndexTargetResolution.outcome(status);
	}

	private static RuntimeException ioFailure(IOException exception) {
		if (Thread.currentThread().isInterrupted()) {
			return new IndexingInterruptedException(exception);
		}
		return new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE, exception);
	}

	private static RuntimeException classify(ElasticsearchException exception) {
		int status = exception.status();
		if (status == 408 || status == 429 || (status >= 500 && status <= 599)) {
			return new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE, exception);
		}
		return new IndexingProtocolException(IndexingErrorCode.INDEXING_REQUEST_REJECTED, exception);
	}

	private static boolean hasType(ElasticsearchException exception, String type) {
		return exception.error() != null && type.equals(exception.error().type());
	}

	/** Проводит один и тот же cycle budget через всю lifecycle-цепочку. */
	private final class LifecycleCalls {

		private final OperationBudget budget;

		private LifecycleCalls(OperationBudget budget) {
			this.budget = budget;
		}

		private void registerPartition(
				IndexPartitionDefinition partition
		) {
			requireMutationAllowed();
			lifecycleLedger.registerPartition(partition);
		}

		private Optional<IndexMaintenanceOperation> reclaimExpiredMaintenance(
				String partitionKey,
				Duration leaseDuration
		) {
			requireMutationAllowed();
			return lifecycleLedger.reclaimExpiredMaintenance(partitionKey, leaseDuration);
		}

		private Optional<IndexMaintenanceOperation> startMaintenance(
				String partitionKey,
				IndexMaintenanceType type,
				IndexGenerationNames targetNames,
				Duration leaseDuration
		) {
			requireMutationAllowed();
			return lifecycleLedger.startMaintenance(
					partitionKey,
					type,
					targetNames,
					leaseDuration);
		}

		private IndexLifecycleTransitionResult recordGenerationUuids(
				String partitionKey,
				UUID operationToken,
				long expectedPartitionVersion,
				long expectedOperationVersion,
				String eventIndexUuid,
				String mentionIndexUuid
		) {
			requireMutationAllowed();
			return lifecycleLedger.recordGenerationUuids(
					partitionKey,
					operationToken,
					expectedPartitionVersion,
					expectedOperationVersion,
					eventIndexUuid,
					mentionIndexUuid);
		}

		private IndexLifecycleTransitionResult advancePhase(
				String partitionKey,
				UUID operationToken,
				long expectedPartitionVersion,
				long expectedOperationVersion,
				IndexMaintenancePhase expectedPhase,
				IndexMaintenancePhase nextPhase
		) {
			requireMutationAllowed();
			return lifecycleLedger.advancePhase(
					partitionKey,
					operationToken,
					expectedPartitionVersion,
					expectedOperationVersion,
					expectedPhase,
					nextPhase);
		}

		private IndexLifecycleTransitionResult completeInitialActivation(
				String partitionKey,
				UUID operationToken,
				long expectedPartitionVersion,
				long expectedOperationVersion
		) {
			requireMutationAllowed();
			return lifecycleLedger.completeInitialActivation(
					partitionKey,
					operationToken,
					expectedPartitionVersion,
					expectedOperationVersion);
		}

		private void requireMutationAllowed() {
			if (budget != null) {
				budget.requireAvailable();
			}
		}

		private void installTemplates() throws IOException {
			if (budget == null) {
				elasticsearch.installTemplates();
			}
			else {
				elasticsearch.installTemplates(budget);
			}
		}

		private Optional<ObservedElasticsearchIndex> findExactIndex(
				String indexName
		) throws IOException {
			return budget == null
					? elasticsearch.findExactIndex(indexName)
					: elasticsearch.findExactIndex(indexName, budget);
		}

		private void createExactIndex(String indexName) throws IOException {
			if (budget == null) {
				elasticsearch.createExactIndex(indexName);
			}
			else {
				elasticsearch.createExactIndex(indexName, budget);
			}
		}

		private IndexAliasMembership readStableAliases() throws IOException {
			return budget == null
					? elasticsearch.readStableAliases()
					: elasticsearch.readStableAliases(budget);
		}

		private void addStableAliases(
				String eventIndexName,
				boolean addEvent,
				String mentionIndexName,
				boolean addMention
		) throws IOException {
			if (budget == null) {
				elasticsearch.addStableAliases(
						eventIndexName,
						addEvent,
						mentionIndexName,
						addMention);
			}
			else {
				elasticsearch.addStableAliases(
						eventIndexName,
						addEvent,
						mentionIndexName,
						addMention,
						budget);
			}
		}
	}

	private record MaintenanceAcquisition(
			IndexMaintenanceOperation operation,
			IndexTargetResolution outcome
	) {
		static MaintenanceAcquisition owned(IndexMaintenanceOperation operation) {
			return new MaintenanceAcquisition(operation, null);
		}

		static MaintenanceAcquisition ready(IndexTargetResolution resolution) {
			return new MaintenanceAcquisition(null, resolution);
		}

		static MaintenanceAcquisition outcome(IndexTargetResolutionStatus status) {
			return ready(ElasticsearchIndexTargetResolver.outcome(status));
		}
	}

	private record BuildingPair(
			IndexMaintenanceOperation operation,
			IndexTargetResolution outcome
	) {
		static BuildingPair owned(IndexMaintenanceOperation operation) {
			return new BuildingPair(operation, null);
		}

		static BuildingPair readyOutcome(IndexTargetResolution outcome) {
			return new BuildingPair(null, outcome);
		}

		static BuildingPair outcome(IndexTargetResolutionStatus status) {
			return readyOutcome(ElasticsearchIndexTargetResolver.outcome(status));
		}
	}

	private record ObservedPair(
			ObservedElasticsearchIndex event,
			ObservedElasticsearchIndex mention,
			IndexTargetResolution outcome
	) {
		static ObservedPair observed(
				ObservedElasticsearchIndex event,
				ObservedElasticsearchIndex mention
		) {
			return new ObservedPair(event, mention, null);
		}

		static ObservedPair readyOutcome(IndexTargetResolution outcome) {
			return new ObservedPair(null, null, outcome);
		}
	}

	private record ObservedIndexResult(
			ObservedElasticsearchIndex index,
			IndexTargetResolution outcome
	) {
		static ObservedIndexResult observed(ObservedElasticsearchIndex index) {
			return new ObservedIndexResult(index, null);
		}

		static ObservedIndexResult outcome(IndexTargetResolutionStatus status) {
			return new ObservedIndexResult(null, ElasticsearchIndexTargetResolver.outcome(status));
		}
	}

	private record AliasPairState(
			boolean eventPresent,
			boolean mentionPresent,
			boolean unexpected
	) {
		boolean complete() {
			return eventPresent && mentionPresent;
		}
	}
}
