package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import com.neighbor.eventmosaic.indexing.api.IndexPartition;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolutionStatus;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

@DisplayName("Initial promotion exact generation")
class ElasticsearchIndexTargetResolverTest {

	private static final Instant NOW = Instant.parse("2026-07-31T10:00:00Z");
	private static final Instant SOURCE_TIME = Instant.parse("2026-07-30T10:15:00Z");
	private static final String PARTITION_KEY = "p20260727";
	private static final String EVENT_INDEX = "gdelt-events-v1-p20260727-g0001";
	private static final String MENTION_INDEX = "gdelt-mentions-v1-p20260727-g0001";
	private static final String EVENT_UUID = "event-es-uuid";
	private static final String MENTION_UUID = "mention-es-uuid";

	private final IndexLifecycleLedger ledger = mock(IndexLifecycleLedger.class);
	private final IndexLifecycleElasticsearchGateway elasticsearch =
			mock(IndexLifecycleElasticsearchGateway.class);
	private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);
	private final ElasticsearchIndexTargetResolver resolver = new ElasticsearchIndexTargetResolver(
			new UtcP7dIndexPartitionGenerationResolver(),
			ledger,
			elasticsearch,
			clock);

	@Test
	@DisplayName("Open maintenance barrier не выдает существующую ACTIVE pair writer")
	void defersActiveTargetsWhileMaintenanceIsOpen() {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.of(operation()));
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status())
				.isEqualTo(IndexTargetResolutionStatus.MAINTENANCE_DEFERRED);
		assertThat(result.targets()).isNull();
		verifyNoInteractions(elasticsearch);
	}

	@Test
	@DisplayName("ACTIVE без полной alias pair возвращает typed missing target")
	void rejectsActiveGenerationWithoutFullAliasPair() throws IOException {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));
		when(elasticsearch.readStableAliases()).thenReturn(IndexAliasMembership.empty());

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.MISSING);
		verify(elasticsearch, never()).findExactIndex(any());
	}

	@Test
	@DisplayName("Unexpected alias target той же partition отклоняет READY binding")
	void rejectsUnexpectedAliasTargetWithinSamePartition() throws IOException {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));
		when(elasticsearch.readStableAliases()).thenReturn(new IndexAliasMembership(
				Set.of(EVENT_INDEX, "gdelt-events-v1-p20260727-g0002"),
				Set.of(MENTION_INDEX)));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		verify(elasticsearch, never()).findExactIndex(any());
	}

	@Test
	@DisplayName("Legacy или malformed alias target отклоняет promotion")
	void rejectsNonPhysicalAliasTarget() throws IOException {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));
		when(elasticsearch.readStableAliases()).thenReturn(new IndexAliasMembership(
				Set.of(EVENT_INDEX, "gdelt-events-v1"),
				Set.of(MENTION_INDEX)));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.OWNERSHIP_LOST);
		verify(elasticsearch, never()).findExactIndex(any());
	}

	@Test
	@DisplayName("Alias targets других partitions не мешают coherent READY pair")
	void returnsReadyForExactActivePairAndIgnoresOtherPartitions() throws IOException {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));
		when(elasticsearch.readStableAliases()).thenReturn(new IndexAliasMembership(
				Set.of(EVENT_INDEX, "gdelt-events-v1-p20260720-g0003"),
				Set.of(MENTION_INDEX, "gdelt-mentions-v1-p20260720-g0003")));
		when(elasticsearch.findExactIndex(EVENT_INDEX)).thenReturn(Optional.of(eventObserved()));
		when(elasticsearch.findExactIndex(MENTION_INDEX)).thenReturn(Optional.of(mentionObserved()));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(result.targets()).isEqualTo(activeTargets());
	}

	@Test
	@DisplayName("Observed write block возвращает typed outcome вместо READY")
	void returnsWriteBlockedForBlockedActiveTarget() throws IOException {
		stubRegisteredPartition();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.of(activeTargets()));
		when(elasticsearch.readStableAliases()).thenReturn(new IndexAliasMembership(
				Set.of(EVENT_INDEX),
				Set.of(MENTION_INDEX)));
		when(elasticsearch.findExactIndex(EVENT_INDEX)).thenReturn(Optional.of(
				new ObservedElasticsearchIndex(EVENT_INDEX, EVENT_UUID, true)));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.WRITE_BLOCKED);
		verify(elasticsearch, never()).findExactIndex(MENTION_INDEX);
	}

	@Test
	@DisplayName("Initial promotion создает pair после templates и дополняет partial alias")
	void createsPairAfterTemplatesAndCompletesPartialAliasMembership() throws IOException {
		stubRegisteredPartition();
		IndexMaintenanceOperation operation = operation();
		IndexGeneration unrecorded = building(null, null);
		IndexGeneration recorded = building(EVENT_UUID, MENTION_UUID);
		when(ledger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.of(activeTargets()));
		when(ledger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of())
				.thenReturn(List.of(unrecorded))
				.thenReturn(List.of(recorded));
		when(ledger.startMaintenance(
				eq(PARTITION_KEY),
				eq(IndexMaintenanceType.INITIAL_PROMOTION),
				any(IndexGenerationNames.class),
				any(Duration.class)))
				.thenReturn(Optional.of(operation));
		when(ledger.advancePhase(
				eq(PARTITION_KEY),
				eq(operation.token()),
				eq(operation.partitionVersion()),
				any(Long.class),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(ledger.recordGenerationUuids(
				eq(PARTITION_KEY),
				eq(operation.token()),
				eq(operation.partitionVersion()),
				any(Long.class),
				eq(EVENT_UUID),
				eq(MENTION_UUID)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(ledger.completeInitialActivation(
				eq(PARTITION_KEY),
				eq(operation.token()),
				eq(operation.partitionVersion()),
				any(Long.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(elasticsearch.findExactIndex(EVENT_INDEX))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.of(eventObserved()))
				.thenReturn(Optional.of(eventObserved()))
				.thenReturn(Optional.of(eventObserved()));
		when(elasticsearch.findExactIndex(MENTION_INDEX))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.of(mentionObserved()))
				.thenReturn(Optional.of(mentionObserved()))
				.thenReturn(Optional.of(mentionObserved()));
		when(elasticsearch.readStableAliases())
				.thenReturn(new IndexAliasMembership(Set.of(EVENT_INDEX), Set.of()))
				.thenReturn(new IndexAliasMembership(Set.of(EVENT_INDEX), Set.of(MENTION_INDEX)))
				.thenReturn(new IndexAliasMembership(Set.of(EVENT_INDEX), Set.of(MENTION_INDEX)));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		InOrder creationOrder = inOrder(elasticsearch);
		creationOrder.verify(elasticsearch).installTemplates();
		creationOrder.verify(elasticsearch).createExactIndex(EVENT_INDEX);
		creationOrder.verify(elasticsearch).createExactIndex(MENTION_INDEX);
		verify(elasticsearch).addStableAliases(
				EVENT_INDEX,
				false,
				MENTION_INDEX,
				true);
	}

	@Test
	@DisplayName("Recovered cutover проверяет recorded UUID до изменения aliases")
	void verifiesRecordedUuidBeforeRecoveredAliasMutation() throws IOException {
		stubRegisteredPartition();
		IndexMaintenanceOperation expired = cutoverRequestedOperation();
		when(ledger.findRecoverableOperation(PARTITION_KEY))
				.thenReturn(Optional.of(expired));
		when(ledger.findActiveTargets(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.reclaimExpiredMaintenance(PARTITION_KEY, Duration.ofMinutes(15)))
				.thenReturn(Optional.of(expired));
		when(ledger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of(building(EVENT_UUID, MENTION_UUID)));
		when(elasticsearch.findExactIndex(EVENT_INDEX)).thenReturn(Optional.of(
				new ObservedElasticsearchIndex(EVENT_INDEX, "replacement-event-uuid", false)));

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.MISSING);
		verify(elasticsearch, never()).readStableAliases();
		verify(elasticsearch, never()).addStableAliases(any(), any(Boolean.class), any(), any(Boolean.class));
	}

	@Test
	@DisplayName("Unacknowledged alias response reconciles observed complete pair")
	void reconcilesUnacknowledgedAliasOutcome() throws IOException {
		stubInitialPromotionWithRecordedPair();
		when(elasticsearch.findExactIndex(EVENT_INDEX)).thenReturn(Optional.of(eventObserved()));
		when(elasticsearch.findExactIndex(MENTION_INDEX)).thenReturn(Optional.of(mentionObserved()));
		when(elasticsearch.readStableAliases())
				.thenReturn(IndexAliasMembership.empty())
				.thenReturn(new IndexAliasMembership(Set.of(EVENT_INDEX), Set.of(MENTION_INDEX)))
				.thenReturn(new IndexAliasMembership(Set.of(EVENT_INDEX), Set.of(MENTION_INDEX)));
		org.mockito.Mockito.doThrow(new IndexingAccessException(
				IndexingErrorCode.INDEXING_UNAVAILABLE))
				.when(elasticsearch)
				.addStableAliases(EVENT_INDEX, true, MENTION_INDEX, true);

		var result = resolver.resolve(SOURCE_TIME);

		assertThat(result.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		verify(elasticsearch).addStableAliases(EVENT_INDEX, true, MENTION_INDEX, true);
	}

	private void stubRegisteredPartition() {
		when(ledger.registerPartition(any(IndexPartitionDefinition.class)))
				.thenReturn(new IndexPartition(partition(), 0, null));
	}

	private void stubInitialPromotionWithRecordedPair() {
		IndexMaintenanceOperation operation = operation();
		when(ledger.findRecoverableOperation(PARTITION_KEY)).thenReturn(Optional.empty());
		when(ledger.findActiveTargets(PARTITION_KEY))
				.thenReturn(Optional.empty())
				.thenReturn(Optional.of(activeTargets()));
		when(ledger.findGenerations(PARTITION_KEY))
				.thenReturn(List.of())
				.thenReturn(List.of(building(EVENT_UUID, MENTION_UUID)));
		when(ledger.startMaintenance(
				eq(PARTITION_KEY),
				eq(IndexMaintenanceType.INITIAL_PROMOTION),
				any(IndexGenerationNames.class),
				any(Duration.class)))
				.thenReturn(Optional.of(operation));
		when(ledger.advancePhase(
				eq(PARTITION_KEY),
				eq(operation.token()),
				eq(operation.partitionVersion()),
				any(Long.class),
				any(IndexMaintenancePhase.class),
				any(IndexMaintenancePhase.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
		when(ledger.completeInitialActivation(
				eq(PARTITION_KEY),
				eq(operation.token()),
				eq(operation.partitionVersion()),
				any(Long.class)))
				.thenReturn(IndexLifecycleTransitionResult.APPLIED);
	}

	private static IndexPartitionDefinition partition() {
		Instant start = Instant.parse("2026-07-27T00:00:00Z");
		return new IndexPartitionDefinition(
				PARTITION_KEY,
				start,
				start.plus(Duration.ofDays(7)),
				Period.ofDays(7));
	}

	private static IndexMaintenanceOperation operation() {
		return new IndexMaintenanceOperation(
				1,
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				IndexMaintenancePhase.PLANNED,
				UUID.fromString("11111111-1111-1111-1111-111111111111"),
				NOW.plus(Duration.ofMinutes(15)),
				1,
				null,
				1,
				0);
	}

	private static IndexMaintenanceOperation cutoverRequestedOperation() {
		return new IndexMaintenanceOperation(
				1,
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				IndexMaintenancePhase.CUTOVER_REQUESTED,
				UUID.fromString("11111111-1111-1111-1111-111111111111"),
				NOW.minus(Duration.ofMinutes(1)),
				1,
				null,
				1,
				3);
	}

	private static IndexGeneration building(String eventUuid, String mentionUuid) {
		return new IndexGeneration(
				1,
				UUID.fromString("22222222-2222-2222-2222-222222222222"),
				PARTITION_KEY,
				1,
				IndexGenerationStatus.BUILDING,
				new IndexGenerationNames(EVENT_INDEX, MENTION_INDEX),
				eventUuid,
				mentionUuid,
				0);
	}

	private static ActiveIndexTargets activeTargets() {
		return new ActiveIndexTargets(
				PARTITION_KEY,
				2,
				1,
				UUID.fromString("22222222-2222-2222-2222-222222222222"),
				new ExactIndexTarget(EVENT_INDEX, EVENT_UUID),
				new ExactIndexTarget(MENTION_INDEX, MENTION_UUID));
	}

	private static ObservedElasticsearchIndex eventObserved() {
		return new ObservedElasticsearchIndex(EVENT_INDEX, EVENT_UUID, false);
	}

	private static ObservedElasticsearchIndex mentionObserved() {
		return new ObservedElasticsearchIndex(MENTION_INDEX, MENTION_UUID, false);
	}
}
