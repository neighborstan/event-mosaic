package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.IndexGeneration;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionGenerationResolver;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolutionStatus;
import com.neighbor.eventmosaic.indexing.api.IndexTargetResolver;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({TestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Initial promotion с PostgreSQL и Elasticsearch")
class ElasticsearchIndexTargetResolverIntegrationTest {

	private static final Instant SOURCE_TIME = Instant.parse("2026-07-30T10:15:00Z");
	private static final Instant EARLIER_SOURCE_TIME =
			Instant.parse("2026-07-20T10:15:00Z");
	private static final String PARTITION_KEY = "p20260727";
	private static final String EARLIER_PARTITION_KEY = "p20260720";
	private static final String EVENT_INDEX = "gdelt-events-v1-p20260727-g0001";
	private static final String MENTION_INDEX = "gdelt-mentions-v1-p20260727-g0001";
	private static final String EARLIER_EVENT_INDEX =
			"gdelt-events-v1-p20260720-g0001";
	private static final String EARLIER_MENTION_INDEX =
			"gdelt-mentions-v1-p20260720-g0001";
	private static final String NEXT_EVENT_INDEX =
			"gdelt-events-v1-p20260727-g0002";
	private static final String NEXT_MENTION_INDEX =
			"gdelt-mentions-v1-p20260727-g0002";
	private static final String EARLIER_NEXT_EVENT_INDEX =
			"gdelt-events-v1-p20260720-g0002";
	private static final String EARLIER_NEXT_MENTION_INDEX =
			"gdelt-mentions-v1-p20260720-g0002";
	private static final String LEGACY_EVENT_INDEX = "gdelt-events-v1";
	private static final String LEGACY_MENTION_INDEX = "gdelt-mentions-v1";
	private static final Duration LEASE = Duration.ofMinutes(15);

	@Autowired
	private IndexTargetResolver targetResolver;

	@Autowired
	private IndexPartitionGenerationResolver partitionResolver;

	@Autowired
	private IndexLifecycleLedger lifecycleLedger;

	@Autowired
	private IndexLifecycleElasticsearchGateway elasticsearch;

	@Autowired
	private ElasticsearchClient client;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private Clock clock;

	@BeforeEach
	void cleanState() throws IOException {
		client.indices().delete(request -> request
				.index(
						EVENT_INDEX,
						MENTION_INDEX,
						EARLIER_EVENT_INDEX,
						EARLIER_MENTION_INDEX,
						NEXT_EVENT_INDEX,
						NEXT_MENTION_INDEX,
						EARLIER_NEXT_EVENT_INDEX,
						EARLIER_NEXT_MENTION_INDEX,
						LEGACY_EVENT_INDEX,
						LEGACY_MENTION_INDEX)
				.ignoreUnavailable(true)
				.allowNoIndices(true));
		jdbcClient.sql("""
				truncate table
				    index_maintenance_operations,
				    ingestion_archive_processing,
				    index_generations,
				    index_logical_partitions,
				    ingestion_source_poll_state,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Templates предшествуют pair create, aliases и SQL ACTIVE")
	void promotesExactPairAndReturnsSameTargetsOnReplay() throws IOException {
		var first = targetResolver.resolve(SOURCE_TIME);

		assertThat(first.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(first.targets()).isNotNull().satisfies(targets -> {
			assertThat(targets.partitionKey()).isEqualTo(PARTITION_KEY);
			assertThat(targets.event().indexName()).isEqualTo(EVENT_INDEX);
			assertThat(targets.mention().indexName()).isEqualTo(MENTION_INDEX);
			assertThat(targets.event().indexUuid()).isNotBlank();
			assertThat(targets.mention().indexUuid()).isNotBlank();
		});
		assertThat(client.indices().exists(request -> request.index(EVENT_INDEX)).value())
				.isTrue();
		assertThat(client.indices().exists(request -> request.index(MENTION_INDEX)).value())
				.isTrue();
		IndexAliasMembership membership = elasticsearch.readStableAliases();
		assertThat(membership.eventIndices()).contains(EVENT_INDEX);
		assertThat(membership.mentionIndices()).contains(MENTION_INDEX);
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY))
				.singleElement()
				.satisfies(generation -> {
					assertThat(generation.status()).isEqualTo(IndexGenerationStatus.ACTIVE);
					assertThat(generation.eventIndexUuid())
							.isEqualTo(first.targets().event().indexUuid());
					assertThat(generation.mentionIndexUuid())
							.isEqualTo(first.targets().mention().indexUuid());
				});

		var replay = targetResolver.resolve(SOURCE_TIME);

		assertThat(replay.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(replay.targets()).isEqualTo(first.targets());
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY)).hasSize(1);
	}

	@Test
	@DisplayName("Две недельные partition сохраняют по одному текущему поколению при повторе")
	void keepsOneCurrentGenerationPerPartitionOnReplay() throws IOException {
		var current = targetResolver.resolve(SOURCE_TIME);
		var earlier = targetResolver.resolve(EARLIER_SOURCE_TIME);

		assertThat(current.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(earlier.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(current.targets().event().indexName()).isEqualTo(EVENT_INDEX);
		assertThat(current.targets().mention().indexName()).isEqualTo(MENTION_INDEX);
		assertThat(earlier.targets().event().indexName()).isEqualTo(EARLIER_EVENT_INDEX);
		assertThat(earlier.targets().mention().indexName()).isEqualTo(EARLIER_MENTION_INDEX);

		IndexAliasMembership membership = elasticsearch.readStableAliases();
		assertThat(membership.eventIndices())
				.filteredOn(index -> index.contains("-" + PARTITION_KEY + "-"))
				.containsExactly(EVENT_INDEX);
		assertThat(membership.mentionIndices())
				.filteredOn(index -> index.contains("-" + PARTITION_KEY + "-"))
				.containsExactly(MENTION_INDEX);
		assertThat(membership.eventIndices())
				.filteredOn(index -> index.contains("-" + EARLIER_PARTITION_KEY + "-"))
				.containsExactly(EARLIER_EVENT_INDEX);
		assertThat(membership.mentionIndices())
				.filteredOn(index -> index.contains("-" + EARLIER_PARTITION_KEY + "-"))
				.containsExactly(EARLIER_MENTION_INDEX);

		var currentReplay = targetResolver.resolve(SOURCE_TIME);
		var earlierReplay = targetResolver.resolve(EARLIER_SOURCE_TIME);

		assertThat(currentReplay.targets()).isEqualTo(current.targets());
		assertThat(earlierReplay.targets()).isEqualTo(earlier.targets());
		assertThat(lifecycleLedger.findGenerations(PARTITION_KEY))
				.singleElement()
				.extracting(IndexGeneration::status)
				.isEqualTo(IndexGenerationStatus.ACTIVE);
		assertThat(lifecycleLedger.findGenerations(EARLIER_PARTITION_KEY))
				.singleElement()
				.extracting(IndexGeneration::status)
				.isEqualTo(IndexGenerationStatus.ACTIVE);
		assertThat(client.indices().exists(request -> request.index(NEXT_EVENT_INDEX)).value())
				.isFalse();
		assertThat(client.indices().exists(request -> request.index(NEXT_MENTION_INDEX)).value())
				.isFalse();
		assertThat(client.indices().exists(request -> request
				.index(EARLIER_NEXT_EVENT_INDEX)).value()).isFalse();
		assertThat(client.indices().exists(request -> request
				.index(EARLIER_NEXT_MENTION_INDEX)).value()).isFalse();
	}

	@Test
	@DisplayName("Initial promotion сохраняет preexisting legacy fixed indices без изменений")
	void preservesLegacyFixedIndicesDuringInitialPromotion() throws IOException {
		LegacyMarkerDocument eventMarker = new LegacyMarkerDocument("event");
		LegacyMarkerDocument mentionMarker = new LegacyMarkerDocument("mention");
		client.indices().create(request -> request.index(LEGACY_EVENT_INDEX));
		client.indices().create(request -> request.index(LEGACY_MENTION_INDEX));
		client.index(request -> request
				.index(LEGACY_EVENT_INDEX)
				.id("legacy-event")
				.refresh(Refresh.WaitFor)
				.document(eventMarker));
		client.index(request -> request
				.index(LEGACY_MENTION_INDEX)
				.id("legacy-mention")
				.refresh(Refresh.WaitFor)
				.document(mentionMarker));
		String eventUuid = indexUuid(LEGACY_EVENT_INDEX);
		String mentionUuid = indexUuid(LEGACY_MENTION_INDEX);

		var resolution = targetResolver.resolve(SOURCE_TIME);

		assertThat(resolution.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(indexUuid(LEGACY_EVENT_INDEX)).isEqualTo(eventUuid);
		assertThat(indexUuid(LEGACY_MENTION_INDEX)).isEqualTo(mentionUuid);
		assertThat(client.count(request -> request.index(LEGACY_EVENT_INDEX)).count())
				.isEqualTo(1);
		assertThat(client.count(request -> request.index(LEGACY_MENTION_INDEX)).count())
				.isEqualTo(1);
		assertThat(client.get(
				request -> request.index(LEGACY_EVENT_INDEX).id("legacy-event"),
				LegacyMarkerDocument.class).source()).isEqualTo(eventMarker);
		assertThat(client.get(
				request -> request.index(LEGACY_MENTION_INDEX).id("legacy-mention"),
				LegacyMarkerDocument.class).source()).isEqualTo(mentionMarker);
	}

	@Test
	@DisplayName("Expired CUTOVER_REQUESTED дополняет partial alias той же generation")
	void reclaimsExpiredOperationAndCompletesPartialAliasMembership() throws IOException {
		var plan = partitionResolver.resolve(SOURCE_TIME, 1);
		lifecycleLedger.registerPartition(plan.partition());
		elasticsearch.installTemplates();
		IndexMaintenanceOperation operation = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				plan.names(),
				LEASE).orElseThrow();
		operation = advance(operation, IndexMaintenancePhase.BUILDING);
		elasticsearch.createExactIndex(EVENT_INDEX);
		elasticsearch.createExactIndex(MENTION_INDEX);
		String eventUuid = elasticsearch.findExactIndex(EVENT_INDEX).orElseThrow().indexUuid();
		String mentionUuid = elasticsearch.findExactIndex(MENTION_INDEX).orElseThrow().indexUuid();
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				eventUuid,
				mentionUuid)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		operation = lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
		operation = advance(operation, IndexMaintenancePhase.VERIFIED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_REQUESTED);
		elasticsearch.addStableAliases(EVENT_INDEX, true, MENTION_INDEX, false);
		assertThat(elasticsearch.readStableAliases()).satisfies(membership -> {
			assertThat(membership.eventIndices()).contains(EVENT_INDEX);
			assertThat(membership.mentionIndices()).doesNotContain(MENTION_INDEX);
		});
		jdbcClient.sql("""
				update index_maintenance_operations
				set heartbeat_at = :expiredHeartbeatAt,
				    lease_expires_at = :expiredAt
				where id = :operationId
				""")
				.param("expiredHeartbeatAt", Timestamp.from(clock.instant().minusSeconds(2)))
				.param("expiredAt", Timestamp.from(clock.instant().minusSeconds(1)))
				.param("operationId", operation.id())
				.update();

		var recovered = targetResolver.resolve(SOURCE_TIME);

		assertThat(recovered.status()).isEqualTo(IndexTargetResolutionStatus.READY);
		assertThat(elasticsearch.readStableAliases()).satisfies(membership -> {
			assertThat(membership.eventIndices()).contains(EVENT_INDEX);
			assertThat(membership.mentionIndices()).contains(MENTION_INDEX);
		});
		List<IndexGeneration> generations = lifecycleLedger.findGenerations(PARTITION_KEY);
		assertThat(generations).singleElement()
				.extracting(IndexGeneration::status)
				.isEqualTo(IndexGenerationStatus.ACTIVE);
		assertThat(lifecycleLedger.findRecoverableOperation(PARTITION_KEY)).isEmpty();
	}

	private IndexMaintenanceOperation advance(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase next
	) {
		assertThat(lifecycleLedger.advancePhase(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				operation.phase(),
				next)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private String indexUuid(String indexName) throws IOException {
		var settings = client.indices()
				.get(request -> request.index(indexName))
				.get(indexName)
				.settings();
		var indexSettings = settings.index() == null ? settings : settings.index();
		return indexSettings.uuid();
	}

	private record LegacyMarkerDocument(String legacyMarker) {
	}
}
