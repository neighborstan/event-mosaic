package com.neighbor.eventmosaic.indexing.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleLedger;
import com.neighbor.eventmosaic.indexing.api.IndexLifecycleTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceOperation;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenancePhase;
import com.neighbor.eventmosaic.indexing.api.IndexMaintenanceType;
import com.neighbor.eventmosaic.indexing.api.IndexPartitionDefinition;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Durable evidence записи rebuild generation в PostgreSQL")
class IndexBuildWriteOutcomeLedgerIntegrationTest {

	private static final String PARTITION_KEY = "p20260713";
	private static final Instant PARTITION_START = Instant.parse("2026-07-13T00:00:00Z");
	private static final Duration LEASE = Duration.ofMinutes(15);

	@Autowired
	private IndexLifecycleLedger lifecycleLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanState() {
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
	@DisplayName("Write outcome повышается монотонно и только COMPLETED разрешает VERIFIED rebuild")
	void writeOutcomeIsMonotonicAndFencesVerifiedRebuild() {
		// Given
		registerPartition();
		completeInitialPromotion();
		IndexMaintenanceOperation rebuild = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.REBUILD,
				names(2),
				LEASE).orElseThrow();
		rebuild = advance(rebuild, IndexMaintenancePhase.FREEZE_REQUESTED);
		rebuild = advance(rebuild, IndexMaintenancePhase.FROZEN);
		rebuild = advance(rebuild, IndexMaintenancePhase.BUILDING);
		rebuild = recordUuids(rebuild, "event-uuid-g2", "mention-uuid-g2");

		// When / Then
		assertThat(advanceResult(rebuild, IndexMaintenancePhase.VERIFIED))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(writeOutcome()).isEqualTo(CleanupBuildWriteOutcome.NONE.name());

		rebuild = recordOutcome(rebuild, CleanupBuildWriteOutcome.UNKNOWN);
		long unknownVersion = rebuild.operationVersion();
		rebuild = recordOutcome(rebuild, CleanupBuildWriteOutcome.NONE);
		assertThat(rebuild.operationVersion()).isEqualTo(unknownVersion + 1);
		assertThat(writeOutcome()).isEqualTo(CleanupBuildWriteOutcome.UNKNOWN.name());

		IndexMaintenanceOperation stale = rebuild;
		rebuild = recordOutcome(rebuild, CleanupBuildWriteOutcome.PARTIAL);
		assertThat(lifecycleLedger.recordBuildWriteOutcome(
				PARTITION_KEY,
				stale.token(),
				stale.partitionVersion(),
				stale.operationVersion(),
				CleanupBuildWriteOutcome.COMPLETED))
				.isEqualTo(IndexLifecycleTransitionResult.OWNERSHIP_LOST);
		assertThat(writeOutcome()).isEqualTo(CleanupBuildWriteOutcome.PARTIAL.name());

		rebuild = recordOutcome(rebuild, CleanupBuildWriteOutcome.COMPLETED);
		assertThat(writeOutcome()).isEqualTo(CleanupBuildWriteOutcome.COMPLETED.name());
		assertThat(advanceResult(rebuild, IndexMaintenancePhase.VERIFIED))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
	}

	private void registerPartition() {
		lifecycleLedger.registerPartition(new IndexPartitionDefinition(
				PARTITION_KEY,
				PARTITION_START,
				PARTITION_START.plus(Duration.ofDays(7)),
				Period.ofDays(7)));
	}

	private void completeInitialPromotion() {
		IndexMaintenanceOperation operation = lifecycleLedger.startMaintenance(
				PARTITION_KEY,
				IndexMaintenanceType.INITIAL_PROMOTION,
				names(1),
				LEASE).orElseThrow();
		operation = advance(operation, IndexMaintenancePhase.BUILDING);
		operation = recordUuids(operation, "event-uuid-g1", "mention-uuid-g1");
		operation = advance(operation, IndexMaintenancePhase.VERIFIED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_REQUESTED);
		operation = advance(operation, IndexMaintenancePhase.CUTOVER_OBSERVED);
		assertThat(lifecycleLedger.completeInitialActivation(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion()))
				.isEqualTo(IndexLifecycleTransitionResult.APPLIED);
	}

	private IndexMaintenanceOperation advance(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase next
	) {
		assertThat(advanceResult(operation, next)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recover();
	}

	private IndexLifecycleTransitionResult advanceResult(
			IndexMaintenanceOperation operation,
			IndexMaintenancePhase next
	) {
		return lifecycleLedger.advancePhase(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				operation.phase(),
				next);
	}

	private IndexMaintenanceOperation recordUuids(
			IndexMaintenanceOperation operation,
			String eventUuid,
			String mentionUuid
	) {
		assertThat(lifecycleLedger.recordGenerationUuids(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				eventUuid,
				mentionUuid)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recover();
	}

	private IndexMaintenanceOperation recordOutcome(
			IndexMaintenanceOperation operation,
			CleanupBuildWriteOutcome minimumOutcome
	) {
		assertThat(lifecycleLedger.recordBuildWriteOutcome(
				PARTITION_KEY,
				operation.token(),
				operation.partitionVersion(),
				operation.operationVersion(),
				minimumOutcome)).isEqualTo(IndexLifecycleTransitionResult.APPLIED);
		return recover();
	}

	private IndexMaintenanceOperation recover() {
		return lifecycleLedger.findRecoverableOperation(PARTITION_KEY).orElseThrow();
	}

	private String writeOutcome() {
		return jdbcClient.sql("""
				select build_write_outcome
				from index_maintenance_operations
				where partition_key = :partitionKey
				  and phase not in ('COMPLETED', 'FAILED')
				""")
				.param("partitionKey", PARTITION_KEY)
				.query(String.class)
				.single();
	}

	private static IndexGenerationNames names(int generation) {
		String suffix = PARTITION_KEY + "-g%04d".formatted(generation);
		return new IndexGenerationNames(
				"gdelt-events-v1-" + suffix,
				"gdelt-mentions-v1-" + suffix);
	}
}
