package com.neighbor.eventmosaic.indexing.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import(PostgreSqlTestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("Целевая PostgreSQL schema Backend Data MVP")
class BackendDataSchemaIntegrationTest {

	private static final List<String> TARGET_TABLES = List.of(
			"index_generations",
			"index_logical_partitions",
			"index_maintenance_operations",
			"ingestion_archive_processing",
			"ingestion_archives",
			"ingestion_gaps",
			"ingestion_runs",
			"ingestion_source_poll_state",
			"ingestion_source_state");

	@Autowired
	private Flyway flyway;

	@Autowired
	private JdbcClient jdbcClient;

	@Test
	@DisplayName("Чистая database получает единую target migration без pending изменений")
	void cleanDatabaseMigratesToTargetSchema() {
		// Given / When
		List<String> tables = applicationTables();

		// Then
		assertThat(flyway.info().pending()).isEmpty();
		assertThat(jdbcClient.sql("""
				select count(*)
				from flyway_schema_history
				where type = 'SQL' and success
				""").query(Integer.class).single()).isEqualTo(1);
		assertThat(jdbcClient.sql("""
				select count(*)
				from flyway_schema_history
				where not success
				""").query(Integer.class).single()).isZero();
		assertThat(tables).containsExactlyElementsOf(TARGET_TABLES);
	}

	@Test
	@DisplayName("Schema хранит aggregate receipt и не создает per-document persistence")
	void schemaContainsOnlyAggregateReceiptState() {
		// Given / When
		List<String> processingColumns = jdbcClient.sql("""
				select column_name
				from information_schema.columns
				where table_schema = current_schema()
				  and table_name = 'ingestion_archive_processing'
				order by column_name
				""").query(String.class).list();
		int forbiddenColumns = jdbcClient.sql("""
				select count(*)
				from information_schema.columns
				where table_schema = current_schema()
				  and column_name in (
				      'document_id',
				      'global_event_id',
				      'raw_mention_id',
				      'mention_identifier',
				      'source_document_key',
				      'event_body',
				      'mention_body'
				  )
				""").query(Integer.class).single();

		// Then
		assertThat(applicationTables()).containsExactlyElementsOf(TARGET_TABLES);
		assertThat(processingColumns).contains(
				"bound_generation_uuid",
				"bound_index_kind",
				"bound_index_name",
				"bound_index_uuid",
				"expected_document_count",
				"receipt_digest_algorithm",
				"expected_identity_digest",
				"actual_document_count",
				"actual_identity_digest",
				"verified_generation_id",
				"verified_index_uuid",
				"receipt_verified_at");
		assertThat(forbiddenColumns).isZero();
		assertThat(applicationTables())
				.noneMatch(table -> table.contains("document")
						|| table.contains("event")
						|| table.contains("mention"));
	}

	@Test
	@DisplayName("Schema отклоняет не-P7D partition, неканоническое имя и cleanup без origin")
	void schemaRejectsInvalidPartitionGenerationAndCleanupState() {
		// Given
		Instant start = Instant.parse("2026-07-28T00:00:00Z");
		Instant now = Instant.parse("2026-07-31T12:00:00Z");

		// When / Then
		assertThatThrownBy(() -> insertPartition("p_invalid", start, start.plusSeconds(6 * 86_400L)))
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining("ck_index_logical_partitions_interval");

		insertPartition("p_schema", start, start.plusSeconds(7 * 86_400L));
		assertThatThrownBy(() -> insertBuildingGeneration("p_schema", "g1", now))
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining("ck_index_generations_names");

		long generationId = insertBuildingGeneration("p_schema", "g0001", now);
		assertThatThrownBy(() -> jdbcClient.sql("""
				update index_generations
				set state = 'CLEANUP_PENDING',
				    cleanup_requested_at = :cleanupRequestedAt
				where id = :generationId
				""")
				.param("cleanupRequestedAt", Timestamp.from(now))
				.param("generationId", generationId)
				.update())
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining("ck_index_generations_cleanup_origin");
	}

	private List<String> applicationTables() {
		return jdbcClient.sql("""
				select table_name
				from information_schema.tables
				where table_schema = current_schema()
				  and table_type = 'BASE TABLE'
				  and table_name <> 'flyway_schema_history'
				order by table_name
				""").query(String.class).list();
	}

	private void insertPartition(String partitionKey, Instant start, Instant end) {
		jdbcClient.sql("""
				insert into index_logical_partitions (
				    partition_key,
				    partition_start_at,
				    partition_end_at,
				    partition_interval,
				    created_at,
				    updated_at
				)
				values (
				    :partitionKey,
				    :partitionStartAt,
				    :partitionEndAt,
				    'P7D',
				    :createdAt,
				    :updatedAt
				)
				""")
				.param("partitionKey", partitionKey)
				.param("partitionStartAt", Timestamp.from(start))
				.param("partitionEndAt", Timestamp.from(end))
				.param("createdAt", Timestamp.from(start))
				.param("updatedAt", Timestamp.from(start))
				.update();
	}

	private long insertBuildingGeneration(String partitionKey, String suffix, Instant now) {
		return jdbcClient.sql("""
				insert into index_generations (
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    mention_index_name,
				    heartbeat_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    1,
				    'BUILDING',
				    :eventIndexName,
				    :mentionIndexName,
				    :heartbeatAt,
				    :createdAt,
				    :updatedAt
				)
				returning id
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", partitionKey)
				.param("eventIndexName", "gdelt-events-v1-" + partitionKey + "-" + suffix)
				.param("mentionIndexName", "gdelt-mentions-v1-" + partitionKey + "-" + suffix)
				.param("heartbeatAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(Long.class)
				.single();
	}
}
