package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageStatus;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import(PostgreSqlTestcontainersConfiguration.class)
@SpringBootTest(properties = "spring.flyway.enabled=false")
@DisplayName("Обновление накопленного ingestion ledger с V001 до V002")
class GdeltLiveIngestionMigrationIntegrationTest {

	private static final Instant WINDOW_FROM = Instant.parse("2026-07-20T00:00:00Z");
	private static final Instant WINDOW_TO = Instant.parse("2026-07-21T00:00:00Z");
	private static final Instant LATEST_UPDATE = WINDOW_TO.minus(Duration.ofMinutes(15));
	private static final Instant RECORDED_AT = Instant.parse("2026-07-21T01:00:00Z");
	private static final Instant DUE_AT = Instant.parse("2026-07-21T00:30:00Z");
	private static final Instant FUTURE_AT = Instant.parse("2099-01-01T00:00:00Z");
	private static final Instant LEASE_EXPIRES_AT = Instant.parse("2099-01-02T00:00:00Z");
	private static final UUID POLL_TOKEN = UUID.fromString("00000000-0000-0000-0000-000000000101");
	private static final UUID ARCHIVE_TOKEN = UUID.fromString("00000000-0000-0000-0000-000000000102");
	private static final UUID PROCESSING_TOKEN = UUID.fromString("00000000-0000-0000-0000-000000000103");
	private static final UUID GENERATION_UUID = UUID.fromString("00000000-0000-0000-0000-000000000201");
	private static final String PARTITION_KEY = "p20260720";
	private static final String EVENT_INDEX_NAME = "gdelt-events-v1-p20260720-g0001";
	private static final String EVENT_INDEX_UUID = "migration-event-index-uuid";
	private static final String MENTION_INDEX_NAME = "gdelt-mentions-v1-p20260720-g0001";
	private static final String MENTION_INDEX_UUID = "migration-mention-index-uuid";

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private IngestionCoverageQuery coverageQuery;

	@Test
	@DisplayName("V002 сохраняет V001 rows и разрешает полное окно после безопасного backfill")
	void additiveMigrationPreservesRowsAndAllowsCompleteCoverage() {
		migrateToV001();
		seedV001State();
		List<UpgradeRowSnapshot> v001IdentitySnapshot = upgradeIdentitySnapshot();
		assertThat(v001IdentitySnapshot).hasSize(19);
		assertThat(successfulSqlMigrations()).isOne();

		migrateToLatest();

		assertThat(successfulSqlMigrations()).isEqualTo(2);
		assertThat(upgradeIdentitySnapshot()).containsExactlyElementsOf(v001IdentitySnapshot);
		assertSourceWatermarksAndPoliciesPreserved();
		assertRetryAndOwnershipEvidencePreserved();
		assertNewSchemaConstraintsAndCursors();
		backfillExistingLatestSource();
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO).status())
				.isEqualTo(IngestionCoverageStatus.COMPLETE);
		assertThat(sourceState("GDELT").firstRunPolicy()).isEqualTo("LATEST");
	}

	private void migrateToV001() {
		Flyway.configure()
				.dataSource(dataSource)
				.locations("classpath:db/migration")
				.target(MigrationVersion.fromVersion("1"))
				.load()
				.migrate();
	}

	private void migrateToLatest() {
		Flyway.configure()
				.dataSource(dataSource)
				.locations("classpath:db/migration")
				.load()
				.migrate();
	}

	private void seedV001State() {
		seedSourceStates();
		seedSourcePollStates();
		seedIndexTarget();
		seedAcquisitionAndProcessingStates();
	}

	private void seedSourceStates() {
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				)
				values
				    ('GDELT', :latestUpdate, :latestUpdate, 'LATEST', :recordedAt, :recordedAt),
				    ('GDELT_FIXED', :windowFrom, :latestUpdate, 'FIXED', :recordedAt, :recordedAt)
				""")
				.param("latestUpdate", Timestamp.from(LATEST_UPDATE))
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void seedSourcePollStates() {
		jdbcClient.sql("""
				insert into ingestion_source_poll_state (
				    source_name,
				    status,
				    attempt_token,
				    lease_expires_at,
				    total_attempt_count,
				    automatic_retries_used,
				    consecutive_retryable_failures,
				    automatic_retry_limit,
				    retry_not_before,
				    last_attempt_at,
				    failed_at,
				    last_error_code,
				    last_error_retryable,
				    created_at,
				    updated_at
				)
				values
				    (
				        'POLL_DUE', 'FAILED', null, null,
				        4, 3, 4, 3, :dueAt, :failedAt, :failedAt,
				        'MANIFEST_TIMEOUT', true, :recordedAt, :recordedAt
				    ),
				    (
				        'POLL_FUTURE', 'FAILED', null, null,
				        4, 3, 4, 3, :futureAt, :failedAt, :failedAt,
				        'MANIFEST_HTTP_ERROR', true, :recordedAt, :recordedAt
				    ),
				    (
				        'POLL_ACTIVE', 'POLLING', :pollToken, :leaseExpiresAt,
				        1, 0, 0, 3, null, :recordedAt, null,
				        null, null, :recordedAt, :recordedAt
				    )
				""")
				.param("dueAt", Timestamp.from(DUE_AT))
				.param("futureAt", Timestamp.from(FUTURE_AT))
				.param("failedAt", Timestamp.from(RECORDED_AT.minus(Duration.ofMinutes(30))))
				.param("pollToken", POLL_TOKEN)
				.param("leaseExpiresAt", Timestamp.from(LEASE_EXPIRES_AT))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void seedIndexTarget() {
		jdbcClient.sql("""
				insert into index_logical_partitions (
				    partition_key,
				    partition_start_at,
				    partition_end_at,
				    partition_interval,
				    state_version,
				    created_at,
				    updated_at
				)
				values (
				    :partitionKey,
				    :partitionStart,
				    :partitionEnd,
				    'P7D',
				    2,
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("partitionKey", PARTITION_KEY)
				.param("partitionStart", Timestamp.from(WINDOW_FROM))
				.param("partitionEnd", Timestamp.from(WINDOW_FROM.plus(Duration.ofDays(7))))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
		jdbcClient.sql("""
				insert into index_generations (
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid,
				    state_version,
				    heartbeat_at,
				    activated_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    1,
				    'ACTIVE',
				    :eventIndexName,
				    :eventIndexUuid,
				    :mentionIndexName,
				    :mentionIndexUuid,
				    1,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("generationUuid", GENERATION_UUID)
				.param("partitionKey", PARTITION_KEY)
				.param("eventIndexName", EVENT_INDEX_NAME)
				.param("eventIndexUuid", EVENT_INDEX_UUID)
				.param("mentionIndexName", MENTION_INDEX_NAME)
				.param("mentionIndexUuid", MENTION_INDEX_UUID)
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void seedAcquisitionAndProcessingStates() {
		long dueRun = insertRun("MIGRATION_FIXTURE", Instant.parse("2026-07-21T02:00:00Z"));
		long futureRun = insertRun("MIGRATION_FIXTURE", Instant.parse("2026-07-21T02:15:00Z"));
		long activeRun = insertRun("MIGRATION_FIXTURE", Instant.parse("2026-07-21T02:30:00Z"));
		long processingRun = insertRun("MIGRATION_FIXTURE", Instant.parse("2026-07-21T02:45:00Z"));
		long failedProcessingRun = insertRun(
				"MIGRATION_FIXTURE",
				Instant.parse("2026-07-21T03:00:00Z"));

		insertFailedArchive(dueRun, "archive-due", DUE_AT);
		insertFailedArchive(futureRun, "archive-future", FUTURE_AT);
		insertActiveArchive(activeRun, "archive-active");
		insertStagedArchive(processingRun, "processing-active");
		insertStagedArchive(failedProcessingRun, "processing-failed");
		insertActiveProcessing("processing-active");
		insertFailedProcessing("processing-failed");
	}

	private long insertRun(String sourceName, Instant updateTime) {
		return jdbcClient.sql("""
				insert into ingestion_runs (
				    source_name,
				    source_update_time,
				    status,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				values (
				    :sourceName,
				    :sourceUpdateTime,
				    'IN_PROGRESS',
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				)
				returning id
				""")
				.param("sourceName", sourceName)
				.param("sourceUpdateTime", Timestamp.from(updateTime))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.query(Long.class)
				.single();
	}

	private void insertFailedArchive(long runId, String key, Instant retryNotBefore) {
		jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key,
				    run_id,
				    source_update_time,
				    archive_name,
				    metadata_url,
				    expected_md5,
				    archive_type,
				    status,
				    failed_at,
				    file_size_bytes,
				    first_seen_at,
				    last_attempt_at,
				    total_attempt_count,
				    automatic_retries_used,
				    consecutive_retryable_failures,
				    automatic_retry_limit,
				    retry_not_before,
				    last_error_code,
				    last_error_retryable,
				    created_at,
				    updated_at
				)
				select
				    :idempotencyKey,
				    ingestion_run.id,
				    ingestion_run.source_update_time,
				    :idempotencyKey || '.zip',
				    'https://storage.googleapis.com/gdelt-open-data/events/'
				        || :idempotencyKey || '.zip',
				    repeat('a', 32),
				    'TRANSLATION_EVENTS',
				    'FAILED',
				    :failedAt,
				    1,
				    :recordedAt,
				    :failedAt,
				    4,
				    3,
				    4,
				    3,
				    :retryNotBefore,
				    'DOWNLOAD_TIMEOUT',
				    true,
				    :recordedAt,
				    :recordedAt
				from ingestion_runs ingestion_run
				where ingestion_run.id = :runId
				""")
				.param("idempotencyKey", key)
				.param("runId", runId)
				.param("failedAt", Timestamp.from(RECORDED_AT.minus(Duration.ofMinutes(30))))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("retryNotBefore", Timestamp.from(retryNotBefore))
				.update();
	}

	private void insertActiveArchive(long runId, String key) {
		jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key,
				    run_id,
				    source_update_time,
				    archive_name,
				    metadata_url,
				    expected_md5,
				    archive_type,
				    status,
				    attempt_token,
				    lease_expires_at,
				    file_size_bytes,
				    first_seen_at,
				    last_attempt_at,
				    total_attempt_count,
				    automatic_retry_limit,
				    created_at,
				    updated_at
				)
				select
				    :idempotencyKey,
				    ingestion_run.id,
				    ingestion_run.source_update_time,
				    :idempotencyKey || '.zip',
				    'https://storage.googleapis.com/gdelt-open-data/events/'
				        || :idempotencyKey || '.zip',
				    repeat('b', 32),
				    'TRANSLATION_EVENTS',
				    'PROCESSING',
				    :attemptToken,
				    :leaseExpiresAt,
				    1,
				    :recordedAt,
				    :recordedAt,
				    1,
				    3,
				    :recordedAt,
				    :recordedAt
				from ingestion_runs ingestion_run
				where ingestion_run.id = :runId
				""")
				.param("idempotencyKey", key)
				.param("runId", runId)
				.param("attemptToken", ARCHIVE_TOKEN)
				.param("leaseExpiresAt", Timestamp.from(LEASE_EXPIRES_AT))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void insertStagedArchive(long runId, String key) {
		jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key,
				    run_id,
				    source_update_time,
				    archive_name,
				    metadata_url,
				    expected_md5,
				    actual_md5,
				    archive_type,
				    status,
				    file_size_bytes,
				    actual_size_bytes,
				    staged_archive_path,
				    staged_csv_path,
				    first_seen_at,
				    last_attempt_at,
				    completed_at,
				    total_attempt_count,
				    automatic_retry_limit,
				    created_at,
				    updated_at
				)
				select
				    :idempotencyKey,
				    ingestion_run.id,
				    ingestion_run.source_update_time,
				    :idempotencyKey || '.zip',
				    'https://storage.googleapis.com/gdelt-open-data/events/'
				        || :idempotencyKey || '.zip',
				    repeat('c', 32),
				    repeat('c', 32),
				    'TRANSLATION_EVENTS',
				    'STAGED',
				    1,
				    1,
				    'staging/' || :idempotencyKey || '.zip',
				    'staging/' || :idempotencyKey || '.csv',
				    :recordedAt,
				    :recordedAt,
				    :recordedAt,
				    1,
				    3,
				    :recordedAt,
				    :recordedAt
				from ingestion_runs ingestion_run
				where ingestion_run.id = :runId
				""")
				.param("idempotencyKey", key)
				.param("runId", runId)
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void insertActiveProcessing(String archiveKey) {
		jdbcClient.sql("""
				insert into ingestion_archive_processing (
				    archive_idempotency_key,
				    source_fingerprint,
				    projection_revision,
				    processing_fingerprint,
				    logical_partition_key,
				    status,
				    attempt_token,
				    lease_expires_at,
				    bound_partition_state_version,
				    bound_generation_id,
				    bound_generation_uuid,
				    bound_index_kind,
				    bound_index_name,
				    bound_index_uuid,
				    total_attempt_count,
				    automatic_retry_limit,
				    last_attempt_at,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				select
				    :archiveKey,
				    'source-' || :archiveKey,
				    'event-v1',
				    repeat('d', 64),
				    logical_partition.partition_key,
				    'PROCESSING',
				    :attemptToken,
				    :leaseExpiresAt,
				    logical_partition.state_version,
				    generation.id,
				    generation.generation_uuid,
				    'EVENT',
				    generation.event_index_name,
				    generation.event_index_uuid,
				    1,
				    3,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				from index_logical_partitions logical_partition
				join index_generations generation
				  on generation.partition_key = logical_partition.partition_key
				 and generation.state = 'ACTIVE'
				where logical_partition.partition_key = :partitionKey
				""")
				.param("archiveKey", archiveKey)
				.param("attemptToken", PROCESSING_TOKEN)
				.param("leaseExpiresAt", Timestamp.from(LEASE_EXPIRES_AT))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("partitionKey", PARTITION_KEY)
				.update();
	}

	private void insertFailedProcessing(String archiveKey) {
		jdbcClient.sql("""
				insert into ingestion_archive_processing (
				    archive_idempotency_key,
				    source_fingerprint,
				    projection_revision,
				    processing_fingerprint,
				    status,
				    total_attempt_count,
				    automatic_retries_used,
				    consecutive_retryable_failures,
				    automatic_retry_limit,
				    retry_not_before,
				    last_attempt_at,
				    failed_at,
				    last_error_code,
				    last_error_retryable,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				values (
				    :archiveKey,
				    'source-' || :archiveKey,
				    'event-v1',
				    repeat('e', 64),
				    'FAILED',
				    4,
				    3,
				    4,
				    3,
				    :retryNotBefore,
				    :failedAt,
				    :failedAt,
				    'INDEX_BULK_REQUEST_FAILED',
				    true,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("archiveKey", archiveKey)
				.param("retryNotBefore", Timestamp.from(DUE_AT))
				.param("failedAt", Timestamp.from(RECORDED_AT.minus(Duration.ofMinutes(30))))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void assertSourceWatermarksAndPoliciesPreserved() {
		assertThat(sourceState("GDELT"))
				.isEqualTo(new SourceState("LATEST", LATEST_UPDATE, LATEST_UPDATE));
		assertThat(sourceState("GDELT_FIXED"))
				.isEqualTo(new SourceState("FIXED", WINDOW_FROM, LATEST_UPDATE));
		assertThat(jdbcClient.sql("select count(*) from ingestion_runs")
				.query(Integer.class).single()).isEqualTo(5);
		assertThat(jdbcClient.sql("select count(*) from ingestion_archives")
				.query(Integer.class).single()).isEqualTo(5);
		assertThat(jdbcClient.sql("select count(*) from ingestion_archive_processing")
				.query(Integer.class).single()).isEqualTo(2);
	}

	private List<UpgradeRowSnapshot> upgradeIdentitySnapshot() {
		return jdbcClient.sql("""
				select table_name, row_identity, state_version
				from (
				    select
				        'ingestion_runs' as table_name,
				        concat_ws(
				            '|', id::text, source_name, source_update_time::text
				        ) as row_identity,
				        null::bigint as state_version
				    from ingestion_runs
				    union all
				    select
				        'ingestion_source_state',
				        source_name,
				        null::bigint
				    from ingestion_source_state
				    union all
				    select
				        'ingestion_source_poll_state',
				        source_name,
				        state_version
				    from ingestion_source_poll_state
				    union all
				    select
				        'ingestion_archives',
				        concat_ws('|', idempotency_key, run_id::text, archive_type),
				        state_version
				    from ingestion_archives
				    union all
				    select
				        'index_logical_partitions',
				        concat_ws(
				            '|',
				            partition_key,
				            partition_start_at::text,
				            partition_end_at::text
				        ),
				        state_version
				    from index_logical_partitions
				    union all
				    select
				        'index_generations',
				        concat_ws(
				            '|',
				            id::text,
				            generation_uuid::text,
				            partition_key,
				            generation_number::text,
				            event_index_name,
				            event_index_uuid,
				            mention_index_name,
				            mention_index_uuid
				        ),
				        state_version
				    from index_generations
				    union all
				    select
				        'ingestion_archive_processing',
				        concat_ws(
				            '|',
				            archive_idempotency_key,
				            processing_fingerprint,
				            bound_generation_id::text,
				            bound_generation_uuid::text,
				            bound_index_kind,
				            bound_index_name,
				            bound_index_uuid
				        ),
				        state_version
				    from ingestion_archive_processing
				) upgrade_rows
				order by table_name, row_identity
				""")
				.query((resultSet, rowNumber) -> new UpgradeRowSnapshot(
						resultSet.getString("table_name"),
						resultSet.getString("row_identity"),
						resultSet.getObject("state_version", Long.class)))
				.list();
	}

	private void assertRetryAndOwnershipEvidencePreserved() {
		assertThat(retryEvidence("ingestion_source_poll_state", "source_name", "POLL_DUE"))
				.isEqualTo(new RetryEvidence(
						1,
						RECORDED_AT.minus(Duration.ofMinutes(30)),
						"MANIFEST_TIMEOUT",
						"MANIFEST_TIMEOUT",
						DUE_AT));
		assertThat(retryEvidence("ingestion_source_poll_state", "source_name", "POLL_FUTURE"))
				.isEqualTo(new RetryEvidence(
						1,
						RECORDED_AT.minus(Duration.ofMinutes(30)),
						"MANIFEST_HTTP_ERROR",
						"MANIFEST_HTTP_ERROR",
						FUTURE_AT));
		assertThat(retryEvidence("ingestion_archives", "idempotency_key", "archive-due"))
				.isEqualTo(new RetryEvidence(
						1,
						RECORDED_AT.minus(Duration.ofMinutes(30)),
						"DOWNLOAD_TIMEOUT",
						"DOWNLOAD_TIMEOUT",
						DUE_AT));
		assertThat(retryEvidence("ingestion_archives", "idempotency_key", "archive-future"))
				.isEqualTo(new RetryEvidence(
						1,
						RECORDED_AT.minus(Duration.ofMinutes(30)),
						"DOWNLOAD_TIMEOUT",
						"DOWNLOAD_TIMEOUT",
						FUTURE_AT));
		assertThat(retryEvidence(
				"ingestion_archive_processing",
				"archive_idempotency_key",
				"processing-failed"))
				.isEqualTo(new RetryEvidence(
						1,
						RECORDED_AT.minus(Duration.ofMinutes(30)),
						"INDEX_BULK_REQUEST_FAILED",
						"INDEX_BULK_REQUEST_FAILED",
						DUE_AT));
		assertThat(retryIsDue("ingestion_source_poll_state", "source_name", "POLL_DUE"))
				.isTrue();
		assertThat(retryIsDue("ingestion_source_poll_state", "source_name", "POLL_FUTURE"))
				.isFalse();
		assertThat(retryIsDue("ingestion_archives", "idempotency_key", "archive-due"))
				.isTrue();
		assertThat(retryIsDue("ingestion_archives", "idempotency_key", "archive-future"))
				.isFalse();
		assertThat(retryIsDue(
				"ingestion_archive_processing",
				"archive_idempotency_key",
				"processing-failed"))
				.isTrue();

		assertThat(activeOwnership(
				"ingestion_source_poll_state",
				"source_name",
				"POLL_ACTIVE"))
				.isEqualTo(new ActiveOwnership(POLL_TOKEN, LEASE_EXPIRES_AT));
		assertThat(activeOwnership(
				"ingestion_archives",
				"idempotency_key",
				"archive-active"))
				.isEqualTo(new ActiveOwnership(ARCHIVE_TOKEN, LEASE_EXPIRES_AT));
		assertThat(activeOwnership(
				"ingestion_archive_processing",
				"archive_idempotency_key",
				"processing-active"))
				.isEqualTo(new ActiveOwnership(PROCESSING_TOKEN, LEASE_EXPIRES_AT));
		assertThat(ownershipIsCurrent(
				"ingestion_source_poll_state",
				"source_name",
				"POLL_ACTIVE"))
				.isTrue();
		assertThat(ownershipIsCurrent(
				"ingestion_archives",
				"idempotency_key",
				"archive-active"))
				.isTrue();
		assertThat(ownershipIsCurrent(
				"ingestion_archive_processing",
				"archive_idempotency_key",
				"processing-active"))
				.isTrue();
	}

	private void assertNewSchemaConstraintsAndCursors() {
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				)
				values (
				    'GDELT_RECENT',
				    :windowFrom,
				    :latestUpdate,
				    'RECENT_WINDOW',
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("latestUpdate", Timestamp.from(LATEST_UPDATE))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
		assertThatThrownBy(() -> jdbcClient.sql("""
					insert into ingestion_source_state (
					    source_name,
					    continuity_baseline,
					    latest_observed_update_time,
					    first_run_policy,
					    initialized_at,
					    updated_at
					)
					values (
					    'GDELT_INVALID',
					    :windowFrom,
					    :latestUpdate,
					    'RECENT',
					    :recordedAt,
					    :recordedAt
					)
					""")
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("latestUpdate", Timestamp.from(LATEST_UPDATE))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update())
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining("ck_ingestion_source_state_policy");

		jdbcClient.sql("""
				insert into ingestion_recent_recovery_plan (
				    source_name,
				    generation,
				    window_from,
				    window_to,
				    source_frontier,
				    catalog_status,
				    created_at,
				    updated_at
				)
				values (
				    'GDELT',
				    1,
				    :windowFrom,
				    :windowTo,
				    :sourceFrontier,
				    'PENDING',
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("windowTo", Timestamp.from(WINDOW_TO))
				.param("sourceFrontier", Timestamp.from(LATEST_UPDATE))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
		assertThatThrownBy(() -> jdbcClient.sql("""
					update ingestion_recent_recovery_plan
					set catalog_status = 'CATALOG_COMPLETE'
					where source_name = 'GDELT'
					""").update())
				.isInstanceOf(DataIntegrityViolationException.class)
				.hasMessageContaining("ck_ingestion_recent_recovery_plan_complete");
		jdbcClient.sql("""
				update ingestion_recent_recovery_plan
				set catalog_status = 'CATALOG_COMPLETE',
				    master_generation = '123456789',
				    master_etag = 'migration-etag',
				    master_verified_from = :windowFrom,
				    master_verified_at = :recordedAt,
				    state_version = state_version + 1,
				    updated_at = :recordedAt
				where source_name = 'GDELT'
				""")
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();

		jdbcClient.sql("""
				insert into ingestion_receipt_audit_state (
				    source_name,
				    status,
				    due_at,
				    automatic_retry_limit,
				    created_at,
				    updated_at
				)
				values (
				    'GDELT',
				    'IDLE',
				    :dueAt,
				    3,
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("dueAt", Timestamp.from(DUE_AT))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
		assertThat(jdbcClient.sql("""
				select due_at
				from ingestion_receipt_audit_state
				where source_name = 'GDELT'
				""")
				.query((resultSet, rowNumber) -> resultSet.getTimestamp(1).toInstant())
				.single()).isEqualTo(DUE_AT);
	}

	private void backfillExistingLatestSource() {
		jdbcClient.sql("""
				update ingestion_source_state
				set continuity_baseline = :windowFrom,
				    updated_at = :recordedAt
				where source_name = 'GDELT'
				""")
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
		insertCompleteRuns();
		insertCompleteEventArchives();
		insertCompleteEventProcessing();
	}

	private void insertCompleteRuns() {
		jdbcClient.sql("""
				insert into ingestion_runs (
				    source_name,
				    source_update_time,
				    status,
				    first_seen_at,
				    completed_at,
				    created_at,
				    updated_at
				)
				select
				    'GDELT',
				    source_update_time,
				    'STAGED',
				    :recordedAt,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				from generate_series(
				    cast(:windowFrom as timestamptz),
				    cast(:windowTo as timestamptz) - interval '15 minutes',
				    interval '15 minutes'
				) as source_update_time
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("windowTo", Timestamp.from(WINDOW_TO))
				.update();
	}

	private void insertCompleteEventArchives() {
		jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key,
				    run_id,
				    source_update_time,
				    archive_name,
				    metadata_url,
				    expected_md5,
				    actual_md5,
				    archive_type,
				    status,
				    file_size_bytes,
				    actual_size_bytes,
				    staged_archive_path,
				    staged_csv_path,
				    first_seen_at,
				    last_attempt_at,
				    completed_at,
				    total_attempt_count,
				    automatic_retry_limit,
				    created_at,
				    updated_at
				)
				select
				    'event:' || ingestion_run.id,
				    ingestion_run.id,
				    ingestion_run.source_update_time,
				    'event-' || ingestion_run.id || '.zip',
				    'https://storage.googleapis.com/gdelt-open-data/events/event-'
				        || ingestion_run.id || '.zip',
				    repeat('f', 32),
				    repeat('f', 32),
				    'TRANSLATION_EVENTS',
				    'STAGED',
				    1,
				    1,
				    'staging/event-' || ingestion_run.id || '.zip',
				    'staging/event-' || ingestion_run.id || '.csv',
				    :recordedAt,
				    :recordedAt,
				    :recordedAt,
				    1,
				    3,
				    :recordedAt,
				    :recordedAt
				from ingestion_runs ingestion_run
				where ingestion_run.source_name = 'GDELT'
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void insertCompleteEventProcessing() {
		jdbcClient.sql("""
				insert into ingestion_archive_processing (
				    archive_idempotency_key,
				    source_fingerprint,
				    projection_revision,
				    processing_fingerprint,
				    logical_partition_key,
				    status,
				    bound_partition_state_version,
				    bound_generation_id,
				    bound_generation_uuid,
				    bound_index_kind,
				    bound_index_name,
				    bound_index_uuid,
				    total_attempt_count,
				    automatic_retry_limit,
				    last_attempt_at,
				    delivered_records,
				    submitted_operations,
				    succeeded_operations,
				    expected_document_count,
				    receipt_digest_algorithm,
				    expected_identity_digest,
				    verified_generation_id,
				    verified_index_uuid,
				    actual_document_count,
				    actual_identity_digest,
				    receipt_verified_at,
				    completed_at,
				    state_version,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				select
				    archive.idempotency_key,
				    'source:' || archive.idempotency_key,
				    'event-v1',
				    md5(archive.idempotency_key) || md5('processing:' || archive.idempotency_key),
				    logical_partition.partition_key,
				    'INDEXED',
				    logical_partition.state_version,
				    generation.id,
				    generation.generation_uuid,
				    'EVENT',
				    generation.event_index_name,
				    generation.event_index_uuid,
				    1,
				    3,
				    :recordedAt,
				    1,
				    1,
				    1,
				    1,
				    'sha256-length-prefix-v1',
				    md5('identity:' || archive.idempotency_key)
				        || md5('identity-2:' || archive.idempotency_key),
				    generation.id,
				    generation.event_index_uuid,
				    1,
				    md5('identity:' || archive.idempotency_key)
				        || md5('identity-2:' || archive.idempotency_key),
				    :recordedAt,
				    :recordedAt,
				    2,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				from ingestion_archives archive
				join index_logical_partitions logical_partition
				  on archive.source_update_time >= logical_partition.partition_start_at
				 and archive.source_update_time < logical_partition.partition_end_at
				join index_generations generation
				  on generation.partition_key = logical_partition.partition_key
				 and generation.state = 'ACTIVE'
				where archive.idempotency_key like 'event:%'
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private SourceState sourceState(String sourceName) {
		return jdbcClient.sql("""
				select
				    first_run_policy,
				    continuity_baseline,
				    latest_observed_update_time
				from ingestion_source_state
				where source_name = :sourceName
				""")
				.param("sourceName", sourceName)
				.query((resultSet, rowNumber) -> new SourceState(
						resultSet.getString("first_run_policy"),
						resultSet.getTimestamp("continuity_baseline").toInstant(),
						resultSet.getTimestamp("latest_observed_update_time").toInstant()))
				.single();
	}

	private RetryEvidence retryEvidence(String table, String keyColumn, String key) {
		String sql = """
				select
				    retry_sequence,
				    last_exhausted_at,
				    last_exhausted_error_code,
				    last_error_code,
				    retry_not_before
				from %s
				where %s = :key
				""".formatted(table, keyColumn);
		return jdbcClient.sql(sql)
				.param("key", key)
				.query((resultSet, rowNumber) -> new RetryEvidence(
						resultSet.getLong("retry_sequence"),
						resultSet.getTimestamp("last_exhausted_at").toInstant(),
						resultSet.getString("last_exhausted_error_code"),
						resultSet.getString("last_error_code"),
						resultSet.getTimestamp("retry_not_before").toInstant()))
				.single();
	}

	private ActiveOwnership activeOwnership(String table, String keyColumn, String key) {
		String sql = """
				select attempt_token, lease_expires_at
				from %s
				where %s = :key
				""".formatted(table, keyColumn);
		return jdbcClient.sql(sql)
				.param("key", key)
				.query((resultSet, rowNumber) -> new ActiveOwnership(
						resultSet.getObject("attempt_token", UUID.class),
						resultSet.getTimestamp("lease_expires_at").toInstant()))
				.single();
	}

	private boolean retryIsDue(String table, String keyColumn, String key) {
		String sql = """
				select retry_not_before <= clock_timestamp()
				from %s
				where %s = :key
				""".formatted(table, keyColumn);
		return jdbcClient.sql(sql)
				.param("key", key)
				.query(Boolean.class)
				.single();
	}

	private boolean ownershipIsCurrent(String table, String keyColumn, String key) {
		String sql = """
				select lease_expires_at > clock_timestamp()
				from %s
				where %s = :key
				""".formatted(table, keyColumn);
		return jdbcClient.sql(sql)
				.param("key", key)
				.query(Boolean.class)
				.single();
	}

	private int successfulSqlMigrations() {
		return jdbcClient.sql("""
				select count(*)
				from flyway_schema_history
				where type = 'SQL' and success
				""")
				.query(Integer.class)
				.single();
	}

	private record SourceState(
			String firstRunPolicy,
			Instant continuityBaseline,
			Instant latestObserved
	) {
	}

	private record RetryEvidence(
			long sequence,
			Instant exhaustedAt,
			String exhaustedErrorCode,
			String currentErrorCode,
			Instant retryNotBefore
	) {
	}

	private record ActiveOwnership(UUID token, Instant leaseExpiresAt) {
	}

	private record UpgradeRowSnapshot(
			String tableName,
			String rowIdentity,
			Long stateVersion
	) {
	}
}
