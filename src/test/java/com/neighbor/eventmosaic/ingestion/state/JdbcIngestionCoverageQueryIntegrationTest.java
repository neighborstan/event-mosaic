package com.neighbor.eventmosaic.ingestion.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageInterval;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageStatus;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger.ActivationOutcome;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Проверка полноты Event-данных в PostgreSQL")
class JdbcIngestionCoverageQueryIntegrationTest {

	private static final Duration SLOT = Duration.ofMinutes(15);
	private static final Instant WINDOW_FROM = Instant.parse("2026-07-20T00:00:00Z");
	private static final Instant WINDOW_TO = Instant.parse("2026-07-21T00:00:00Z");
	private static final Instant RECORDED_AT = Instant.parse("2026-07-21T01:00:00Z");
	private static final String PARTITION_KEY = "p20260720";
	private static final String ACTIVE_EVENT_INDEX = "gdelt-events-v1-p20260720-g0002";
	private static final String ACTIVE_EVENT_UUID = "event-index-uuid-g0002";
	private static final String ACTIVE_MENTION_INDEX = "gdelt-mentions-v1-p20260720-g0002";
	private static final String ACTIVE_MENTION_UUID = "mention-index-uuid-g0002";
	private static final String OLD_EVENT_INDEX = "gdelt-events-v1-p20260720-g0001";
	private static final String OLD_EVENT_UUID = "event-index-uuid-g0001";
	private static final String OLD_MENTION_INDEX = "gdelt-mentions-v1-p20260720-g0001";
	private static final String OLD_MENTION_UUID = "mention-index-uuid-g0001";

	@Autowired
	private IngestionCoverageQuery coverageQuery;

	@Autowired
	private RecentRecoveryPlanLedger recentRecoveryPlanLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanLedger() {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Без инициализированного continuity state полнота остается неизвестной")
	void uninitializedSourceReturnsUnknown() {
		IngestionCoverageEvidence evidence = coverageQuery.read(WINDOW_FROM, WINDOW_TO);

		assertThat(evidence.status()).isEqualTo(IngestionCoverageStatus.UNKNOWN);
		assertThat(evidence.missingIntervals()).isNull();
	}

	@Test
	@DisplayName("Все 96 Event-слотов с текущими receipts дают полную суточную сетку")
	void currentActiveReceiptsForAllSlotsReturnComplete() {
		prepareCompleteGrid(WINDOW_FROM.plus(Duration.ofHours(7)));

		IngestionCoverageEvidence evidence = coverageQuery.read(WINDOW_FROM, WINDOW_TO);

		assertThat(evidence).isEqualTo(IngestionCoverageEvidence.complete());
		assertThat(indexedEventCount()).isEqualTo(96);
		assertThat(receiptDocumentCount(WINDOW_FROM.plus(Duration.ofHours(7)))).isZero();
	}

	@Test
	@DisplayName("Активация recent-плана расширяет LATEST baseline назад, а 96 Event receipts дают полное окно")
	void recentActivationBackshiftsLatestBaselineAndCompleteGridCoversWindow() {
		insertLatestSourceState(WINDOW_TO);
		RecentWindowPlan plan = RecentWindowPlan.fromBoundaries(
				WINDOW_FROM,
				WINDOW_TO,
				WINDOW_TO);

		var activation = recentRecoveryPlanLedger.activate(update(WINDOW_TO), plan);
		insertPartitionAndGenerations();
		insertRuns();
		insertEventArchives();
		insertIndexedEventProcessing(null);

		assertThat(activation.outcome()).isEqualTo(ActivationOutcome.ACTIVATED);
		assertThat(sourceWatermarks()).satisfies(watermarks -> {
			assertThat(watermarks.baseline()).isEqualTo(WINDOW_FROM);
			assertThat(watermarks.latestObserved()).isEqualTo(WINDOW_TO);
			assertThat(watermarks.firstRunPolicy()).isEqualTo("LATEST");
		});
		assertThat(indexedEventCount()).isEqualTo(96);
		assertThat(coverageQuery.read(WINDOW_FROM, WINDOW_TO))
				.isEqualTo(IngestionCoverageEvidence.complete());
	}

	@Test
	@DisplayName("Baseline, отсутствующие и незавершенные слоты образуют обрезанные объединенные интервалы")
	void baselineGapsPendingAndTrailingSlotsReturnMergedPartialIntervals() {
		prepareCompleteGrid(null);
		Instant baseline = WINDOW_FROM.plus(SLOT);
		Instant latestObserved = WINDOW_TO.minus(Duration.ofMinutes(45));
		updateSourceRange(baseline, latestObserved);
		removeUpdate(WINDOW_FROM.plus(Duration.ofHours(6)));
		removeUpdate(WINDOW_FROM.plus(Duration.ofHours(6)).plus(SLOT));
		makeEventPending(WINDOW_FROM.plus(Duration.ofHours(12)));
		removeUpdate(WINDOW_TO.minus(Duration.ofMinutes(30)));
		removeUpdate(WINDOW_TO.minus(SLOT));

		IngestionCoverageEvidence evidence = coverageQuery.read(WINDOW_FROM, WINDOW_TO);

		assertThat(evidence.status()).isEqualTo(IngestionCoverageStatus.PARTIAL);
		assertThat(evidence.missingIntervals()).containsExactly(
				interval(WINDOW_FROM, WINDOW_FROM.plus(SLOT)),
				interval(
						WINDOW_FROM.plus(Duration.ofHours(6)),
						WINDOW_FROM.plus(Duration.ofHours(6)).plus(Duration.ofMinutes(30))),
				interval(
						WINDOW_FROM.plus(Duration.ofHours(12)),
						WINDOW_FROM.plus(Duration.ofHours(12)).plus(SLOT)),
				interval(WINDOW_TO.minus(Duration.ofMinutes(30)), WINDOW_TO));
	}

	@Test
	@DisplayName("Receipt устаревшего поколения не подтверждает слот текущего ACTIVE поколения")
	void supersededGenerationReceiptLeavesSlotMissing() {
		prepareCompleteGrid(null);
		Instant staleSlot = WINDOW_FROM.plus(Duration.ofHours(9));
		moveReceiptToSupersededGeneration(staleSlot);

		IngestionCoverageEvidence evidence = coverageQuery.read(WINDOW_FROM, WINDOW_TO);

		assertThat(evidence).isEqualTo(IngestionCoverageEvidence.partial(List.of(
				interval(staleSlot, staleSlot.plus(SLOT)))));
	}

	@Test
	@DisplayName("Ошибка Mention и частичный run не ухудшают подтвержденное Event-покрытие")
	void mentionFailureDoesNotAffectEventCoverage() {
		prepareCompleteGrid(null);
		insertFailedMentionAndMarkRunPartial(WINDOW_FROM.plus(Duration.ofHours(4)));

		IngestionCoverageEvidence evidence = coverageQuery.read(WINDOW_FROM, WINDOW_TO);

		assertThat(evidence).isEqualTo(IngestionCoverageEvidence.complete());
	}

	private void prepareCompleteGrid(Instant zeroReceiptAt) {
		insertSourceState(WINDOW_FROM, WINDOW_TO.minus(SLOT));
		insertPartitionAndGenerations();
		insertRuns();
		insertEventArchives();
		insertIndexedEventProcessing(zeroReceiptAt);
	}

	private void insertSourceState(Instant baseline, Instant latestObserved) {
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				)
				values ('GDELT', :baseline, :latestObserved, 'FIXED', :recordedAt, :recordedAt)
				""")
				.param("baseline", Timestamp.from(baseline))
				.param("latestObserved", Timestamp.from(latestObserved))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void insertLatestSourceState(Instant latestObserved) {
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				)
				values ('GDELT', :latestObserved, :latestObserved, 'LATEST', :recordedAt, :recordedAt)
				""")
				.param("latestObserved", Timestamp.from(latestObserved))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void updateSourceRange(Instant baseline, Instant latestObserved) {
		jdbcClient.sql("""
				update ingestion_source_state
				set continuity_baseline = :baseline,
				    latest_observed_update_time = :latestObserved,
				    updated_at = :recordedAt
				where source_name = 'GDELT'
				""")
				.param("baseline", Timestamp.from(baseline))
				.param("latestObserved", Timestamp.from(latestObserved))
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.update();
	}

	private void insertPartitionAndGenerations() {
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

		insertGeneration(
				1,
				"SUPERSEDED",
				OLD_EVENT_INDEX,
				OLD_EVENT_UUID,
				OLD_MENTION_INDEX,
				OLD_MENTION_UUID);
		insertGeneration(
				2,
				"ACTIVE",
				ACTIVE_EVENT_INDEX,
				ACTIVE_EVENT_UUID,
				ACTIVE_MENTION_INDEX,
				ACTIVE_MENTION_UUID);
	}

	private void insertGeneration(
			int generationNumber,
			String state,
			String eventIndexName,
			String eventIndexUuid,
			String mentionIndexName,
			String mentionIndexUuid
	) {
		JdbcClient.StatementSpec statement = jdbcClient.sql("""
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
				    superseded_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    :generationNumber,
				    :state,
				    :eventIndexName,
				    :eventIndexUuid,
				    :mentionIndexName,
				    :mentionIndexUuid,
				    1,
				    :recordedAt,
				    :recordedAt,
				    :supersededAt,
				    :recordedAt,
				    :recordedAt
				)
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", PARTITION_KEY)
				.param("generationNumber", generationNumber)
				.param("state", state)
				.param("eventIndexName", eventIndexName)
				.param("eventIndexUuid", eventIndexUuid)
				.param("mentionIndexName", mentionIndexName)
				.param("mentionIndexUuid", mentionIndexUuid)
				.param("recordedAt", Timestamp.from(RECORDED_AT));
		if ("SUPERSEDED".equals(state)) {
			statement.param("supersededAt", Timestamp.from(RECORDED_AT));
		} else {
			statement.param("supersededAt", null, Types.TIMESTAMP_WITH_TIMEZONE);
		}
		statement.update();
	}

	private void insertRuns() {
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

	private void insertEventArchives() {
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
				    'http://data.gdeltproject.org/gdeltv2/event-' || ingestion_run.id || '.zip',
				    repeat('a', 32),
				    repeat('a', 32),
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
				  and ingestion_run.source_update_time >= :windowFrom
				  and ingestion_run.source_update_time < :windowTo
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("windowTo", Timestamp.from(WINDOW_TO))
				.update();
	}

	private void insertIndexedEventProcessing(Instant zeroReceiptAt) {
		JdbcClient.StatementSpec statement = jdbcClient.sql("""
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
				    source_invalid_records,
				    mapping_rejected_records,
				    submitted_operations,
				    succeeded_operations,
				    failed_operations,
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
				    active_generation.id,
				    active_generation.generation_uuid,
				    'EVENT',
				    active_generation.event_index_name,
				    active_generation.event_index_uuid,
				    1,
				    3,
				    :recordedAt,
				    case when archive.source_update_time = :zeroReceiptAt then 0 else 1 end,
				    0,
				    0,
				    case when archive.source_update_time = :zeroReceiptAt then 0 else 1 end,
				    case when archive.source_update_time = :zeroReceiptAt then 0 else 1 end,
				    0,
				    case when archive.source_update_time = :zeroReceiptAt then 0 else 1 end,
				    'sha256-length-prefix-v1',
				    md5('identity:' || archive.idempotency_key)
				        || md5('identity-2:' || archive.idempotency_key),
				    active_generation.id,
				    active_generation.event_index_uuid,
				    case when archive.source_update_time = :zeroReceiptAt then 0 else 1 end,
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
				join index_generations active_generation
				  on active_generation.partition_key = logical_partition.partition_key
				 and active_generation.state = 'ACTIVE'
				where archive.archive_type = 'TRANSLATION_EVENTS'
				  and archive.source_update_time >= :windowFrom
				  and archive.source_update_time < :windowTo
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("windowFrom", Timestamp.from(WINDOW_FROM))
				.param("windowTo", Timestamp.from(WINDOW_TO));
		if (zeroReceiptAt == null) {
			statement.param("zeroReceiptAt", null, Types.TIMESTAMP_WITH_TIMEZONE);
		} else {
			statement.param("zeroReceiptAt", Timestamp.from(zeroReceiptAt));
		}
		statement.update();
	}

	private void removeUpdate(Instant updateTime) {
		jdbcClient.sql("""
				delete from ingestion_archive_processing processing
				using ingestion_archives archive
				where processing.archive_idempotency_key = archive.idempotency_key
				  and archive.source_update_time = :updateTime
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.update();
		jdbcClient.sql("""
				delete from ingestion_archives
				where source_update_time = :updateTime
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.update();
		jdbcClient.sql("""
				delete from ingestion_runs
				where source_name = 'GDELT'
				  and source_update_time = :updateTime
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.update();
	}

	private void makeEventPending(Instant updateTime) {
		jdbcClient.sql("""
				delete from ingestion_archive_processing processing
				using ingestion_archives archive
				where processing.archive_idempotency_key = archive.idempotency_key
				  and archive.source_update_time = :updateTime
				  and archive.archive_type = 'TRANSLATION_EVENTS'
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.update();
		jdbcClient.sql("""
				insert into ingestion_archive_processing (
				    archive_idempotency_key,
				    source_fingerprint,
				    projection_revision,
				    processing_fingerprint,
				    status,
				    automatic_retry_limit,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				select
				    archive.idempotency_key,
				    'source:' || archive.idempotency_key,
				    'event-v1',
				    md5('pending:' || archive.idempotency_key)
				        || md5('pending-2:' || archive.idempotency_key),
				    'PENDING',
				    3,
				    :recordedAt,
				    :recordedAt,
				    :recordedAt
				from ingestion_archives archive
				where archive.source_update_time = :updateTime
				  and archive.archive_type = 'TRANSLATION_EVENTS'
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("updateTime", Timestamp.from(updateTime))
				.update();
	}

	private void moveReceiptToSupersededGeneration(Instant updateTime) {
		jdbcClient.sql("""
				update ingestion_archive_processing processing
				set bound_generation_id = old_generation.id,
				    bound_generation_uuid = old_generation.generation_uuid,
				    bound_index_name = old_generation.event_index_name,
				    bound_index_uuid = old_generation.event_index_uuid,
				    verified_generation_id = old_generation.id,
				    verified_index_uuid = old_generation.event_index_uuid,
				    updated_at = :recordedAt
				from ingestion_archives archive,
				     index_generations old_generation
				where processing.archive_idempotency_key = archive.idempotency_key
				  and archive.source_update_time = :updateTime
				  and old_generation.partition_key = processing.logical_partition_key
				  and old_generation.generation_number = 1
				  and old_generation.state = 'SUPERSEDED'
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("updateTime", Timestamp.from(updateTime))
				.update();
	}

	private void insertFailedMentionAndMarkRunPartial(Instant updateTime) {
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
				    automatic_retry_limit,
				    last_error_code,
				    last_error_retryable,
				    created_at,
				    updated_at
				)
				select
				    'mention:' || ingestion_run.id,
				    ingestion_run.id,
				    ingestion_run.source_update_time,
				    'mention-' || ingestion_run.id || '.zip',
				    'http://data.gdeltproject.org/gdeltv2/mention-' || ingestion_run.id || '.zip',
				    repeat('b', 32),
				    'TRANSLATION_MENTIONS',
				    'FAILED',
				    :recordedAt,
				    1,
				    :recordedAt,
				    :recordedAt,
				    1,
				    3,
				    'DOWNLOAD_HTTP_STATUS_REJECTED',
				    false,
				    :recordedAt,
				    :recordedAt
				from ingestion_runs ingestion_run
				where ingestion_run.source_name = 'GDELT'
				  and ingestion_run.source_update_time = :updateTime
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("updateTime", Timestamp.from(updateTime))
				.update();
		jdbcClient.sql("""
				update ingestion_runs
				set status = 'PARTIAL',
				    completed_at = null,
				    last_failed_at = :recordedAt,
				    last_error_code = 'DOWNLOAD_HTTP_STATUS_REJECTED',
				    last_error_retryable = false,
				    updated_at = :recordedAt
				where source_name = 'GDELT'
				  and source_update_time = :updateTime
				""")
				.param("recordedAt", Timestamp.from(RECORDED_AT))
				.param("updateTime", Timestamp.from(updateTime))
				.update();
	}

	private long indexedEventCount() {
		return jdbcClient.sql("""
				select count(*)
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.archive_type = 'TRANSLATION_EVENTS'
				  and processing.status = 'INDEXED'
				""")
				.query(Long.class)
				.single();
	}

	private long receiptDocumentCount(Instant updateTime) {
		return jdbcClient.sql("""
				select processing.expected_document_count
				from ingestion_archive_processing processing
				join ingestion_archives archive
				  on archive.idempotency_key = processing.archive_idempotency_key
				where archive.source_update_time = :updateTime
				  and archive.archive_type = 'TRANSLATION_EVENTS'
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.query(Long.class)
				.single();
	}

	private SourceWatermarks sourceWatermarks() {
		return jdbcClient.sql("""
				select continuity_baseline, latest_observed_update_time, first_run_policy
				from ingestion_source_state
				where source_name = 'GDELT'
				""")
				.query((resultSet, rowNumber) -> new SourceWatermarks(
						IngestionJdbcMappers.instant(resultSet, "continuity_baseline"),
						IngestionJdbcMappers.instant(resultSet, "latest_observed_update_time"),
						resultSet.getString("first_run_policy")))
				.single();
	}

	private static IngestionCoverageInterval interval(Instant from, Instant to) {
		return new IngestionCoverageInterval(from, to);
	}

	private record SourceWatermarks(
			Instant baseline,
			Instant latestObserved,
			String firstRunPolicy
	) {
	}
}
