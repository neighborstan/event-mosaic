package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlanner;
import java.sql.Timestamp;
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

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Operational snapshot backend data из PostgreSQL")
class JdbcBackendDataOperationalRepositoryIntegrationTest {

	@Test
	@DisplayName("Диагностика учитывает первый и остальные активные индексы без ложного сообщения о расхождении")
	void includesEveryActiveGenerationInAliasExpectation() {
		for (int offset = 0; offset < 2; offset++) {
			Instant start = Instant.parse("2026-07-13T00:00:00Z").plus(Duration.ofDays(7L * offset));
			String key = offset == 0 ? "p20260713" : "p20260720";
			jdbcClient.sql("""
					insert into index_logical_partitions
					(partition_key, partition_start_at, partition_end_at, partition_interval)
					values (:key, :start, :end, 'P7D')
					""").param("key", key).param("start", Timestamp.from(start))
					.param("end", Timestamp.from(start.plus(Duration.ofDays(7)))).update();
			jdbcClient.sql("""
					insert into index_generations
					(generation_uuid, partition_key, generation_number, state, event_index_name,
					 mention_index_name, event_index_uuid, mention_index_uuid, heartbeat_at, activated_at)
					values (:uuid, :key, 1, 'ACTIVE', :event, :mention, :eventUuid, :mentionUuid, :at, :at)
					""").param("uuid", java.util.UUID.randomUUID()).param("key", key)
					.param("event", "gdelt-events-v1-" + key + "-g0001")
					.param("mention", "gdelt-mentions-v1-" + key + "-g0001")
					.param("eventUuid", "event-uuid-" + key).param("mentionUuid", "mention-uuid-" + key)
					.param("at", Timestamp.from(start)).update();
			var snapshot = repository.read();
			assertThat(snapshot.activeEventIndices()).hasSize(offset + 1)
					.contains("gdelt-events-v1-p20260713-g0001");
			assertThat(snapshot.activeMentionIndices()).hasSize(offset + 1)
					.contains("gdelt-mentions-v1-p20260713-g0001");
		}
	}

	@Autowired
	private JdbcBackendDataOperationalRepository repository;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private RecentRecoveryPlanLedger plans;

	@Autowired
	private RecentWindowPlanner planner;

	@Autowired
	private SourcePollLedger sourcePolls;

	@Autowired
	private IngestionArchiveLedger archives;

	@BeforeEach
	void cleanState() {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    ingestion_source_poll_state,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs,
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Первый известный update без indexed pair дает один полный интервал lag")
	void includesFirstKnownUpdateInLag() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name,
				    continuity_baseline,
				    latest_observed_update_time,
				    first_run_policy,
				    initialized_at,
				    updated_at
				) values (
				    :sourceName,
				    :updateTime,
				    :updateTime,
				    'LATEST',
				    :updateTime,
				    :updateTime
				)
				""")
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("updateTime", Timestamp.from(updateTime))
				.update();

		BackendDataDatabaseSnapshot snapshot = repository.read();

		assertThat(snapshot.lagSeconds()).isEqualTo(900);
		assertThat(snapshot.live().sourceLagSeconds()).isEqualTo(2700);
	}

	@Test
	@DisplayName("Регистрация старой пары уменьшает текущие пропуски и не удаляет историю их обнаружения")
	void derivesCurrentBootstrapAndRemainingGapsFromActivePlan() {
		Instant frontier = Instant.parse("2026-07-20T12:45:00Z");
		var plan = planner.plan(frontier);
		var activated = plans.activate(GdeltTestFixtures.update(frontier), plan);

		BackendDataDatabaseSnapshot initial = repository.read();

		assertThat(initial.openGaps()).isEqualTo(96);
		assertThat(initial.live().eventBootstrapRemaining()).isEqualTo(97);
		assertThat(initial.live().mentionBootstrapRemaining()).isEqualTo(97);
		assertThat(initial.live().catalogPending()).isTrue();
		assertThat(initial.live().sourceLagSeconds()).isZero();
		assertThat(initial.live().successfulPollAgeSeconds()).isEqualTo(-1);

		plans.registerCatalog(new RecentRecoveryPlanLedger.CatalogRegistration(
				activated.plan().orElseThrow().revision(),
				List.of(GdeltTestFixtures.update(plan.windowFrom())),
				"1234", "catalog-version", plan.windowFrom(), true));

		assertThat(repository.read().openGaps()).isEqualTo(95);
		assertThat(repository.read().live().catalogPending()).isFalse();
		assertThat(plans.gapAuditSummary().slotCount()).isEqualTo(96);
	}

	@Test
	@DisplayName("Пауза после исчерпания повторов видна отдельно от возраста последнего успешного опроса")
	void reportsSourceOutageAgeCooldownAndDueSequence() {
		String source = GdeltSourceContract.SOURCE_NAME;
		sourcePolls.register(source);
		var first = sourcePolls.claim(source, Duration.ofMinutes(15)).orElseThrow();
		sourcePolls.markSucceeded(source, first.token());
		var second = sourcePolls.claim(source, Duration.ofMinutes(15)).orElseThrow();
		sourcePolls.markFailed(source, second.token(), new IngestionFailure(IngestionErrorCode.MANIFEST_HTTP_ERROR, true));
		jdbcClient.sql("""
				update ingestion_source_poll_state
				set last_succeeded_at = :lastSucceeded,
				    total_attempt_count = 5,
				    automatic_retries_used = automatic_retry_limit,
				    retry_not_before = :retryAt,
				    last_exhausted_at = :now,
				    last_exhausted_error_code = last_error_code
				where source_name = :source
				""")
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("lastSucceeded", Timestamp.from(FixedClockTestConfiguration.NOW.minus(Duration.ofMinutes(40))))
				.param("retryAt", Timestamp.from(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(10))))
				.param("source", source).update();

		BackendDataDatabaseSnapshot snapshot = repository.read();

		assertThat(snapshot.live().successfulPollAgeSeconds()).isEqualTo(2400);
		assertThat(snapshot.live().sourceOutageAgeSeconds()).isEqualTo(2400);
		assertThat(snapshot.live().sourceRetryDelaySeconds()).isEqualTo(600);
		assertThat(snapshot.live().sourceCooldown()).isTrue();
		assertThat(snapshot.sourcePollRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(0, 1, 1));

		jdbcClient.sql("update ingestion_source_poll_state set retry_not_before = :now where source_name = :source")
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW)).param("source", source).update();
		assertThat(repository.read().sourcePollRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(1, 0, 1));
	}

	@Test
	@DisplayName("Истекшая последняя попытка показывает паузу от конца владения и становится готовой ровно на ее границе")
	void expiredExhaustedSourceLeaseUsesTheSameCooldownAsClaim() {
		String source = GdeltSourceContract.SOURCE_NAME;
		sourcePolls.register(source);
		sourcePolls.claim(source, Duration.ofMinutes(15)).orElseThrow();
		jdbcClient.sql("""
				update ingestion_source_poll_state set last_attempt_at = :started, lease_expires_at = :expired,
				    total_attempt_count = 4, automatic_retries_used = automatic_retry_limit
				where source_name = :source
				""")
				.param("started", Timestamp.from(FixedClockTestConfiguration.NOW.minus(Duration.ofMinutes(20))))
				.param("expired", Timestamp.from(FixedClockTestConfiguration.NOW.minus(Duration.ofMinutes(5))))
				.param("source", source).update();

		BackendDataDatabaseSnapshot cooling = repository.read();

		assertThat(cooling.sourcePollRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(0, 1, 1));
		assertThat(cooling.live().sourceCooldown()).isTrue();
		assertThat(cooling.live().sourceRetryDelaySeconds()).isEqualTo(600);

		jdbcClient.sql("update ingestion_source_poll_state set lease_expires_at = :expired where source_name = :source")
				.param("expired", Timestamp.from(FixedClockTestConfiguration.NOW.minus(Duration.ofMinutes(15))))
				.param("source", source).update();
		assertThat(repository.read().sourcePollRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(1, 0, 1));
		assertThat(repository.read().live().sourceRetryDelaySeconds()).isZero();
	}

	@Test
	@DisplayName("Отложенная проверка сохраненных документов видна как отдельный повтор после отказа инфраструктуры")
	void reportsIndependentReceiptAuditRetries() {
		Instant frontier = Instant.parse("2026-07-20T12:45:00Z");
		plans.activate(GdeltTestFixtures.update(frontier), planner.plan(frontier));
		jdbcClient.sql("""
				insert into ingestion_receipt_audit_state (
				    source_name, status, due_at, total_attempt_count, automatic_retries_used,
				    automatic_retry_limit, retry_not_before, last_attempt_at, failed_at,
				    last_error_code, last_error_retryable, last_exhausted_at,
				    last_exhausted_error_code, last_outcome
				) values ('GDELT', 'FAILED', :now, 4, 3, 3, :retryAt, :now, :now,
				    'INDEXING_IO_ERROR', true, :now, 'INDEXING_IO_ERROR', 'INFRASTRUCTURE_FAILURE')
				""")
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("retryAt", Timestamp.from(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(15))))
				.update();

		BackendDataDatabaseSnapshot snapshot = repository.read();

		assertThat(snapshot.live().receiptAuditRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(0, 1, 1));
		assertThat(snapshot.processingRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(0, 0, 0));
	}

	@Test
	@DisplayName("Готовые повторы старых архивов за пределами текущего плана не увеличивают очередь восстановления")
	void excludesAgedOutArchiveRetries() {
		Instant frontier = Instant.parse("2026-07-20T12:45:00Z");
		var plan = planner.plan(frontier);
		plans.activate(GdeltTestFixtures.update(frontier), plan);
		Instant old = plan.windowFrom().minus(Duration.ofMinutes(15));
		archives.registerDiscoveredUpdate(GdeltTestFixtures.update(old), FirstRunPolicy.LATEST, null);
		for (Instant timestamp : List.of(frontier, old)) {
			String key = GdeltTestFixtures.update(timestamp).archives().stream()
					.filter(archive -> archive.archiveType() == ArchiveType.TRANSLATION_EVENTS)
					.findFirst().orElseThrow().idempotencyKey();
			var attempt = archives.claimArchive(key, Duration.ofMinutes(15)).orElseThrow();
			archives.markFailed(key, attempt.token(), new IngestionFailure(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, true));
		}
		jdbcClient.sql("update ingestion_archives set retry_not_before = :now where status = 'FAILED'")
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW)).update();

		assertThat(repository.read().acquisitionRetries()).isEqualTo(new BackendDataOperationalSnapshot.RetryCounts(1, 0, 0));
	}
}
