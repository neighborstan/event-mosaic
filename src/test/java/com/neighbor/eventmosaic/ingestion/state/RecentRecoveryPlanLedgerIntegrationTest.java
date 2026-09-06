package com.neighbor.eventmosaic.ingestion.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger.ActivationOutcome;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger.CatalogRegistration;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger.CatalogRegistrationOutcome;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger.CatalogStatus;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlanner;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Хранение и восстановление recent-плана в PostgreSQL")
class RecentRecoveryPlanLedgerIntegrationTest {

	private static final Instant FRONTIER = Instant.parse("2026-07-20T12:45:00Z");

	@Autowired
	private RecentRecoveryPlanLedger ledger;

	@Autowired
	private RecentWindowPlanner planner;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanLedger() {
		removePlanInsertFault();
		jdbcClient.sql("""
				truncate table ingestion_recent_recovery_plan, ingestion_gaps,
				    ingestion_source_state, ingestion_archives, ingestion_runs
				restart identity cascade
				""").update();
	}

	@AfterEach
	void removeFaultFixture() {
		removePlanInsertFault();
	}

	@Test
	@DisplayName("Первая транзакция атомарно сохраняет watermarks, latest pair, план и audit gap")
	void activatesInitialPlanAtomically() {
		RecentWindowPlan plan = planner.plan(FRONTIER);

		var result = ledger.activate(update(FRONTIER), plan);

		assertThat(result.outcome()).isEqualTo(ActivationOutcome.ACTIVATED);
		assertThat(result.plan().orElseThrow().catalogStatus()).isEqualTo(CatalogStatus.PENDING);
		assertThat(result.auditGapsCreated()).isEqualTo(1);
		assertThat(jdbcClient.sql("select count(*) from ingestion_runs")
				.query(Integer.class).single()).isOne();
		assertThat(jdbcClient.sql("select count(*) from ingestion_archives")
				.query(Integer.class).single()).isEqualTo(2);
		assertThat(sourceState()).satisfies(state -> {
			assertThat(state.baseline()).isEqualTo(plan.windowFrom());
			assertThat(state.latest()).isEqualTo(FRONTIER);
			assertThat(state.policy()).isEqualTo("RECENT_WINDOW");
		});
		assertThat(ledger.remainingGaps()).singleElement().satisfies(gap -> {
			assertThat(gap.firstMissingUpdateTime()).isEqualTo(plan.windowFrom());
			assertThat(gap.lastMissingUpdateTime())
					.isEqualTo(FRONTIER.minus(Duration.ofMinutes(15)));
		});
		assertThat(ledger.gapAuditSummary()).satisfies(summary -> {
			assertThat(summary.intervalCount()).isOne();
			assertThat(summary.slotCount()).isEqualTo(96);
		});
	}

	@Test
	@DisplayName("Сбой при сохранении плана откатывает source, latest pair и audit rows целиком")
	void planInsertFailureRollsBackInitialActivation() {
		RecentWindowPlan plan = planner.plan(FRONTIER);
		installPlanInsertFault();

		assertThatExceptionOfType(DataAccessException.class)
				.isThrownBy(() -> ledger.activate(update(FRONTIER), plan));

		assertThat(ledgerRowCounts()).isEqualTo(new LedgerRowCounts(0, 0, 0, 0, 0));
	}

	@Test
	@DisplayName("Pinned catalog регистрируется одним batch и уменьшает derived gaps, не удаляя audit")
	void registersPinnedCatalogAndKeepsAuditEvidence() {
		RecentWindowPlan plan = planner.plan(FRONTIER);
		var activated = ledger.activate(update(FRONTIER), plan);
		Instant first = plan.windowFrom();
		Instant third = first.plus(Duration.ofMinutes(30));
		var registration = new CatalogRegistration(
				activated.plan().orElseThrow().revision(),
				List.of(update(first), update(third)),
				"123456",
				"catalog-etag",
				first,
				true);

		var applied = ledger.registerCatalog(registration);
		var repeated = ledger.registerCatalog(registration);

		assertThat(applied.outcome()).isEqualTo(CatalogRegistrationOutcome.APPLIED);
		assertThat(applied.plan().catalogStatus()).isEqualTo(CatalogStatus.CATALOG_COMPLETE);
		assertThat(repeated.outcome()).isEqualTo(CatalogRegistrationOutcome.ALREADY_COMPLETE);
		assertThat(jdbcClient.sql("select count(*) from ingestion_runs")
				.query(Integer.class).single()).isEqualTo(3);
		assertThat(ledger.remainingGaps()).hasSize(2);
		assertThat(ledger.gapAuditSummary().slotCount()).isEqualTo(96);
		assertThat(ledger.archivesNewestFirst(ArchiveType.TRANSLATION_EVENTS))
				.extracting(state -> state.archive().sourceUpdateTime())
				.containsExactly(FRONTIER, third, first);
	}

	@Test
	@DisplayName("Новая revision отклоняет старый catalog, а конфликт metadata откатывает весь batch")
	void rejectsStaleCatalogAndRollsBackConflictingBatch() {
		RecentWindowPlan firstPlan = planner.plan(FRONTIER);
		var first = ledger.activate(update(FRONTIER), firstPlan);
		Instant newerFrontier = FRONTIER.plus(Duration.ofMinutes(15));
		RecentWindowPlan secondPlan = planner.plan(newerFrontier);
		var second = ledger.activate(update(newerFrontier), secondPlan);
		Instant target = secondPlan.windowFrom();

		var stale = ledger.registerCatalog(new CatalogRegistration(
				first.plan().orElseThrow().revision(),
				List.of(update(target)),
				"100",
				"old-etag",
				target,
				true));

		assertThat(stale.outcome()).isEqualTo(CatalogRegistrationOutcome.STALE_PLAN);
		assertThat(runCount(target)).isZero();
		var conflicting = new CatalogRegistration(
				second.plan().orElseThrow().revision(),
				List.of(
						update(target),
						update(newerFrontier, "11111111111111111111111111111111")),
				"200",
				"new-etag",
				target,
				true);
		assertThatExceptionOfType(SourceDataViolationException.class)
				.isThrownBy(() -> ledger.registerCatalog(conflicting));

		assertThat(runCount(target)).isZero();
		assertThat(ledger.currentPlan().orElseThrow().catalogStatus())
				.isEqualTo(CatalogStatus.PENDING);
	}

	@Test
	@DisplayName("Устаревший latest не откатывает watermarks и active revision")
	void staleFrontierIsAnIdempotentNoOp() {
		RecentWindowPlan firstPlan = planner.plan(FRONTIER);
		ledger.activate(update(FRONTIER), firstPlan);
		Instant newerFrontier = FRONTIER.plus(Duration.ofMinutes(15));
		RecentWindowPlan currentPlan = planner.plan(newerFrontier);
		var revised = ledger.activate(update(newerFrontier), currentPlan);
		long runCountBefore = totalRunCount();

		var stale = ledger.activate(update(FRONTIER), firstPlan);

		assertThat(revised.outcome()).isEqualTo(ActivationOutcome.REVISED);
		assertThat(stale.outcome()).isEqualTo(ActivationOutcome.STALE_FRONTIER);
		assertThat(stale.plan().orElseThrow().revision())
				.isEqualTo(revised.plan().orElseThrow().revision());
		assertThat(sourceState().latest()).isEqualTo(newerFrontier);
		assertThat(totalRunCount()).isEqualTo(runCountBefore);
	}

	@Test
	@DisplayName("Derived gaps берутся из bounded target set без зависимости от audit rows и старой baseline")
	void derivesBoundedGapsWithoutAuditDriver() {
		RecentWindowPlan plan = planner.plan(FRONTIER);
		Instant historicalBaseline = Instant.parse("2015-02-18T00:00:00Z");
		jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name, continuity_baseline, latest_observed_update_time,
				    first_run_policy, initialized_at, updated_at
				)
				values ('GDELT', :baseline, :latest, 'LATEST', :now, :now)
				""")
				.param("baseline", Timestamp.from(historicalBaseline))
				.param("latest", Timestamp.from(FRONTIER))
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW))
				.update();
		ledger.activate(update(FRONTIER), plan);
		jdbcClient.sql("delete from ingestion_gaps").update();

		assertThat(ledger.remainingGaps()).singleElement().satisfies(gap -> {
			assertThat(gap.firstMissingUpdateTime()).isEqualTo(plan.windowFrom());
			assertThat(gap.lastMissingUpdateTime())
					.isEqualTo(plan.windowTo().minus(Duration.ofMinutes(15)));
		});
		assertThat(sourceState().baseline()).isEqualTo(historicalBaseline);
		assertThat(ledger.gapAuditSummary().slotCount()).isZero();
	}

	private int runCount(Instant updateTime) {
		return jdbcClient.sql("""
				select count(*) from ingestion_runs
				where source_name = 'GDELT' and source_update_time = :updateTime
				""")
				.param("updateTime", Timestamp.from(updateTime))
				.query(Integer.class)
				.single();
	}

	private long totalRunCount() {
		return jdbcClient.sql("select count(*) from ingestion_runs")
				.query(Long.class)
				.single();
	}

	private SourceStateSnapshot sourceState() {
		return jdbcClient.sql("""
				select continuity_baseline, latest_observed_update_time, first_run_policy
				from ingestion_source_state where source_name = 'GDELT'
				""")
				.query((resultSet, rowNumber) -> new SourceStateSnapshot(
						IngestionJdbcMappers.instant(resultSet, "continuity_baseline"),
						IngestionJdbcMappers.instant(resultSet, "latest_observed_update_time"),
						resultSet.getString("first_run_policy")))
				.single();
	}

	private void installPlanInsertFault() {
		jdbcClient.sql("""
				create function test_fail_recent_plan_insert()
				returns trigger
				language plpgsql
				as $function$
				begin
				    raise exception 'test recent plan insert fault';
				end;
				$function$
				""").update();
		jdbcClient.sql("""
				create trigger test_fail_recent_plan_insert
				before insert on ingestion_recent_recovery_plan
				for each row execute function test_fail_recent_plan_insert()
				""").update();
	}

	private void removePlanInsertFault() {
		jdbcClient.sql("""
				drop trigger if exists test_fail_recent_plan_insert
				on ingestion_recent_recovery_plan
				""").update();
		jdbcClient.sql("drop function if exists test_fail_recent_plan_insert()")
				.update();
	}

	private LedgerRowCounts ledgerRowCounts() {
		return jdbcClient.sql("""
				select
				    (select count(*) from ingestion_source_state) as source_count,
				    (select count(*) from ingestion_recent_recovery_plan) as plan_count,
				    (select count(*) from ingestion_gaps) as gap_count,
				    (select count(*) from ingestion_runs) as run_count,
				    (select count(*) from ingestion_archives) as archive_count
				""")
				.query((resultSet, rowNumber) -> new LedgerRowCounts(
						resultSet.getLong("source_count"),
						resultSet.getLong("plan_count"),
						resultSet.getLong("gap_count"),
						resultSet.getLong("run_count"),
						resultSet.getLong("archive_count")))
				.single();
	}

	private record SourceStateSnapshot(Instant baseline, Instant latest, String policy) {
	}

	private record LedgerRowCounts(
			long sourceCount,
			long planCount,
			long gapCount,
			long runCount,
			long archiveCount
	) {
	}
}
