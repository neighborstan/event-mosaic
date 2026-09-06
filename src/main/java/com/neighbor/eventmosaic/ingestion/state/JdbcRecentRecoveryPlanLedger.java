package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Хранит в PostgreSQL план восстановления данных GDELT за последние сутки. Транзакции не дают частично обновить план или
 * применить результат проверки к уже устаревшей его версии.
 */
@Repository
public class JdbcRecentRecoveryPlanLedger implements RecentRecoveryPlanLedger {

	private static final String PARAM_SOURCE_NAME = "sourceName";

	private final JdbcClient jdbcClient;
	private final JdbcIngestionRunRepository runRepository;
	private final JdbcIngestionArchiveRepository archiveRepository;
	private final Clock clock;

	/**
	 * Создает хранилище плана на общем доступе к базе данных и существующих хранилищах запусков и архивов.
	 */
	public JdbcRecentRecoveryPlanLedger(
			JdbcClient jdbcClient,
			JdbcIngestionRunRepository runRepository,
			JdbcIngestionArchiveRepository archiveRepository,
			Clock clock
	) {
		this.jdbcClient = Objects.requireNonNull(jdbcClient, "jdbcClient must not be null");
		this.runRepository = Objects.requireNonNull(runRepository, "runRepository must not be null");
		this.archiveRepository = Objects.requireNonNull(
				archiveRepository, "archiveRepository must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	@Override
	@Transactional
	public ActivationResult activate(DiscoveredUpdate newestUpdate, RecentWindowPlan plan) {
		Objects.requireNonNull(newestUpdate, "newestUpdate must not be null");
		Objects.requireNonNull(plan, "plan must not be null");
		if (!newestUpdate.sourceUpdateTime().equals(plan.sourceFrontier())) {
			throw new IllegalArgumentException("newest update must match the captured source frontier");
		}

		Instant now = clock.instant();
		Optional<SourceState> source = findSourceForUpdate();
		boolean sourceCreated = source.isEmpty() && insertInitialSource(plan, now) == 1;
		source = findSourceForUpdate();
		if (source.isEmpty()) {
			throw new IllegalStateException("source state was not created");
		}
		Optional<PlanState> current = findPlanForUpdate();
		SourceState durableSource = source.orElseThrow();
		if (!sourceCreated && durableSource.latestObserved().isAfter(plan.sourceFrontier())) {
			return new ActivationResult(ActivationOutcome.STALE_FRONTIER, current, 0);
		}
		if (current.isPresent()
				&& current.orElseThrow().revision().sourceFrontier().isAfter(plan.sourceFrontier())) {
			return new ActivationResult(ActivationOutcome.STALE_FRONTIER, current, 0);
		}

		registerUpdate(newestUpdate, now);
		if (current.isEmpty()) {
			if (!sourceCreated) {
				activateExistingSource(durableSource, plan, now);
			}
			insertPlan(plan, now);
			int gaps = insertMissingAuditGaps(
					plan,
					plan.windowFrom(),
					plan.sourceFrontier().minus(GdeltSourceContract.UPDATE_INTERVAL),
					now);
			return new ActivationResult(
					ActivationOutcome.ACTIVATED,
					findPlanForUpdate(),
					gaps);
		}

		PlanState existing = current.orElseThrow();
		if (sameRevision(existing.revision(), plan)) {
			return new ActivationResult(ActivationOutcome.UNCHANGED, current, 0);
		}
		Instant previousLatest = durableSource.latestObserved();
		updateLatestOnly(previousLatest, plan.sourceFrontier(), now);
		revisePlan(existing.revision().generation() + 1, plan, now);
		int gaps = insertMissingAuditGaps(
				plan,
				max(previousLatest.plus(GdeltSourceContract.UPDATE_INTERVAL), plan.windowFrom()),
				plan.sourceFrontier().minus(GdeltSourceContract.UPDATE_INTERVAL),
				now);
		return new ActivationResult(ActivationOutcome.REVISED, findPlanForUpdate(), gaps);
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<PlanState> currentPlan() {
		return findPlan(false);
	}

	@Override
	@Transactional
	public CatalogRegistrationResult registerCatalog(CatalogRegistration registration) {
		Objects.requireNonNull(registration, "registration must not be null");
		PlanState current = findPlanForUpdate()
				.orElseThrow(() -> new IllegalStateException("recent recovery plan is not active"));
		if (!current.revision().equals(registration.revision())) {
			return new CatalogRegistrationResult(CatalogRegistrationOutcome.STALE_PLAN, current);
		}
		if (current.catalogStatus() == CatalogStatus.CATALOG_COMPLETE) {
			return new CatalogRegistrationResult(CatalogRegistrationOutcome.ALREADY_COMPLETE, current);
		}

		RecentWindowPlan plan = RecentWindowPlan.fromBoundaries(
				current.revision().windowFrom(),
				current.revision().windowTo(),
				current.revision().sourceFrontier());
		Set<Instant> targets = Set.copyOf(plan.targetUpdateTimes());
		Instant now = clock.instant();
		for (DiscoveredUpdate update : registration.updates()) {
			if (!targets.contains(update.sourceUpdateTime())) {
				throw new IllegalArgumentException("catalog update is outside the pinned plan");
			}
			registerUpdate(update, now);
		}
		updateCatalogEvidence(registration, now);
		return new CatalogRegistrationResult(
				CatalogRegistrationOutcome.APPLIED,
				findPlanForUpdate().orElseThrow());
	}

	@Override
	@Transactional(readOnly = true)
	public List<Gap> remainingGaps() {
		List<Instant> missing = jdbcClient.sql("""
				with current_plan as (
				    select *
				    from ingestion_recent_recovery_plan
				    where source_name = :sourceName
				), target_slots as (
				    select generate_series(
				        window_from,
				        window_to - interval '15 minutes',
				        interval '15 minutes'
				    ) as missing_time
				    from current_plan
				    union
				    select generate_series(
				        window_to,
				        source_frontier,
				        interval '15 minutes'
				    ) as missing_time
				    from current_plan
				    where source_frontier >= window_to
				    union
				    select source_frontier as missing_time
				    from current_plan
				)
				select slots.missing_time
				from target_slots slots
				where not exists (
				      select 1
				      from ingestion_runs run
				      join ingestion_archives archive on archive.run_id = run.id
				      where run.source_name = :sourceName
				        and run.source_update_time = slots.missing_time
				        and archive.archive_type in ('TRANSLATION_EVENTS', 'TRANSLATION_MENTIONS')
				      group by run.id
				      having count(*) = 2
				  )
				order by slots.missing_time
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query((resultSet, rowNumber) ->
						IngestionJdbcMappers.instant(resultSet, "missing_time"))
				.list();
		return groupGaps(missing);
	}

	@Override
	@Transactional(readOnly = true)
	public GapAuditSummary gapAuditSummary() {
		return jdbcClient.sql("""
				select count(*) as interval_count,
				       coalesce(sum(
				           floor(extract(epoch from (
				               last_missing_update_time - first_missing_update_time
				           )) / 900)::bigint + 1
				       ), 0) as slot_count
				from ingestion_gaps
				where source_name = :sourceName
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query((resultSet, rowNumber) -> new GapAuditSummary(
						resultSet.getLong("interval_count"),
						resultSet.getLong("slot_count")))
				.single();
	}

	@Override
	@Transactional(readOnly = true)
	public List<IngestionArchiveState> archivesNewestFirst(ArchiveType archiveType) {
		Objects.requireNonNull(archiveType, "archiveType must not be null");
		return jdbcClient.sql("""
				select archive.*
				from ingestion_archives archive
				join ingestion_recent_recovery_plan plan
				  on plan.source_name = :sourceName
				where archive.archive_type = :archiveType
				  and (
				      (archive.source_update_time >= plan.window_from
				       and archive.source_update_time < plan.window_to)
				      or (plan.source_frontier >= plan.window_to
				          and archive.source_update_time >= plan.window_to
				          and archive.source_update_time <= plan.source_frontier)
				      or archive.source_update_time = plan.source_frontier
				  )
				order by archive.source_update_time desc, archive.idempotency_key
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("archiveType", archiveType.name())
				.query(IngestionJdbcMappers.ARCHIVE)
				.list();
	}

	private Optional<SourceState> findSourceForUpdate() {
		return jdbcClient.sql("""
				select continuity_baseline, latest_observed_update_time
				from ingestion_source_state
				where source_name = :sourceName
				for update
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query((resultSet, rowNumber) -> new SourceState(
						IngestionJdbcMappers.instant(resultSet, "continuity_baseline"),
						IngestionJdbcMappers.instant(resultSet, "latest_observed_update_time")))
				.optional();
	}

	private int insertInitialSource(RecentWindowPlan plan, Instant now) {
		Instant baseline = min(plan.windowFrom(), plan.sourceFrontier());
		return jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name, continuity_baseline, latest_observed_update_time,
				    first_run_policy, initialized_at, updated_at
				)
				values (:sourceName, :baseline, :latest, 'RECENT_WINDOW', :now, :now)
				on conflict (source_name) do nothing
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("baseline", Timestamp.from(baseline))
				.param("latest", Timestamp.from(plan.sourceFrontier()))
				.param("now", Timestamp.from(now))
				.update();
	}

	private void activateExistingSource(SourceState source, RecentWindowPlan plan, Instant now) {
		jdbcClient.sql("""
				update ingestion_source_state
				set continuity_baseline = :baseline,
				    latest_observed_update_time = :latest,
				    updated_at = :now
				where source_name = :sourceName
				""")
				.param("baseline", Timestamp.from(min(
						source.baseline(), min(plan.windowFrom(), plan.sourceFrontier()))))
				.param("latest", Timestamp.from(max(source.latestObserved(), plan.sourceFrontier())))
				.param("now", Timestamp.from(now))
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.update();
	}

	private void updateLatestOnly(Instant previousLatest, Instant frontier, Instant now) {
		if (!frontier.isAfter(previousLatest)) {
			return;
		}
		jdbcClient.sql("""
				update ingestion_source_state
				set latest_observed_update_time = :latest, updated_at = :now
				where source_name = :sourceName
				""")
				.param("latest", Timestamp.from(frontier))
				.param("now", Timestamp.from(now))
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.update();
	}

	private void registerUpdate(DiscoveredUpdate update, Instant now) {
		long runId = runRepository.findOrCreate(update.sourceUpdateTime(), now);
		for (DiscoveredArchive archive : update.archives()) {
			archiveRepository.register(runId, archive, now);
		}
	}

	private void insertPlan(RecentWindowPlan plan, Instant now) {
		jdbcClient.sql("""
				insert into ingestion_recent_recovery_plan (
				    source_name, generation, window_from, window_to, source_frontier,
				    catalog_status, state_version, created_at, updated_at
				)
				values (:sourceName, 1, :windowFrom, :windowTo, :frontier,
				        'PENDING', 0, :now, :now)
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("windowFrom", Timestamp.from(plan.windowFrom()))
				.param("windowTo", Timestamp.from(plan.windowTo()))
				.param("frontier", Timestamp.from(plan.sourceFrontier()))
				.param("now", Timestamp.from(now))
				.update();
	}

	private void revisePlan(long generation, RecentWindowPlan plan, Instant now) {
		jdbcClient.sql("""
				update ingestion_recent_recovery_plan
				set generation = :generation,
				    window_from = :windowFrom,
				    window_to = :windowTo,
				    source_frontier = :frontier,
				    catalog_status = 'PENDING',
				    master_generation = null,
				    master_etag = null,
				    master_verified_from = null,
				    master_verified_at = null,
				    state_version = state_version + 1,
				    updated_at = :now
				where source_name = :sourceName
				""")
				.param("generation", generation)
				.param("windowFrom", Timestamp.from(plan.windowFrom()))
				.param("windowTo", Timestamp.from(plan.windowTo()))
				.param("frontier", Timestamp.from(plan.sourceFrontier()))
				.param("now", Timestamp.from(now))
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.update();
	}

	private void updateCatalogEvidence(CatalogRegistration registration, Instant now) {
		jdbcClient.sql("""
				update ingestion_recent_recovery_plan
				set catalog_status = :status,
				    master_generation = :masterGeneration,
				    master_etag = :masterEtag,
				    master_verified_from = :masterVerifiedFrom,
				    master_verified_at = :now,
				    state_version = state_version + 1,
				    updated_at = :now
				where source_name = :sourceName
				  and generation = :generation
				""")
				.param("status", registration.catalogComplete()
						? CatalogStatus.CATALOG_COMPLETE.name() : CatalogStatus.PENDING.name())
				.param("masterGeneration", registration.masterGeneration())
				.param("masterEtag", registration.masterEtag())
				.param("masterVerifiedFrom", Timestamp.from(registration.masterVerifiedFrom()))
				.param("now", Timestamp.from(now))
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("generation", registration.revision().generation())
				.update();
	}

	private int insertMissingAuditGaps(
			RecentWindowPlan plan,
			Instant firstCandidate,
			Instant lastCandidate,
			Instant now
	) {
		if (firstCandidate.isAfter(lastCandidate)) {
			return 0;
		}
		Set<Instant> registered = compatiblePairs(plan);
		List<Instant> missing = plan.targetUpdateTimes().stream()
				.filter(slot -> !slot.isBefore(firstCandidate) && !slot.isAfter(lastCandidate))
				.filter(slot -> !registered.contains(slot))
				.toList();
		int inserted = 0;
		for (Gap gap : groupGaps(missing)) {
			inserted += jdbcClient.sql("""
					insert into ingestion_gaps (
					    source_name, first_missing_update_time, last_missing_update_time, detected_at
					)
					values (:sourceName, :firstMissing, :lastMissing, :now)
					on conflict (source_name, first_missing_update_time, last_missing_update_time)
					do nothing
					""")
					.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
					.param("firstMissing", Timestamp.from(gap.firstMissingUpdateTime()))
					.param("lastMissing", Timestamp.from(gap.lastMissingUpdateTime()))
					.param("now", Timestamp.from(now))
					.update();
		}
		return inserted;
	}

	private Set<Instant> compatiblePairs(RecentWindowPlan plan) {
		Instant lower = min(plan.windowFrom(), plan.sourceFrontier());
		Instant upper = max(plan.windowTo().minus(GdeltSourceContract.UPDATE_INTERVAL),
				plan.sourceFrontier());
		return new HashSet<>(jdbcClient.sql("""
				select run.source_update_time
				from ingestion_runs run
				join ingestion_archives archive on archive.run_id = run.id
				where run.source_name = :sourceName
				  and run.source_update_time between :lower and :upper
				  and archive.archive_type in ('TRANSLATION_EVENTS', 'TRANSLATION_MENTIONS')
				group by run.id, run.source_update_time
				having count(*) = 2
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("lower", Timestamp.from(lower))
				.param("upper", Timestamp.from(upper))
				.query((resultSet, rowNumber) ->
						IngestionJdbcMappers.instant(resultSet, "source_update_time"))
				.list());
	}

	private Optional<PlanState> findPlanForUpdate() {
		return findPlan(true);
	}

	private Optional<PlanState> findPlan(boolean forUpdate) {
		String lock = forUpdate ? " for update" : "";
		return jdbcClient.sql("""
				select *
				from ingestion_recent_recovery_plan
				where source_name = :sourceName
				""" + lock)
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query(JdbcRecentRecoveryPlanLedger::mapPlan)
				.optional();
	}

	private static PlanState mapPlan(ResultSet resultSet, int rowNumber) throws SQLException {
		return new PlanState(
				new Revision(
						resultSet.getLong("generation"),
						IngestionJdbcMappers.instant(resultSet, "window_from"),
						IngestionJdbcMappers.instant(resultSet, "window_to"),
						IngestionJdbcMappers.instant(resultSet, "source_frontier")),
				CatalogStatus.valueOf(resultSet.getString("catalog_status")),
				resultSet.getString("master_generation"),
				resultSet.getString("master_etag"),
				IngestionJdbcMappers.nullableInstant(resultSet, "master_verified_from"),
				IngestionJdbcMappers.nullableInstant(resultSet, "master_verified_at"),
				resultSet.getLong("state_version"));
	}

	private static boolean sameRevision(Revision revision, RecentWindowPlan plan) {
		return revision.windowFrom().equals(plan.windowFrom())
				&& revision.windowTo().equals(plan.windowTo())
				&& revision.sourceFrontier().equals(plan.sourceFrontier());
	}

	private static List<Gap> groupGaps(List<Instant> missing) {
		if (missing.isEmpty()) {
			return List.of();
		}
		var gaps = new ArrayList<Gap>();
		Instant first = missing.getFirst();
		Instant previous = first;
		for (int index = 1; index < missing.size(); index++) {
			Instant current = missing.get(index);
			if (!previous.plus(GdeltSourceContract.UPDATE_INTERVAL).equals(current)) {
				gaps.add(new Gap(first, previous));
				first = current;
			}
			previous = current;
		}
		gaps.add(new Gap(first, previous));
		return List.copyOf(gaps);
	}

	private static Instant min(Instant first, Instant second) {
		return first.isBefore(second) ? first : second;
	}

	private static Instant max(Instant first, Instant second) {
		return first.isAfter(second) ? first : second;
	}

	private record SourceState(Instant baseline, Instant latestObserved) {
	}
}
