package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionGap;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Управляет latest-observed state и наблюдаемыми 15-минутными gaps GDELT.
 */
@Repository
class JdbcIngestionContinuityRepository {

	private static final String PARAM_SOURCE_NAME = "sourceName";

	private final JdbcClient jdbcClient;

	JdbcIngestionContinuityRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	int update(
			Instant updateTime,
			FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt,
			Instant now
	) {
		validateFirstRunArguments(firstRunPolicy, firstRunStartAt, updateTime);
		Optional<Instant> latestObserved = findLatestObservedForUpdate();

		if (latestObserved.isEmpty()) {
			Instant baseline = firstRunPolicy == FirstRunPolicy.LATEST ? updateTime : firstRunStartAt;
			if (insertSourceState(baseline, updateTime, firstRunPolicy, now) == 1) {
				return insertGapIfNeeded(baseline, updateTime, now);
			}
			latestObserved = findLatestObservedForUpdate();
		}

		Instant previousLatest = latestObserved.orElseThrow();
		if (!updateTime.isAfter(previousLatest)) {
			return 0;
		}
		int gapsCreated = insertGapIfNeeded(previousLatest, updateTime, now);
		jdbcClient.sql("""
				update ingestion_source_state
				set latest_observed_update_time = :latestObserved,
				    updated_at = :updatedAt
				where source_name = :sourceName
				""")
				.param("latestObserved", Timestamp.from(updateTime))
				.param("updatedAt", Timestamp.from(now))
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.update();
		return gapsCreated;
	}

	private Optional<Instant> findLatestObservedForUpdate() {
		return jdbcClient.sql("""
				select latest_observed_update_time
				from ingestion_source_state
				where source_name = :sourceName
				for update
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query((resultSet, rowNumber) ->
						IngestionJdbcMappers.instant(resultSet, "latest_observed_update_time"))
				.optional();
	}

	List<IngestionGap> findOpenGaps() {
		return jdbcClient.sql("""
				select *
				from ingestion_gaps
				where source_name = :sourceName
				order by first_missing_update_time
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.query((resultSet, rowNumber) -> new IngestionGap(
						resultSet.getLong("id"),
						IngestionJdbcMappers.instant(resultSet, "first_missing_update_time"),
						IngestionJdbcMappers.instant(resultSet, "last_missing_update_time"),
						IngestionJdbcMappers.instant(resultSet, "detected_at")))
				.list();
	}

	private int insertSourceState(
			Instant baseline,
			Instant updateTime,
			FirstRunPolicy firstRunPolicy,
			Instant now
	) {
		return jdbcClient.sql("""
				insert into ingestion_source_state (
				    source_name, continuity_baseline, latest_observed_update_time,
				    first_run_policy, initialized_at, updated_at
				)
				values (
				    :sourceName, :baseline, :latestObserved,
				    :firstRunPolicy, :initializedAt, :updatedAt
				)
				on conflict (source_name) do nothing
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("baseline", Timestamp.from(baseline))
				.param("latestObserved", Timestamp.from(updateTime))
				.param("firstRunPolicy", firstRunPolicy.name())
				.param("initializedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.update();
	}

	private int insertGapIfNeeded(Instant previous, Instant current, Instant now) {
		Instant firstMissing = previous.plus(GdeltSourceContract.UPDATE_INTERVAL);
		Instant lastMissing = current.minus(GdeltSourceContract.UPDATE_INTERVAL);
		if (firstMissing.isAfter(lastMissing)) {
			return 0;
		}
		return jdbcClient.sql("""
				insert into ingestion_gaps (
				    source_name, first_missing_update_time, last_missing_update_time, detected_at
				)
				values (
				    :sourceName, :firstMissing, :lastMissing, :detectedAt
				)
				on conflict (source_name, first_missing_update_time, last_missing_update_time) do nothing
				""")
				.param(PARAM_SOURCE_NAME, GdeltSourceContract.SOURCE_NAME)
				.param("firstMissing", Timestamp.from(firstMissing))
				.param("lastMissing", Timestamp.from(lastMissing))
				.param("detectedAt", Timestamp.from(now))
				.update();
	}

	private static void validateFirstRunArguments(
			FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt,
			Instant updateTime
	) {
		if (firstRunPolicy == FirstRunPolicy.LATEST && firstRunStartAt != null) {
			throw new IllegalArgumentException("firstRunStartAt must be absent for LATEST firstRunPolicy");
		}
		if (firstRunPolicy == FirstRunPolicy.FIXED && firstRunStartAt == null) {
			throw new IllegalArgumentException("firstRunStartAt is required for FIXED firstRunPolicy");
		}
		if (firstRunStartAt != null && !GdeltSourceContract.isUpdateBoundary(firstRunStartAt)) {
			throw new IllegalArgumentException("firstRunStartAt must align to a 15-minute UTC boundary");
		}
		if (firstRunStartAt != null && firstRunStartAt.isAfter(updateTime)) {
			throw new IllegalArgumentException("firstRunStartAt must not be after discovered update");
		}
	}
}
