package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.gdelt.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.RecordedIngestionFailure;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Управляет ingestion run rows и их производным status/failure projection.
 */
@Repository
class JdbcIngestionRunRepository {

	private final JdbcClient jdbcClient;

	JdbcIngestionRunRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	long findOrCreate(Instant sourceUpdateTime, Instant now) {
		Optional<Long> inserted = jdbcClient.sql("""
				insert into ingestion_runs (
				    source_name, source_update_time, status, first_seen_at, created_at, updated_at
				)
				values (
				    :sourceName, :sourceUpdateTime, :status, :firstSeenAt, :createdAt, :updatedAt
				)
				on conflict (source_name, source_update_time) do nothing
				returning id
				""")
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("sourceUpdateTime", Timestamp.from(sourceUpdateTime))
				.param("status", IngestionRunStatus.DISCOVERED.name())
				.param("firstSeenAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(Long.class)
				.optional();
		return inserted.orElseGet(() -> findIdByUpdateTime(sourceUpdateTime).orElseThrow());
	}

	Optional<IngestionRunRow> findByUpdateTime(Instant sourceUpdateTime) {
		return jdbcClient.sql("""
				select *
				from ingestion_runs
				where source_name = :sourceName
				  and source_update_time = :sourceUpdateTime
				""")
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("sourceUpdateTime", Timestamp.from(sourceUpdateTime))
				.query(IngestionJdbcMappers.RUN)
				.optional();
	}

	void recalculate(long runId, Instant now) {
		lockRun(runId);
		RunCounts counts = jdbcClient.sql("""
				select
				    count(*) filter (where status = 'STAGED') as staged_count,
				    count(*) filter (where status = 'PROCESSING') as processing_count,
				    count(*) filter (where status = 'FAILED') as failed_count,
				    count(*) filter (where attempt_count > 0) as attempted_count
				from ingestion_archives
				where run_id = :runId
				""")
				.param("runId", runId)
				.query(JdbcIngestionRunRepository::mapCounts)
				.single();
		IngestionRunStatus status = IngestionRunStatusPolicy.derive(counts);
		Optional<RecordedIngestionFailure> failure = findLatestFailure(runId);
		if (failure.isPresent()) {
			updateWithFailure(runId, status, failure.orElseThrow(), now);
		} else {
			updateWithoutFailure(runId, status, now);
		}
	}

	private void lockRun(long runId) {
		jdbcClient.sql("select id from ingestion_runs where id = :runId for update")
				.param("runId", runId)
				.query(Long.class)
				.single();
	}

	private Optional<Long> findIdByUpdateTime(Instant sourceUpdateTime) {
		return jdbcClient.sql("""
				select id
				from ingestion_runs
				where source_name = :sourceName
				  and source_update_time = :sourceUpdateTime
				""")
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("sourceUpdateTime", Timestamp.from(sourceUpdateTime))
				.query(Long.class)
				.optional();
	}

	private Optional<RecordedIngestionFailure> findLatestFailure(long runId) {
		return jdbcClient.sql("""
				select failed_at, last_error_code, last_error_retryable
				from ingestion_archives
				where run_id = :runId
				  and status = 'FAILED'
				order by failed_at desc, idempotency_key asc
				limit 1
				""")
				.param("runId", runId)
				.query((resultSet, rowNumber) -> IngestionJdbcMappers.mapRecordedFailure(
						resultSet,
						"failed_at"))
				.optional();
	}

	private void updateWithFailure(
			long runId,
			IngestionRunStatus status,
			RecordedIngestionFailure recordedFailure,
			Instant now
	) {
		var failure = recordedFailure.failure();
		jdbcClient.sql("""
				update ingestion_runs
				set status = :status,
				    completed_at = case when :status = 'STAGED' then cast(:completedAt as timestamptz) else null end,
				    last_failed_at = :lastFailedAt,
				    last_error_code = :lastErrorCode,
				    last_error_retryable = :lastErrorRetryable,
				    updated_at = :updatedAt
				where id = :runId
				""")
				.param("status", status.name())
				.param("completedAt", Timestamp.from(now))
				.param("lastFailedAt", Timestamp.from(recordedFailure.occurredAt()))
				.param("lastErrorCode", failure.code().code())
				.param("lastErrorRetryable", failure.retryable())
				.param("updatedAt", Timestamp.from(now))
				.param("runId", runId)
				.update();
	}

	private void updateWithoutFailure(long runId, IngestionRunStatus status, Instant now) {
		jdbcClient.sql("""
				update ingestion_runs
				set status = :status,
				    completed_at = case when :status = 'STAGED' then cast(:completedAt as timestamptz) else null end,
				    last_failed_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    updated_at = :updatedAt
				where id = :runId
				""")
				.param("status", status.name())
				.param("completedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("runId", runId)
				.update();
	}

	private static RunCounts mapCounts(ResultSet resultSet, int rowNumber) throws SQLException {
		return new RunCounts(
				resultSet.getInt("staged_count"),
				resultSet.getInt("processing_count"),
				resultSet.getInt("failed_count"),
				resultSet.getInt("attempted_count")
		);
	}
}
