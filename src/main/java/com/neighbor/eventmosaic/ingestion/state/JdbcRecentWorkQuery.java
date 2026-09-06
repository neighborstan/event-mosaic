package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Выбирает только готовую к продолжению работу текущего суточного плана и проверяет принадлежность архивов этому плану. */
@Repository
class JdbcRecentWorkQuery {

	private static final String ACTIVE_TARGET = """
			((archive.source_update_time >= plan.window_from
			  and archive.source_update_time < plan.window_to)
			 or (plan.source_frontier >= plan.window_to
			     and archive.source_update_time >= plan.window_to
			     and archive.source_update_time <= plan.source_frontier)
			 or archive.source_update_time = plan.source_frontier)
			""";

	private final JdbcClient jdbcClient;
	private final Duration maximumDelay;

	JdbcRecentWorkQuery(JdbcClient jdbcClient, BackendDataProperties properties) {
		this.jdbcClient = jdbcClient;
		this.maximumDelay = properties.retry().maximumDelay();
	}

	boolean belongsToActivePlan(String archiveKey) {
		return jdbcClient.sql("""
				select exists (
				    select 1
				    from ingestion_archives archive
				    join ingestion_runs run on run.id = archive.run_id
				    join ingestion_recent_recovery_plan plan on plan.source_name = run.source_name
				    where archive.idempotency_key = :archiveKey and
				""" + ACTIVE_TARGET + ")")
				.param("archiveKey", archiveKey)
				.query(Boolean.class).single();
	}

	List<IngestionArchiveState> findEligible(int limit, Set<String> excludedKeys, Instant now) {
		String exclusions = excludedKeys.isEmpty() ? "" : "and archive.idempotency_key not in (:excludedKeys)\n";
		JdbcClient.StatementSpec statement = jdbcClient.sql("""
				select archive.*
				from ingestion_archives archive
				join ingestion_runs run on run.id = archive.run_id
				join ingestion_recent_recovery_plan plan on plan.source_name = run.source_name
				left join ingestion_archive_processing processing
				  on processing.archive_idempotency_key = archive.idempotency_key
				where
				""" + ACTIVE_TARGET + """
				and (
				    archive.status = 'DISCOVERED'
				    or (archive.status = 'FAILED' and archive.last_error_retryable
				        and archive.retry_not_before <= :now)
				    or (archive.status = 'PROCESSING' and archive.lease_expires_at <= :now
				        and (archive.automatic_retries_used < archive.automatic_retry_limit
				             or archive.lease_expires_at <= :exhaustedLeaseBoundary))
				    or (archive.status = 'STAGED' and (
				        processing.archive_idempotency_key is null
				        or processing.status = 'PENDING'
				        or (processing.status = 'FAILED' and processing.last_error_retryable
				            and processing.retry_not_before <= :now)
				        or (processing.status = 'PROCESSING' and processing.lease_expires_at <= :now
				            and (processing.automatic_retries_used < processing.automatic_retry_limit
				                 or processing.lease_expires_at <= :exhaustedLeaseBoundary))
				    ))
				)
				""" + exclusions + """
				order by archive.archive_type, archive.source_update_time desc, archive.idempotency_key
				limit :limit
				""")
				.param("now", Timestamp.from(now))
				.param("exhaustedLeaseBoundary", Timestamp.from(now.minus(maximumDelay)))
				.param("limit", limit);
		if (!excludedKeys.isEmpty()) {
			statement = statement.param("excludedKeys", excludedKeys);
		}
		return statement.query(IngestionJdbcMappers.ARCHIVE).list();
	}
}
