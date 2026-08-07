package com.neighbor.eventmosaic.ingestion.observability;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Выполняет bounded aggregate reads operational state из clean V001 schema. */
@Repository
class JdbcBackendDataOperationalRepository {

	private static final String SNAPSHOT_SQL = """
			select
			    coalesce((
			        select greatest(
			            0,
			            extract(epoch from (
			                source.latest_observed_update_time
			                - coalesce(
			                    indexed.latest_indexed_at,
				                    source.continuity_baseline
				                        - (:updateIntervalSeconds * interval '1 second')
			                )
			            ))::bigint
			        )
			        from ingestion_source_state source
			        left join lateral (
			            select max(complete_run.source_update_time) as latest_indexed_at
			            from (
			                select run.id, run.source_update_time
			                from ingestion_runs run
			                join ingestion_archives archive on archive.run_id = run.id
			                left join ingestion_archive_processing processing
			                    on processing.archive_idempotency_key = archive.idempotency_key
			                where run.source_name = :sourceName
			                group by run.id, run.source_update_time
			                having count(*) = 2
			                   and bool_and(
			                       archive.status = 'STAGED'
			                       and coalesce(processing.status = 'INDEXED', false)
			                   )
			            ) complete_run
			        ) indexed on true
			        where source.source_name = :sourceName
			    ), 0) as lag_seconds,
			    (select count(*) from ingestion_gaps) as open_gaps,
			    (select count(*) from ingestion_source_poll_state
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before <= :now) as source_poll_retry_due,
			    (select count(*) from ingestion_source_poll_state
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before > :now) as source_poll_retry_deferred,
			    (select count(*) from ingestion_source_poll_state
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used >= automatic_retry_limit) as source_poll_retry_exhausted,
			    (select count(*) from ingestion_archives
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before <= :now) as acquisition_retry_due,
			    (select count(*) from ingestion_archives
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before > :now) as acquisition_retry_deferred,
			    (select count(*) from ingestion_archives
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used >= automatic_retry_limit) as acquisition_retry_exhausted,
			    (select count(*) from ingestion_archive_processing
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before <= :now) as processing_retry_due,
			    (select count(*) from ingestion_archive_processing
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used < automatic_retry_limit
			          and retry_not_before > :now) as processing_retry_deferred,
			    (select count(*) from ingestion_archive_processing
			        where status = 'FAILED'
			          and last_error_retryable = true
			          and automatic_retries_used >= automatic_retry_limit) as processing_retry_exhausted,
			    ((select count(*) from ingestion_source_poll_state
			        where status = 'FAILED' and last_error_retryable = false)
			     + (select count(*) from ingestion_archives
			        where status = 'FAILED' and last_error_retryable = false)
			     + (select count(*) from ingestion_archive_processing
			        where status = 'FAILED' and last_error_retryable = false)) as permanent_failures,
			    (select count(*) from ingestion_archive_processing
			        where last_error_code = 'INDEX_RECEIPT_MISMATCH') as receipt_mismatches,
			    (select count(*) from ingestion_archive_processing
			        where last_error_code = 'INDEX_RECEIPT_SURPLUS') as receipt_surpluses,
			    (select count(*) from index_generations where state = 'BUILDING') as building_generations,
			    (select count(*) from index_generations where state = 'ACTIVE') as active_generations,
			    (select count(*) from index_generations where state = 'SUPERSEDED') as superseded_generations,
			    (select count(*) from index_logical_partitions
			        where repair_cause is not null) as repair_required_partitions,
			    (select count(*) from index_maintenance_operations
			        where phase not in ('COMPLETED', 'FAILED')) as open_maintenance_operations,
			    (select count(*) from index_maintenance_operations
			        where operation_kind in ('INITIAL_PROMOTION', 'REBUILD')
			          and phase in ('CUTOVER_REQUESTED', 'CUTOVER_OBSERVED'))
			        as alias_transition_operations
			""";

	private final JdbcClient jdbcClient;
	private final Clock clock;

	JdbcBackendDataOperationalRepository(JdbcClient jdbcClient, Clock clock) {
		this.jdbcClient = jdbcClient;
		this.clock = clock;
	}

	BackendDataDatabaseSnapshot read() {
		Instant now = clock.instant();
		SnapshotCounts counts = jdbcClient.sql(SNAPSHOT_SQL)
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("now", Timestamp.from(now))
				.param("updateIntervalSeconds", GdeltSourceContract.UPDATE_INTERVAL.toSeconds())
				.query((resultSet, _) -> new SnapshotCounts(
						resultSet.getLong("lag_seconds"),
						resultSet.getLong("open_gaps"),
						resultSet.getLong("source_poll_retry_due"),
						resultSet.getLong("source_poll_retry_deferred"),
						resultSet.getLong("source_poll_retry_exhausted"),
						resultSet.getLong("acquisition_retry_due"),
						resultSet.getLong("acquisition_retry_deferred"),
						resultSet.getLong("acquisition_retry_exhausted"),
						resultSet.getLong("processing_retry_due"),
						resultSet.getLong("processing_retry_deferred"),
						resultSet.getLong("processing_retry_exhausted"),
						resultSet.getLong("permanent_failures"),
						resultSet.getLong("receipt_mismatches"),
						resultSet.getLong("receipt_surpluses"),
						resultSet.getLong("building_generations"),
						resultSet.getLong("active_generations"),
						resultSet.getLong("superseded_generations"),
						resultSet.getLong("repair_required_partitions"),
						resultSet.getLong("open_maintenance_operations"),
						resultSet.getLong("alias_transition_operations")))
				.single();
		Set<String> eventIndices = new LinkedHashSet<>();
		Set<String> mentionIndices = new LinkedHashSet<>();
		jdbcClient.sql("""
				select event_index_name, mention_index_name
				from index_generations
				where state = 'ACTIVE'
				order by partition_key
				""")
				.query(resultSet -> {
					while (resultSet.next()) {
						eventIndices.add(resultSet.getString("event_index_name"));
						mentionIndices.add(resultSet.getString("mention_index_name"));
					}
				});
		return counts.toSnapshot(eventIndices, mentionIndices);
	}

	private record SnapshotCounts(
			long lagSeconds,
			long openGaps,
			long sourcePollRetryDue,
			long sourcePollRetryDeferred,
			long sourcePollRetryExhausted,
			long acquisitionRetryDue,
			long acquisitionRetryDeferred,
			long acquisitionRetryExhausted,
			long processingRetryDue,
			long processingRetryDeferred,
			long processingRetryExhausted,
			long permanentFailures,
			long receiptMismatches,
			long receiptSurpluses,
			long buildingGenerations,
			long activeGenerations,
			long supersededGenerations,
			long repairRequiredPartitions,
			long openMaintenanceOperations,
			long aliasTransitionOperations
	) {

		private BackendDataDatabaseSnapshot toSnapshot(
				Set<String> eventIndices,
				Set<String> mentionIndices
		) {
			return new BackendDataDatabaseSnapshot(
					lagSeconds,
					openGaps,
					new BackendDataOperationalSnapshot.RetryCounts(
							sourcePollRetryDue,
							sourcePollRetryDeferred,
							sourcePollRetryExhausted),
					new BackendDataOperationalSnapshot.RetryCounts(
							acquisitionRetryDue,
							acquisitionRetryDeferred,
							acquisitionRetryExhausted),
					new BackendDataOperationalSnapshot.RetryCounts(
							processingRetryDue,
							processingRetryDeferred,
							processingRetryExhausted),
					permanentFailures,
					new BackendDataOperationalSnapshot.ReceiptCounts(
							receiptMismatches,
							receiptSurpluses),
					new BackendDataOperationalSnapshot.GenerationCounts(
							buildingGenerations,
							activeGenerations,
							supersededGenerations),
					repairRequiredPartitions,
					openMaintenanceOperations,
					aliasTransitionOperations,
					eventIndices,
					mentionIndices);
		}
	}
}
