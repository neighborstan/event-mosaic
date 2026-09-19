package com.neighbor.eventmosaic.ingestion.observability;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Читает состояние текущего восстановления и хранения, не раскрывая внешние идентификаторы. */
@Repository
class JdbcBackendDataOperationalRepository {

	private static final String SNAPSHOT_SQL = """
			with current_plan as (
			    select * from ingestion_recent_recovery_plan where source_name = :sourceName
			), target_slots as (
			    select generate_series(window_from, window_to - interval '15 minutes', interval '15 minutes') as slot
			    from current_plan
			    union
			    select generate_series(window_to, source_frontier, interval '15 minutes') as slot
			    from current_plan where source_frontier >= window_to
			    union
			    select source_frontier from current_plan
			), recent_archives as (
			    select archive.* from ingestion_archives archive
			    join ingestion_runs run on run.id = archive.run_id and run.source_name = :sourceName
			    join target_slots slots on slots.slot = archive.source_update_time
			), recent_processing as (
			    select processing.* from ingestion_archive_processing processing
			    join recent_archives archive on archive.idempotency_key = processing.archive_idempotency_key
			), retry_candidates as (
			    select 'source_poll' as owner, status = 'FAILED' and last_error_retryable as failed,
			        automatic_retries_used >= automatic_retry_limit as exhausted, retry_not_before, lease_expires_at
			    from ingestion_source_poll_state where source_name = :sourceName
			    union all
			    select 'acquisition', status = 'FAILED' and last_error_retryable,
			        automatic_retries_used >= automatic_retry_limit, retry_not_before, lease_expires_at
			    from recent_archives
			    union all
			    select 'processing', status = 'FAILED' and last_error_retryable,
			        automatic_retries_used >= automatic_retry_limit, retry_not_before, lease_expires_at
			    from recent_processing
			    union all
			    select 'receipt_audit', status = 'FAILED' and last_error_retryable,
			        automatic_retries_used >= automatic_retry_limit,
			        case when status = 'FAILED' then greatest(due_at, retry_not_before) end, lease_expires_at
			    from ingestion_receipt_audit_state where source_name = :sourceName
			), retry_states as (
			    select owner, exhausted,
			        case when failed then retry_not_before else lease_expires_at
			            + (case when exhausted then :maximumRetryDelaySeconds else 0 end)
			                * interval '1 second' end as retry_at
			    from retry_candidates where failed or lease_expires_at <= :now
			)
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
			    (select count(*) from target_slots slots where
			        (select count(*) from recent_archives archive where archive.source_update_time = slots.slot) <> 2
			    ) as open_gaps,
			    coalesce((select case when last_succeeded_at is null then -1
			        else greatest(0, extract(epoch from (:now - last_succeeded_at)))::bigint end
			        from ingestion_source_poll_state where source_name = :sourceName), -1) as successful_poll_age,
			    coalesce((select greatest(0, extract(epoch from (:now - coalesce(last_succeeded_at, created_at))))::bigint
			        from ingestion_source_poll_state where source_name = :sourceName), 0) as source_outage_age,
			    coalesce((select greatest(0, extract(epoch from (
			        date_bin(interval '15 minutes', cast(:now as timestamptz), timestamptz '2000-01-01')
			        - interval '15 minutes' - latest_observed_update_time)))::bigint
			        from ingestion_source_state where source_name = :sourceName), -1) as source_lag,
			    coalesce((select greatest(0, ceil(extract(epoch from (retry_at - :now))))::bigint
			        from retry_states where owner = 'source_poll'), 0) as source_retry_delay,
			    coalesce((select exhausted from retry_states where owner = 'source_poll'), false) as source_cooldown,
			    coalesce((select catalog_status = 'PENDING' from current_plan), false) as catalog_pending,
			    (select count(*) from target_slots slots where not exists (
			        select 1 from recent_archives archive join recent_processing processing
			        on processing.archive_idempotency_key = archive.idempotency_key
			        where archive.source_update_time = slots.slot and archive.archive_type = 'TRANSLATION_EVENTS'
			          and archive.status = 'STAGED' and processing.status = 'INDEXED'
			    )) as event_bootstrap_remaining,
			    (select count(*) from target_slots slots where not exists (
			        select 1 from recent_archives archive join recent_processing processing
			        on processing.archive_idempotency_key = archive.idempotency_key
			        where archive.source_update_time = slots.slot and archive.archive_type = 'TRANSLATION_MENTIONS'
			          and archive.status = 'STAGED' and processing.status = 'INDEXED'
			    )) as mention_bootstrap_remaining,
			    (select count(*) from retry_states where owner = 'source_poll' and retry_at <= :now) as source_poll_retry_due,
			    (select count(*) from retry_states where owner = 'source_poll' and retry_at > :now) as source_poll_retry_deferred,
			    (select count(*) from retry_states where owner = 'source_poll' and exhausted) as source_poll_retry_exhausted,
			    (select count(*) from retry_states where owner = 'acquisition' and retry_at <= :now) as acquisition_retry_due,
			    (select count(*) from retry_states where owner = 'acquisition' and retry_at > :now) as acquisition_retry_deferred,
			    (select count(*) from retry_states where owner = 'acquisition' and exhausted) as acquisition_retry_exhausted,
			    (select count(*) from retry_states where owner = 'processing' and retry_at <= :now) as processing_retry_due,
			    (select count(*) from retry_states where owner = 'processing' and retry_at > :now) as processing_retry_deferred,
			    (select count(*) from retry_states where owner = 'processing' and exhausted) as processing_retry_exhausted,
			    (select count(*) from retry_states where owner = 'receipt_audit' and retry_at <= :now) as receipt_audit_retry_due,
			    (select count(*) from retry_states where owner = 'receipt_audit' and retry_at > :now) as receipt_audit_retry_deferred,
			    (select count(*) from retry_states where owner = 'receipt_audit' and exhausted) as receipt_audit_retry_exhausted,
			    ((select count(*) from ingestion_source_poll_state
			        where source_name = :sourceName and status = 'FAILED' and last_error_retryable = false)
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
	private final long maximumRetryDelaySeconds;

	JdbcBackendDataOperationalRepository(JdbcClient jdbcClient, Clock clock, BackendDataProperties properties) {
		this.jdbcClient = jdbcClient;
		this.clock = clock;
		this.maximumRetryDelaySeconds = properties.retry().maximumDelay().toSeconds();
	}

	BackendDataDatabaseSnapshot read() {
		Instant now = clock.instant();
		SnapshotCounts counts = jdbcClient.sql(SNAPSHOT_SQL)
				.param("sourceName", GdeltSourceContract.SOURCE_NAME)
				.param("now", Timestamp.from(now))
				.param("updateIntervalSeconds", GdeltSourceContract.UPDATE_INTERVAL.toSeconds())
				.param("maximumRetryDelaySeconds", maximumRetryDelaySeconds)
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
						resultSet.getLong("alias_transition_operations"),
						new BackendDataOperationalSnapshot.LiveCounts(
								resultSet.getLong("successful_poll_age"),
								resultSet.getLong("source_outage_age"),
								resultSet.getLong("source_lag"),
								resultSet.getLong("source_retry_delay"),
								resultSet.getBoolean("source_cooldown"),
								resultSet.getBoolean("catalog_pending"),
								resultSet.getLong("event_bootstrap_remaining"),
								resultSet.getLong("mention_bootstrap_remaining"),
								new BackendDataOperationalSnapshot.RetryCounts(
										resultSet.getLong("receipt_audit_retry_due"),
										resultSet.getLong("receipt_audit_retry_deferred"),
										resultSet.getLong("receipt_audit_retry_exhausted")))))
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
					eventIndices.add(resultSet.getString("event_index_name"));
					mentionIndices.add(resultSet.getString("mention_index_name"));
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
			long aliasTransitionOperations,
			BackendDataOperationalSnapshot.LiveCounts live
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
					mentionIndices,
					live);
		}
	}
}
