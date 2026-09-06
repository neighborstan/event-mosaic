package com.neighbor.eventmosaic.ingestion.audit;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.retry.RetryDelayPolicy;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Хранит срок и владельца фоновой проверки отдельно от успешного состояния обработки архивов. */
@Repository
public class JdbcReceiptAuditRepository {

	private static final String SOURCE = GdeltSourceContract.SOURCE_NAME;
	private static final String CLEAR_BINDING = """
			attempt_token = null, lease_expires_at = null,
			archive_idempotency_key = null, processing_fingerprint = null,
			processing_attempt_count = null, processing_state_version = null,
			logical_partition_key = null, partition_state_version = null,
			generation_id = null, generation_uuid = null, index_kind = null, index_uuid = null
			""";
	private final JdbcClient jdbc;
	private final ArchiveProcessingLedger processingLedger;
	private final Clock clock;
	private final Duration interval;
	private final Duration lease;
	private final BackendDataProperties.Retry retry;
	private final RetryDelayPolicy retryDelay;

	/** Использует общий источник времени и существующие правила повтора после сбоя. */
	public JdbcReceiptAuditRepository(
			JdbcClient jdbc,
			ArchiveProcessingLedger processingLedger,
			Clock clock,
			GdeltIngestionProperties properties,
			BackendDataProperties backendProperties,
			RetryDelayPolicy retryDelay
	) {
		this.jdbc = jdbc;
		this.processingLedger = processingLedger;
		this.clock = clock;
		this.interval = properties.automatic().receiptAudit().interval();
		this.lease = properties.continuity().recoveryLease();
		this.retry = backendProperties.retry();
		this.retryDelay = retryDelay;
	}

	/** Получает один самый старый результат; продолжение пакета обязано предъявить версию предыдущего завершения. */
	@Transactional
	public Optional<ReceiptAuditClaim> claim(Long continuationVersion) {
		Instant now = clock.instant();
		jdbc.sql("""
				insert into ingestion_receipt_audit_state(source_name, status, due_at, automatic_retry_limit)
				select source_name, 'IDLE', :now, :retryLimit from ingestion_recent_recovery_plan
				where source_name = :source
				on conflict (source_name) do nothing
				""")
				.param("source", SOURCE).param("now", Timestamp.from(now))
				.param("retryLimit", retry.automaticRetryLimit()).update();
		AuditState state = lockState().orElse(null);
		if (state == null || !eligible(state, continuationVersion, now)) {
			return Optional.empty();
		}
		Optional<String> archiveKey = jdbc.sql("""
				select p.archive_idempotency_key
				from ingestion_archive_processing p
				join ingestion_archives a on a.idempotency_key = p.archive_idempotency_key
				join ingestion_runs r on r.id = a.run_id
				join ingestion_recent_recovery_plan plan on plan.source_name = r.source_name
				where plan.source_name = :source and p.status = 'INDEXED'
				  and (a.source_update_time = plan.source_frontier
				       or (a.source_update_time >= plan.window_from
				           and a.source_update_time < plan.window_to)
				       or (a.source_update_time >= plan.window_to
				           and a.source_update_time <= plan.source_frontier))
				  and p.receipt_verified_at <= :oldestAllowed
				  and not exists (
				      select 1 from index_logical_partitions partition_state
				      where partition_state.partition_key = p.logical_partition_key
				        and partition_state.repair_cause is not null)
				  and not exists (
				      select 1 from index_maintenance_operations m
				      where m.partition_key = p.logical_partition_key
				        and m.phase not in ('COMPLETED', 'FAILED'))
				order by p.receipt_verified_at, a.source_update_time, a.archive_type, a.idempotency_key
				limit 1
				""")
				.param("source", SOURCE).param("oldestAllowed", Timestamp.from(now.minus(interval)))
				.query(String.class).optional();
		if (archiveKey.isEmpty()) {
			idle(state, null, now);
			return Optional.empty();
		}
		var processing = processingLedger.findByArchiveIdempotencyKey(archiveKey.orElseThrow()).orElseThrow();
		var target = lockCurrentTarget(processing.targetBinding().partitionKey(),
				processing.targetBinding().indexKind()).orElse(null);
		if (target == null) {
			idle(state, ReceiptAuditOutcome.STALE_BINDING, now);
			return Optional.empty();
		}
		lockProcessing(archiveKey.orElseThrow());
		processing = processingLedger.findByArchiveIdempotencyKey(archiveKey.orElseThrow()).orElseThrow();
		if (processing.status() != ArchiveProcessingStatus.INDEXED) {
			idle(state, ReceiptAuditOutcome.STALE_BINDING, now);
			return Optional.empty();
		}
		UUID token = UUID.randomUUID();
		boolean retryAttempt = !state.status().equals("IDLE");
		boolean restart = retryAttempt && state.retriesUsed() >= state.retryLimit();
		int used = !retryAttempt || restart ? 0 : state.retriesUsed() + 1;
		jdbc.sql("""
				update ingestion_receipt_audit_state set status = 'AUDITING',
				    attempt_token = :token, lease_expires_at = :leaseExpires,
				    archive_idempotency_key = :archiveKey, processing_fingerprint = :fingerprint,
				    processing_attempt_count = :attemptCount, processing_state_version = :processingVersion,
				    logical_partition_key = :partition, partition_state_version = :partitionVersion,
				    generation_id = :generationId, generation_uuid = :generationUuid,
				    index_kind = :kind, index_uuid = :indexUuid,
				    total_attempt_count = total_attempt_count + 1, automatic_retries_used = :used,
				    consecutive_retryable_failures = :failures,
				    retry_sequence = retry_sequence + :sequenceIncrement,
				    last_exhausted_at = case when :expiredExhausted then :expiredAt else last_exhausted_at end,
				    last_exhausted_error_code = case when :expiredExhausted then 'ATTEMPT_LEASE_EXPIRED' else last_exhausted_error_code end,
				    last_attempt_at = :now, due_at = :due,
				    failed_at = null, last_error_code = null, last_error_retryable = null, retry_not_before = null,
				    state_version = state_version + 1, updated_at = :now
				where source_name = :source
				""")
				.param("source", SOURCE).param("token", token).param("leaseExpires", Timestamp.from(now.plus(lease)))
				.param("archiveKey", archiveKey.orElseThrow()).param("fingerprint", processing.fingerprint().processingFingerprint())
				.param("attemptCount", processing.attempt().count()).param("processingVersion", processing.stateVersion())
				.param("partition", target.partitionKey()).param("partitionVersion", target.partitionStateVersion())
				.param("generationId", target.generationId()).param("generationUuid", target.generationUuid())
				.param("kind", target.indexKind().name()).param("indexUuid", target.indexUuid())
				.param("used", used).param("failures", restart || !retryAttempt ? 0 : state.failures())
				.param("sequenceIncrement", restart ? 1 : 0).param("now", Timestamp.from(now))
				.param("expiredExhausted", restart && state.status().equals("AUDITING"))
				.param("expiredAt", Timestamp.from(state.leaseExpires() == null ? now : state.leaseExpires()))
				.param("due", Timestamp.from(continuationVersion == null ? now.plus(interval) : state.due()))
				.update();
		return Optional.of(new ReceiptAuditClaim(token, processing, target));
	}

	/** Применяет результат только к прежнему владельцу и неизменившемуся физическому индексу. */
	@Transactional
	public Optional<Long> complete(ReceiptAuditClaim claim, ArchiveReceiptVerification verification) {
		AuditState state = lockOwned(claim).orElse(null);
		if (state == null) {
			return Optional.empty();
		}
		var processing = claim.processing();
		boolean surplus = verification.status() == ArchiveReceiptStatus.SURPLUS;
		AttemptTransitionResult applied = verification.matched()
				? processingLedger.recordReceiptMatch(processing.archiveIdempotencyKey(),
						processing.fingerprint().processingFingerprint(), processing.attempt().count(), processing.stateVersion(),
						processing.targetBinding(), claim.currentTarget(), verification)
				: processingLedger.recordReceiptMismatch(processing.archiveIdempotencyKey(),
						processing.fingerprint().processingFingerprint(), processing.attempt().count(), processing.stateVersion(),
						processing.targetBinding(), claim.currentTarget(), verification,
						new ArchiveProcessingFailure(surplus ? "INDEX_RECEIPT_SURPLUS" : "INDEX_RECEIPT_MISMATCH", !surplus));
		if (applied != AttemptTransitionResult.APPLIED) {
			idle(state, ReceiptAuditOutcome.STALE_BINDING, clock.instant());
			return Optional.empty();
		}
		if (surplus) {
			requestRepair(claim, "SURPLUS");
		}
		return Optional.of(idle(state, ReceiptAuditOutcome.valueOf(verification.status().name()), clock.instant()));
	}

	/** Сохраняет доказанную потерю или подмену индекса как причину его восстановления. */
	@Transactional
	public Optional<Long> missing(ReceiptAuditClaim claim, IndexTargetUnavailableReason reason) {
		AuditState state = lockOwned(claim).orElse(null);
		if (state == null) {
			return Optional.empty();
		}
		if (!bindingUnchanged(claim)) {
			idle(state, ReceiptAuditOutcome.STALE_BINDING, clock.instant());
			return Optional.empty();
		}
		requestRepair(claim, reason == IndexTargetUnavailableReason.MISSING ? "MISSING_CURRENT" : "CORRUPT_CURRENT");
		return Optional.of(idle(state, ReceiptAuditOutcome.MISSING_GENERATION, clock.instant()));
	}

	/** Откладывает недоступную проверку и сохраняет историю исчерпания повторов, не меняя обработку архива. */
	@Transactional
	public void fail(ReceiptAuditClaim claim, String errorCode) {
		AuditState state = lockOwned(claim).orElse(null);
		if (state == null) {
			return;
		}
		if (!bindingUnchanged(claim)) {
			idle(state, ReceiptAuditOutcome.STALE_BINDING, clock.instant());
			return;
		}
		Instant now = clock.instant();
		boolean exhausted = state.retriesUsed() >= state.retryLimit();
		Instant next = exhausted ? now.plus(retry.maximumDelay()) : retryDelay.retryNotBefore(now, state.failures(), Duration.ZERO);
		jdbc.sql("""
				update ingestion_receipt_audit_state set status = 'FAILED',
				    failed_at = :now, last_error_code = :code, last_error_retryable = true,
				    retry_not_before = :retryAt, consecutive_retryable_failures = consecutive_retryable_failures + 1,
				    last_exhausted_at = case when :exhausted then :now else last_exhausted_at end,
				    last_exhausted_error_code = case when :exhausted then :code else last_exhausted_error_code end,
				    last_outcome = 'INFRASTRUCTURE_FAILURE', state_version = state_version + 1, updated_at = :now,
				""" + CLEAR_BINDING + " where source_name = :source")
				.param("source", SOURCE).param("now", Timestamp.from(now)).param("code", errorCode)
				.param("retryAt", Timestamp.from(next)).param("exhausted", exhausted).update();
	}

	private boolean eligible(AuditState state, Long continuationVersion, Instant now) {
		if (continuationVersion != null) {
			return state.status().equals("IDLE") && state.version() == continuationVersion;
		}
		return switch (state.status()) {
			case "IDLE" -> !state.due().isAfter(now);
			case "FAILED" -> state.retryAt() != null && !state.due().isAfter(now) && !state.retryAt().isAfter(now);
			case "AUDITING" -> !(state.retriesUsed() >= state.retryLimit()
					? state.leaseExpires().plus(retry.maximumDelay()) : state.leaseExpires()).isAfter(now);
			default -> false;
		};
	}

	private Optional<AuditState> lockOwned(ReceiptAuditClaim claim) {
		var state = lockState().filter(current -> current.status().equals("AUDITING")
				&& claim.token().equals(current.token()) && current.leaseExpires().isAfter(clock.instant()));
		if (state.isEmpty()) {
			return Optional.empty();
		}
		lockPartition(claim.currentTarget().partitionKey());
		if (!state.orElseThrow().leaseExpires().isAfter(clock.instant())) {
			return Optional.empty();
		}
		var processing = claim.processing();
		var target = claim.currentTarget();
		boolean bindingMatches = jdbc.sql("""
				select count(*) = 1 from ingestion_receipt_audit_state
				where source_name = :source and attempt_token = :token
				  and archive_idempotency_key = :archiveKey and processing_fingerprint = :fingerprint
				  and processing_attempt_count = :attemptCount and processing_state_version = :processingVersion
				  and logical_partition_key = :partition and partition_state_version = :partitionVersion
				  and generation_id = :generationId and generation_uuid = :generationUuid
				  and index_kind = :kind and index_uuid = :indexUuid
				""").param("source", SOURCE).param("token", claim.token())
				.param("archiveKey", processing.archiveIdempotencyKey()).param("fingerprint", processing.fingerprint().processingFingerprint())
				.param("attemptCount", processing.attempt().count()).param("processingVersion", processing.stateVersion())
				.param("partition", target.partitionKey()).param("partitionVersion", target.partitionStateVersion())
				.param("generationId", target.generationId()).param("generationUuid", target.generationUuid())
				.param("kind", target.indexKind().name()).param("indexUuid", target.indexUuid())
				.query(Boolean.class).single();
		return bindingMatches ? state : Optional.empty();
	}

	private boolean bindingUnchanged(ReceiptAuditClaim claim) {
		String key = claim.processing().archiveIdempotencyKey();
		lockProcessing(key);
		var processing = processingLedger.findByArchiveIdempotencyKey(key).orElseThrow();
		var target = lockCurrentTarget(claim.currentTarget().partitionKey(), claim.currentTarget().indexKind());
		return processing.equals(claim.processing()) && target.filter(claim.currentTarget()::equals).isPresent();
	}

	private Optional<ArchiveProcessingTargetBinding> lockCurrentTarget(String partition, GdeltIndexKind kind) {
		lockPartition(partition);
		return jdbc.sql("""
				select p.state_version, g.id, g.generation_uuid,
				       g.event_index_name, g.event_index_uuid, g.mention_index_name, g.mention_index_uuid
				from index_logical_partitions p join index_generations g on g.partition_key = p.partition_key
				where p.partition_key = :partition and g.state = 'ACTIVE'
				  and not exists (select 1 from index_maintenance_operations m
				      where m.partition_key = p.partition_key and m.phase not in ('COMPLETED', 'FAILED'))
				""")
				.param("partition", partition).query((row, _) -> new ArchiveProcessingTargetBinding(kind,
						partition, row.getLong("state_version"), row.getLong("id"),
						row.getObject("generation_uuid", UUID.class),
						row.getString(kind == GdeltIndexKind.EVENT ? "event_index_name" : "mention_index_name"),
						row.getString(kind == GdeltIndexKind.EVENT ? "event_index_uuid" : "mention_index_uuid"))).optional();
	}

	private void lockPartition(String partition) {
		// Смена поколения блокирует сначала раздел, затем архивы. Проверка соблюдает тот же порядок.
		jdbc.sql("select partition_key from index_logical_partitions where partition_key = :partition for update")
				.param("partition", partition).query(String.class).optional();
	}

	private void lockProcessing(String key) {
		jdbc.sql("select archive_idempotency_key from ingestion_archive_processing where archive_idempotency_key = :key for update")
				.param("key", key).query(String.class).single();
	}

	private void requestRepair(ReceiptAuditClaim claim, String cause) {
		jdbc.sql("""
				update index_logical_partitions set repair_cause = :cause, repair_requested_at = :now,
				    updated_at = :now
				where partition_key = :partition and state_version = :version
				""")
				.param("cause", cause).param("now", Timestamp.from(clock.instant()))
				.param("partition", claim.currentTarget().partitionKey()).param("version", claim.currentTarget().partitionStateVersion()).update();
	}

	private long idle(AuditState state, ReceiptAuditOutcome outcome, Instant now) {
		jdbc.sql("""
				update ingestion_receipt_audit_state set status = 'IDLE',
				    due_at = :due, last_outcome = coalesce(:outcome, last_outcome),
				    last_audited_at = case when :outcome is not null then :now else last_audited_at end,
				    failed_at = null, last_error_code = null, last_error_retryable = null, retry_not_before = null,
				    automatic_retries_used = 0, consecutive_retryable_failures = 0,
				    state_version = state_version + 1, updated_at = :now,
				""" + CLEAR_BINDING + " where source_name = :source")
				.param("source", SOURCE).param("due", Timestamp.from(state.due().isAfter(now) ? state.due() : now.plus(interval)))
				.param("outcome", outcome == null ? null : outcome.name(), java.sql.Types.VARCHAR)
				.param("now", Timestamp.from(now)).update();
		return state.version() + 1;
	}

	private Optional<AuditState> lockState() {
		return jdbc.sql("select * from ingestion_receipt_audit_state where source_name = :source for update")
				.param("source", SOURCE).query((row, _) -> new AuditState(row.getString("status"),
						row.getTimestamp("due_at").toInstant(), row.getObject("attempt_token", UUID.class),
						instant(row.getTimestamp("lease_expires_at")), instant(row.getTimestamp("retry_not_before")),
						row.getInt("automatic_retries_used"), row.getInt("automatic_retry_limit"),
						row.getInt("consecutive_retryable_failures"), row.getLong("state_version"))).optional();
	}

	private static Instant instant(Timestamp timestamp) {
		return timestamp == null ? null : timestamp.toInstant();
	}

	/** Нужная для атомарного перехода часть единственной строки проверки источника. */
	private record AuditState(String status, Instant due, UUID token, Instant leaseExpires, Instant retryAt,
			int retriesUsed, int retryLimit, int failures, long version) {
	}
}
