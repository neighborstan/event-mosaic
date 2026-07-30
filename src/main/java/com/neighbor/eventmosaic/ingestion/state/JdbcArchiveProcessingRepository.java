package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Выполняет SQL registration, ownership и transitions processing rows.
 */
@Repository
class JdbcArchiveProcessingRepository {

	private static final String PARAM_ARCHIVE_KEY = "archiveIdempotencyKey";
	private static final String PARAM_ATTEMPT_TOKEN = "attemptToken";
	private static final String PARAM_PROCESSING_STATUS = "processingStatus";
	private static final String PARAM_STATUS = "status";
	private static final String PARAM_UPDATED_AT = "updatedAt";
	private static final String PARAM_DELIVERED = "deliveredRecords";
	private static final String PARAM_SOURCE_INVALID = "sourceInvalidRecords";
	private static final String PARAM_MAPPING_REJECTED = "mappingRejectedRecords";
	private static final String PARAM_SUBMITTED = "submittedOperations";
	private static final String PARAM_SUCCEEDED = "succeededOperations";
	private static final String PARAM_FAILED = "failedOperations";
	private static final String PARAM_RECEIPT = "receiptDocuments";
	private static final String PARAM_FIRST_FAILED_LINE = "firstFailedLine";
	private static final String SELECT_PROCESSING_STATE_BY_ARCHIVE_KEY = """
			select
			    archive_idempotency_key,
			    source_fingerprint,
			    projection_revision,
			    processing_fingerprint,
			    status,
			    attempt_token,
			    lease_expires_at,
			    attempt_count,
			    last_attempt_at,
			    delivered_records,
			    source_invalid_records,
			    mapping_rejected_records,
			    submitted_operations,
			    succeeded_operations,
			    failed_operations,
			    receipt_documents,
			    first_failed_line,
			    failed_at,
			    completed_at,
			    last_error_code,
			    last_error_retryable,
			    first_seen_at
			from ingestion_archive_processing
			where archive_idempotency_key = :archiveIdempotencyKey
			""";

	private final JdbcClient jdbcClient;

	JdbcArchiveProcessingRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	ArchiveProcessingState register(
			String archiveIdempotencyKey,
			ArchiveProcessingFingerprint fingerprint,
			Instant now
	) {
		int inserted = jdbcClient.sql("""
				insert into ingestion_archive_processing (
				    archive_idempotency_key,
				    source_fingerprint,
				    projection_revision,
				    processing_fingerprint,
				    status,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				select
				    idempotency_key,
				    :sourceFingerprint,
				    :projectionRevision,
				    :processingFingerprint,
				    :status,
				    :firstSeenAt,
				    :createdAt,
				    :updatedAt
				from ingestion_archives
				where idempotency_key = :archiveIdempotencyKey
				  and status = :stagedStatus
				on conflict (archive_idempotency_key) do nothing
				""")
				.param("sourceFingerprint", fingerprint.sourceFingerprint())
				.param("projectionRevision", fingerprint.projectionRevision())
				.param("processingFingerprint", fingerprint.processingFingerprint())
				.param(PARAM_STATUS, ArchiveProcessingStatus.PENDING.name())
				.param("firstSeenAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.param("stagedStatus", IngestionArchiveStatus.STAGED.name())
				.update();

		if (inserted == 0) {
			verifyRegistrationConflict(archiveIdempotencyKey, fingerprint);
		}
		return findByArchiveIdempotencyKey(archiveIdempotencyKey)
				.orElseThrow(() -> new IllegalStateException(
						"Registered archive processing state disappeared"));
	}

	Optional<ArchiveProcessingAttempt> claim(
			String archiveIdempotencyKey,
			Duration leaseDuration,
			Instant now
	) {
		ArchiveProcessingState state = findForUpdate(archiveIdempotencyKey).orElse(null);
		if (state == null || !isClaimable(state, now)) {
			return Optional.empty();
		}

		UUID token = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update ingestion_archive_processing
				set status = :status,
				    attempt_token = :attemptToken,
				    lease_expires_at = :leaseExpiresAt,
				    attempt_count = attempt_count + 1,
				    last_attempt_at = :lastAttemptAt,
				    delivered_records = 0,
				    source_invalid_records = 0,
				    mapping_rejected_records = 0,
				    submitted_operations = 0,
				    succeeded_operations = 0,
				    failed_operations = 0,
				    receipt_documents = 0,
				    first_failed_line = null,
				    failed_at = null,
				    completed_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    updated_at = :updatedAt
				where archive_idempotency_key = :archiveIdempotencyKey
				""")
				.param(PARAM_STATUS, ArchiveProcessingStatus.PROCESSING.name())
				.param(PARAM_ATTEMPT_TOKEN, token)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("lastAttemptAt", Timestamp.from(now))
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.update();
		if (updated != 1) {
			throw new IllegalStateException(
					"Claim changed an unexpected number of processing rows: " + updated);
		}

		return Optional.of(new ArchiveProcessingAttempt(
				archiveIdempotencyKey,
				state.fingerprint(),
				token,
				leaseExpiresAt,
				state.attempt().count() + 1,
				state.status() == ArchiveProcessingStatus.PROCESSING
		));
	}

	AttemptTransitionResult checkpoint(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress,
			Instant leaseExpiresAt,
			Instant now
	) {
		int updated = bindProgress(
				jdbcClient.sql("""
						update ingestion_archive_processing
						set delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    receipt_documents = :receiptDocuments,
						    first_failed_line = :firstFailedLine,
						    lease_expires_at = greatest(lease_expires_at, :leaseExpiresAt),
						    updated_at = :updatedAt
						where archive_idempotency_key = :archiveIdempotencyKey
						  and status = :processingStatus
						  and attempt_token = :attemptToken
						  and delivered_records <= :deliveredRecords
						  and source_invalid_records <= :sourceInvalidRecords
						  and mapping_rejected_records <= :mappingRejectedRecords
						  and submitted_operations <= :submittedOperations
						  and succeeded_operations <= :succeededOperations
						  and failed_operations <= :failedOperations
						  and receipt_documents <= :receiptDocuments
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""")
						.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
						.param(PARAM_UPDATED_AT, Timestamp.from(now))
						.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
						.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
						.param(PARAM_ATTEMPT_TOKEN, attemptToken),
				progress
		).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult markIndexed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress,
			Instant now
	) {
		int updated = bindProgress(
				jdbcClient.sql("""
						update ingestion_archive_processing
						set status = :status,
						    delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    receipt_documents = :receiptDocuments,
						    first_failed_line = :firstFailedLine,
						    attempt_token = null,
						    lease_expires_at = null,
						    failed_at = null,
						    completed_at = :completedAt,
						    last_error_code = null,
						    last_error_retryable = null,
						    updated_at = :updatedAt
						where archive_idempotency_key = :archiveIdempotencyKey
						  and status = :processingStatus
						  and attempt_token = :attemptToken
						  and delivered_records <= :deliveredRecords
						  and source_invalid_records <= :sourceInvalidRecords
						  and mapping_rejected_records <= :mappingRejectedRecords
						  and submitted_operations <= :submittedOperations
						  and succeeded_operations <= :succeededOperations
						  and failed_operations <= :failedOperations
						  and receipt_documents <= :receiptDocuments
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""")
						.param(PARAM_STATUS, ArchiveProcessingStatus.INDEXED.name())
						.param("completedAt", Timestamp.from(now))
						.param(PARAM_UPDATED_AT, Timestamp.from(now))
						.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
						.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
						.param(PARAM_ATTEMPT_TOKEN, attemptToken),
				progress
		).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult markFailed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress,
			Instant now
	) {
		int updated = bindProgress(
				jdbcClient.sql("""
						update ingestion_archive_processing
						set status = :status,
						    delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    receipt_documents = :receiptDocuments,
						    first_failed_line = :firstFailedLine,
						    attempt_token = null,
						    lease_expires_at = null,
						    failed_at = :failedAt,
						    completed_at = null,
						    last_error_code = :errorCode,
						    last_error_retryable = :retryable,
						    updated_at = :updatedAt
						where archive_idempotency_key = :archiveIdempotencyKey
						  and status = :processingStatus
						  and attempt_token = :attemptToken
						  and delivered_records <= :deliveredRecords
						  and source_invalid_records <= :sourceInvalidRecords
						  and mapping_rejected_records <= :mappingRejectedRecords
						  and submitted_operations <= :submittedOperations
						  and succeeded_operations <= :succeededOperations
						  and failed_operations <= :failedOperations
						  and receipt_documents <= :receiptDocuments
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""")
						.param(PARAM_STATUS, ArchiveProcessingStatus.FAILED.name())
						.param("failedAt", Timestamp.from(now))
						.param("errorCode", failure.errorCode())
						.param("retryable", failure.retryable())
						.param(PARAM_UPDATED_AT, Timestamp.from(now))
						.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
						.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
						.param(PARAM_ATTEMPT_TOKEN, attemptToken),
				progress
		).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			ArchiveProcessingFailure failure,
			Instant now
	) {
		int updated = jdbcClient.sql("""
				update ingestion_archive_processing
				set status = :status,
				    failed_at = :failedAt,
				    completed_at = null,
				    last_error_code = :errorCode,
				    last_error_retryable = :retryable,
				    updated_at = :updatedAt
				where archive_idempotency_key = :archiveIdempotencyKey
				  and processing_fingerprint = :processingFingerprint
				  and attempt_count = :expectedAttemptCount
				  and status = :indexedStatus
				""")
				.param(PARAM_STATUS, ArchiveProcessingStatus.FAILED.name())
				.param("failedAt", Timestamp.from(now))
				.param("errorCode", failure.errorCode())
				.param("retryable", failure.retryable())
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.param("processingFingerprint", expectedProcessingFingerprint)
				.param("expectedAttemptCount", expectedAttemptCount)
				.param("indexedStatus", ArchiveProcessingStatus.INDEXED.name())
				.update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	Optional<ArchiveProcessingState> findByArchiveIdempotencyKey(
			String archiveIdempotencyKey
	) {
		return jdbcClient.sql(SELECT_PROCESSING_STATE_BY_ARCHIVE_KEY)
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.query(ArchiveProcessingJdbcMapper.STATE)
				.optional();
	}

	private Optional<ArchiveProcessingState> findForUpdate(String archiveIdempotencyKey) {
		return jdbcClient.sql(SELECT_PROCESSING_STATE_BY_ARCHIVE_KEY + "for update")
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.query(ArchiveProcessingJdbcMapper.STATE)
				.optional();
	}

	private void verifyRegistrationConflict(
			String archiveIdempotencyKey,
			ArchiveProcessingFingerprint fingerprint
	) {
		Optional<String> parentStatus = jdbcClient.sql("""
				select status
				from ingestion_archives
				where idempotency_key = :archiveIdempotencyKey
				""")
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.query(String.class)
				.optional();
		if (parentStatus.isEmpty()) {
			throw new IllegalArgumentException("Unknown acquisition archive");
		}
		if (!IngestionArchiveStatus.STAGED.name().equals(parentStatus.orElseThrow())) {
			throw new IllegalStateException(
					"Acquisition archive must be STAGED before processing registration");
		}
		ArchiveProcessingState existing = findByArchiveIdempotencyKey(archiveIdempotencyKey)
				.orElseThrow(() -> new IllegalStateException(
						"Concurrent processing registration disappeared"));
		if (!existing.fingerprint().equals(fingerprint)) {
			throw new IllegalStateException(
					"Processing fingerprint conflicts with registered state");
		}
	}

	private static boolean isClaimable(ArchiveProcessingState state, Instant now) {
		return switch (state.status()) {
			case PENDING -> true;
			case FAILED -> state.failure() != null
					&& state.failure().failure().retryable();
			case PROCESSING -> !state.attempt().leaseExpiresAt().isAfter(now);
			case INDEXED -> false;
		};
	}

	private static JdbcClient.StatementSpec bindProgress(
			JdbcClient.StatementSpec statement,
			ArchiveProcessingProgress progress
	) {
		return statement
				.param(PARAM_DELIVERED, progress.deliveredRecords())
				.param(PARAM_SOURCE_INVALID, progress.sourceInvalidRecords())
				.param(PARAM_MAPPING_REJECTED, progress.mappingRejectedRecords())
				.param(PARAM_SUBMITTED, progress.submittedOperations())
				.param(PARAM_SUCCEEDED, progress.succeededOperations())
				.param(PARAM_FAILED, progress.failedOperations())
				.param(PARAM_RECEIPT, progress.receiptDocuments())
				.param(PARAM_FIRST_FAILED_LINE, progress.firstFailedLine(), Types.BIGINT);
	}
}
