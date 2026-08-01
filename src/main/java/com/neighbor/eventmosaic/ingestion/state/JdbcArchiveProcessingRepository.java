package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimResult;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.retry.RetryDelayPolicy;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
	private static final String PARAM_PARTITION_KEY = "partitionKey";
	private static final String PARAM_PARTITION_VERSION = "partitionStateVersion";
	private static final String PARAM_GENERATION_ID = "generationId";
	private static final String PARAM_GENERATION_UUID = "generationUuid";
	private static final String PARAM_INDEX_KIND = "indexKind";
	private static final String PARAM_INDEX_NAME = "indexName";
	private static final String PARAM_INDEX_UUID = "indexUuid";
	private static final String PARAM_PROCESSING_STATUS = "processingStatus";
	private static final String PARAM_STATUS = "status";
	private static final String PARAM_UPDATED_AT = "updatedAt";
	private static final String PARAM_DELIVERED = "deliveredRecords";
	private static final String PARAM_SOURCE_INVALID = "sourceInvalidRecords";
	private static final String PARAM_MAPPING_REJECTED = "mappingRejectedRecords";
	private static final String PARAM_SUBMITTED = "submittedOperations";
	private static final String PARAM_SUCCEEDED = "succeededOperations";
	private static final String PARAM_FAILED = "failedOperations";
	private static final String PARAM_FIRST_FAILED_LINE = "firstFailedLine";
	private static final String STORED_BINDING_PREDICATE = """
			and logical_partition_key = :expectedPartitionKey
			and bound_partition_state_version = :expectedPartitionStateVersion
			and bound_generation_id = :expectedGenerationId
			and bound_generation_uuid = :expectedGenerationUuid
			and bound_index_kind = :expectedIndexKind
			and bound_index_name = :expectedIndexName
			and bound_index_uuid = :expectedIndexUuid
			""";
	private static final String ACTIVE_BINDING_PREDICATE = """
			and logical_partition_key = :partitionKey
			and bound_partition_state_version = :partitionStateVersion
			and bound_generation_id = :generationId
			and bound_generation_uuid = :generationUuid
			and bound_index_kind = :indexKind
			and bound_index_name = :indexName
			and bound_index_uuid = :indexUuid
			and exists (
			    select 1
			    from index_logical_partitions partition_state
			    join index_generations generation
			      on generation.partition_key = partition_state.partition_key
			    where partition_state.partition_key = :partitionKey
			      and partition_state.state_version = :partitionStateVersion
			      and generation.id = :generationId
			      and generation.generation_uuid = :generationUuid
			      and generation.state = 'ACTIVE'
			      and (
			          (
			              :indexKind = 'EVENT'
			              and generation.event_index_name = :indexName
			              and generation.event_index_uuid = :indexUuid
			          )
			          or
			          (
			              :indexKind = 'MENTION'
			              and generation.mention_index_name = :indexName
			              and generation.mention_index_uuid = :indexUuid
			          )
			      )
			      and not exists (
			          select 1
			          from index_maintenance_operations maintenance
			          where maintenance.partition_key = partition_state.partition_key
			            and maintenance.phase not in ('COMPLETED', 'FAILED')
			      )
			)
			""";
	private static final String SELECT_PROCESSING_STATE_BY_ARCHIVE_KEY = """
			select
			    archive_idempotency_key,
			    source_fingerprint,
			    projection_revision,
			    processing_fingerprint,
			    status,
			    state_version,
			    attempt_token,
			    lease_expires_at,
			    logical_partition_key,
			    bound_partition_state_version,
			    bound_generation_id,
			    bound_generation_uuid,
			    bound_index_kind,
			    bound_index_name,
			    bound_index_uuid,
			    total_attempt_count,
			    automatic_retries_used,
			    consecutive_retryable_failures,
			    automatic_retry_limit,
			    retry_not_before,
			    last_attempt_at,
			    delivered_records,
			    source_invalid_records,
			    mapping_rejected_records,
			    submitted_operations,
			    succeeded_operations,
			    failed_operations,
			    actual_document_count,
			    expected_document_count,
			    receipt_digest_algorithm,
			    expected_identity_digest,
			    verified_generation_id,
			    verified_index_uuid,
			    actual_identity_digest,
			    receipt_verified_at,
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
	private final int automaticRetryLimit;
	private final RetryDelayPolicy retryDelayPolicy;

	JdbcArchiveProcessingRepository(
			JdbcClient jdbcClient,
			BackendDataProperties properties,
			RetryDelayPolicy retryDelayPolicy
	) {
		this.jdbcClient = jdbcClient;
		this.automaticRetryLimit = properties.retry().automaticRetryLimit();
		this.retryDelayPolicy = retryDelayPolicy;
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
				    automatic_retry_limit,
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
				    :automaticRetryLimit,
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
				.param("automaticRetryLimit", automaticRetryLimit)
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

	ArchiveProcessingClaimResult claim(
			String archiveIdempotencyKey,
			ArchiveProcessingTargetBinding targetBinding,
			Duration leaseDuration,
			Instant now
	) {
		ArchiveProcessingState state = findForUpdate(archiveIdempotencyKey).orElse(null);
		if (state == null || !isClaimable(state, now)) {
			return ArchiveProcessingClaimResult.outcome(
					ArchiveProcessingClaimStatus.NOT_CLAIMABLE);
		}

		ArchiveProcessingClaimStatus targetStatus = validateAndLockTarget(
				archiveIdempotencyKey,
				targetBinding);
		if (targetStatus != ArchiveProcessingClaimStatus.CLAIMED) {
			return ArchiveProcessingClaimResult.outcome(targetStatus);
		}

		UUID token = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		boolean recovered = state.status() == ArchiveProcessingStatus.PROCESSING;
		boolean automaticRetry = state.status() == ArchiveProcessingStatus.FAILED || recovered;
		int updated = jdbcClient.sql("""
				update ingestion_archive_processing
				set status = :status,
				    attempt_token = :attemptToken,
				    lease_expires_at = :leaseExpiresAt,
				    total_attempt_count = total_attempt_count + 1,
				    automatic_retries_used = automatic_retries_used + :automaticRetryIncrement,
				    retry_not_before = null,
				    last_attempt_at = :lastAttemptAt,
				    logical_partition_key = :partitionKey,
				    bound_partition_state_version = :partitionStateVersion,
				    bound_generation_id = :generationId,
				    bound_generation_uuid = :generationUuid,
				    bound_index_kind = :indexKind,
				    bound_index_name = :indexName,
				    bound_index_uuid = :indexUuid,
				    delivered_records = 0,
				    source_invalid_records = 0,
				    mapping_rejected_records = 0,
				    submitted_operations = 0,
				    succeeded_operations = 0,
				    failed_operations = 0,
				    first_failed_line = null,
				    expected_document_count = null,
				    receipt_digest_algorithm = null,
				    expected_identity_digest = null,
				    verified_generation_id = null,
				    verified_index_uuid = null,
				    actual_document_count = null,
				    actual_identity_digest = null,
				    receipt_verified_at = null,
				    failed_at = null,
				    completed_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where archive_idempotency_key = :archiveIdempotencyKey
				""")
				.param(PARAM_STATUS, ArchiveProcessingStatus.PROCESSING.name())
				.param(PARAM_ATTEMPT_TOKEN, token)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("lastAttemptAt", Timestamp.from(now))
				.param("automaticRetryIncrement", automaticRetry ? 1 : 0)
				.param(PARAM_PARTITION_KEY, targetBinding.partitionKey())
				.param(PARAM_PARTITION_VERSION, targetBinding.partitionStateVersion())
				.param(PARAM_GENERATION_ID, targetBinding.generationId())
				.param(PARAM_GENERATION_UUID, targetBinding.generationUuid())
				.param(PARAM_INDEX_KIND, targetBinding.indexKind().name())
				.param(PARAM_INDEX_NAME, targetBinding.indexName())
				.param(PARAM_INDEX_UUID, targetBinding.indexUuid())
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.update();
		if (updated != 1) {
			throw new IllegalStateException(
					"Claim changed an unexpected number of processing rows: " + updated);
		}

		return ArchiveProcessingClaimResult.claimed(new ArchiveProcessingAttempt(
				archiveIdempotencyKey,
				state.fingerprint(),
				targetBinding,
				token,
				leaseExpiresAt,
				state.attempt().count() + 1,
				recovered
		));
	}

	AttemptTransitionResult checkpoint(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			Instant leaseExpiresAt,
			Instant now
	) {
		if (!ownsCurrentTarget(attempt)) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		int updated = bindTarget(
				bindProgress(
						jdbcClient.sql("""
						update ingestion_archive_processing
						set delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    first_failed_line = :firstFailedLine,
						    lease_expires_at = greatest(lease_expires_at, :leaseExpiresAt),
						    state_version = state_version + 1,
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
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""" + ACTIVE_BINDING_PREDICATE)
						.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
						.param(PARAM_UPDATED_AT, Timestamp.from(now))
						.param(PARAM_ARCHIVE_KEY, attempt.archiveIdempotencyKey())
						.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
						.param(PARAM_ATTEMPT_TOKEN, attempt.token()),
						progress),
				attempt.targetBinding()).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult markIndexed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification,
			Instant now
	) {
		if (!ownsCurrentTarget(attempt)) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		int updated = bindTarget(
				bindProgress(
						bindReceiptVerification(
								jdbcClient.sql("""
						update ingestion_archive_processing
						set status = :status,
						    delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    first_failed_line = :firstFailedLine,
						    attempt_token = null,
						    lease_expires_at = null,
						    failed_at = null,
						    completed_at = :completedAt,
						    automatic_retries_used = 0,
						    consecutive_retryable_failures = 0,
						    retry_not_before = null,
						    last_error_code = null,
						    last_error_retryable = null,
						    expected_document_count = :expectedDocumentCount,
						    receipt_digest_algorithm = :receiptDigestAlgorithm,
						    expected_identity_digest = :expectedIdentityDigest,
						    verified_generation_id = :verifiedGenerationId,
						    verified_index_uuid = :verifiedIndexUuid,
						    actual_document_count = :actualDocumentCount,
						    actual_identity_digest = :actualIdentityDigest,
						    receipt_verified_at = :receiptVerifiedAt,
						    state_version = state_version + 1,
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
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""" + ACTIVE_BINDING_PREDICATE)
								.param(PARAM_STATUS, ArchiveProcessingStatus.INDEXED.name())
								.param("completedAt", Timestamp.from(now))
								.param(PARAM_UPDATED_AT, Timestamp.from(now))
								.param(PARAM_ARCHIVE_KEY, attempt.archiveIdempotencyKey())
								.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
								.param(PARAM_ATTEMPT_TOKEN, attempt.token()),
								verification,
								attempt.targetBinding(),
								now),
						progress),
				attempt.targetBinding()).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult markFailed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification,
			Instant now
	) {
		ArchiveProcessingState state = findForUpdate(attempt.archiveIdempotencyKey())
				.orElse(null);
		if (state == null
				|| validateAndLockTarget(
						attempt.archiveIdempotencyKey(),
						attempt.targetBinding()) != ArchiveProcessingClaimStatus.CLAIMED) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		OffsetDateTime retryNotBefore = failure.retryable()
				? OffsetDateTime.ofInstant(
						retryDelayPolicy.retryNotBefore(
								now,
								state.attempt().retry().consecutiveRetryableFailures(),
								Duration.ZERO),
						ZoneOffset.UTC)
				: null;
		int updated = bindTarget(
				bindProgress(
						bindReceiptVerification(
								jdbcClient.sql("""
						update ingestion_archive_processing
						set status = :status,
						    delivered_records = :deliveredRecords,
						    source_invalid_records = :sourceInvalidRecords,
						    mapping_rejected_records = :mappingRejectedRecords,
						    submitted_operations = :submittedOperations,
						    succeeded_operations = :succeededOperations,
						    failed_operations = :failedOperations,
						    first_failed_line = :firstFailedLine,
						    attempt_token = null,
						    lease_expires_at = null,
						    bound_partition_state_version = null,
						    bound_generation_id = null,
						    bound_generation_uuid = null,
						    bound_index_kind = null,
						    bound_index_name = null,
						    bound_index_uuid = null,
						    failed_at = :failedAt,
						    completed_at = null,
						    consecutive_retryable_failures = case
						        when :retryable then consecutive_retryable_failures + 1
						        else 0
						    end,
						    retry_not_before = :retryNotBefore,
						    last_error_code = :errorCode,
						    last_error_retryable = :retryable,
						    expected_document_count = :expectedDocumentCount,
						    receipt_digest_algorithm = :receiptDigestAlgorithm,
						    expected_identity_digest = :expectedIdentityDigest,
						    verified_generation_id = :verifiedGenerationId,
						    verified_index_uuid = :verifiedIndexUuid,
						    actual_document_count = :actualDocumentCount,
						    actual_identity_digest = :actualIdentityDigest,
						    receipt_verified_at = :receiptVerifiedAt,
						    state_version = state_version + 1,
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
						  and (
						      first_failed_line is null
						      or first_failed_line = :firstFailedLine
						  )
						""" + ACTIVE_BINDING_PREDICATE)
								.param(PARAM_STATUS, ArchiveProcessingStatus.FAILED.name())
								.param("failedAt", Timestamp.from(now))
								.param("errorCode", failure.errorCode())
								.param("retryable", failure.retryable())
								.param("retryNotBefore", retryNotBefore, Types.TIMESTAMP_WITH_TIMEZONE)
								.param(PARAM_UPDATED_AT, Timestamp.from(now))
								.param(PARAM_ARCHIVE_KEY, attempt.archiveIdempotencyKey())
								.param(PARAM_PROCESSING_STATUS, ArchiveProcessingStatus.PROCESSING.name())
								.param(PARAM_ATTEMPT_TOKEN, attempt.token()),
								verification,
								attempt.targetBinding(),
								now),
						progress),
				attempt.targetBinding()).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult recordReceiptMatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification,
			Instant now
	) {
		ArchiveProcessingState state = findForUpdate(archiveIdempotencyKey).orElse(null);
		if (state == null
				|| validateAndLockTarget(
						archiveIdempotencyKey,
						verifiedCurrentTargetBinding)
						!= ArchiveProcessingClaimStatus.CLAIMED) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		int updated = bindStoredTarget(bindTarget(bindReceiptVerification(jdbcClient.sql("""
				update ingestion_archive_processing
				set logical_partition_key = :partitionKey,
				    bound_partition_state_version = :partitionStateVersion,
				    bound_generation_id = :generationId,
				    bound_generation_uuid = :generationUuid,
				    bound_index_kind = :indexKind,
				    bound_index_name = :indexName,
				    bound_index_uuid = :indexUuid,
				    expected_document_count = :expectedDocumentCount,
				    receipt_digest_algorithm = :receiptDigestAlgorithm,
				    expected_identity_digest = :expectedIdentityDigest,
				    verified_generation_id = :verifiedGenerationId,
				    verified_index_uuid = :verifiedIndexUuid,
				    actual_document_count = :actualDocumentCount,
				    actual_identity_digest = :actualIdentityDigest,
				    receipt_verified_at = :receiptVerifiedAt,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where archive_idempotency_key = :archiveIdempotencyKey
				  and processing_fingerprint = :processingFingerprint
				  and total_attempt_count = :expectedAttemptCount
				  and state_version = :expectedStateVersion
				  and status = :indexedStatus
				  and expected_document_count = :expectedDocumentCount
				  and receipt_digest_algorithm = :receiptDigestAlgorithm
				  and expected_identity_digest = :expectedIdentityDigest
				""" + STORED_BINDING_PREDICATE)
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.param("processingFingerprint", expectedProcessingFingerprint)
				.param("expectedAttemptCount", expectedAttemptCount)
				.param("expectedStateVersion", expectedStateVersion)
				.param("indexedStatus", ArchiveProcessingStatus.INDEXED.name()),
				verification,
				verifiedCurrentTargetBinding,
				now),
				verifiedCurrentTargetBinding),
				expectedStoredTargetBinding).update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification,
			ArchiveProcessingFailure failure,
			Instant now
	) {
		ArchiveProcessingState state = findForUpdate(archiveIdempotencyKey).orElse(null);
		if (state == null
				|| validateAndLockTarget(
						archiveIdempotencyKey,
						verifiedCurrentTargetBinding)
						!= ArchiveProcessingClaimStatus.CLAIMED) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		OffsetDateTime retryNotBefore = failure.retryable()
				? OffsetDateTime.ofInstant(
						retryDelayPolicy.retryNotBefore(
								now,
								state.attempt().retry().consecutiveRetryableFailures(),
								Duration.ZERO),
						ZoneOffset.UTC)
				: null;
		int updated = bindStoredTarget(bindReceiptVerification(jdbcClient.sql("""
				update ingestion_archive_processing
				set status = :status,
				    failed_at = :failedAt,
				    completed_at = null,
				    verified_generation_id = :verifiedGenerationId,
				    verified_index_uuid = :verifiedIndexUuid,
				    actual_document_count = :actualDocumentCount,
				    actual_identity_digest = :actualIdentityDigest,
				    receipt_verified_at = :receiptVerifiedAt,
				    bound_partition_state_version = null,
				    bound_generation_id = null,
				    bound_generation_uuid = null,
				    bound_index_kind = null,
				    bound_index_name = null,
				    bound_index_uuid = null,
				    consecutive_retryable_failures = case
				        when :retryable then consecutive_retryable_failures + 1
				        else 0
				    end,
				    retry_not_before = :retryNotBefore,
				    last_error_code = :errorCode,
				    last_error_retryable = :retryable,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where archive_idempotency_key = :archiveIdempotencyKey
				  and processing_fingerprint = :processingFingerprint
				  and total_attempt_count = :expectedAttemptCount
				  and state_version = :expectedStateVersion
				  and status = :indexedStatus
				  and expected_document_count = :expectedDocumentCount
				  and receipt_digest_algorithm = :receiptDigestAlgorithm
				  and expected_identity_digest = :expectedIdentityDigest
				""" + STORED_BINDING_PREDICATE)
				.param(PARAM_STATUS, ArchiveProcessingStatus.FAILED.name())
				.param("failedAt", Timestamp.from(now))
				.param("errorCode", failure.errorCode())
				.param("retryable", failure.retryable())
				.param("retryNotBefore", retryNotBefore, Types.TIMESTAMP_WITH_TIMEZONE)
				.param(PARAM_UPDATED_AT, Timestamp.from(now))
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.param("processingFingerprint", expectedProcessingFingerprint)
				.param("expectedAttemptCount", expectedAttemptCount)
				.param("expectedStateVersion", expectedStateVersion)
				.param("indexedStatus", ArchiveProcessingStatus.INDEXED.name()),
				verification,
				verifiedCurrentTargetBinding,
				now),
				expectedStoredTargetBinding).update();
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
		if (existing.attempt().retry().automaticRetryLimit() != automaticRetryLimit) {
			throw new IllegalStateException(
					"Configured processing retry limit conflicts with durable state");
		}
	}

	private ArchiveProcessingClaimStatus validateAndLockTarget(
			String archiveIdempotencyKey,
			ArchiveProcessingTargetBinding binding
	) {
		PartitionTargetState partition = jdbcClient.sql("""
				select
				    partition_state.state_version,
				    (
				        select active.id
				        from index_generations active
				        where active.partition_key = partition_state.partition_key
				          and active.state = 'ACTIVE'
				    ) as active_generation_id,
				    partition_state.partition_start_at,
				    partition_state.partition_end_at
				from index_logical_partitions partition_state
				where partition_state.partition_key = :partitionKey
				for share
				""")
				.param(PARAM_PARTITION_KEY, binding.partitionKey())
				.query((resultSet, rowNumber) -> new PartitionTargetState(
						resultSet.getLong("state_version"),
						resultSet.getObject("active_generation_id", Long.class),
						resultSet.getTimestamp("partition_start_at").toInstant(),
						resultSet.getTimestamp("partition_end_at").toInstant()))
				.optional()
				.orElse(null);
		if (partition == null) {
			return ArchiveProcessingClaimStatus.OWNERSHIP_LOST;
		}
		ArchiveTargetState archive = jdbcClient.sql("""
				select source_update_time, archive_type
				from ingestion_archives
				where idempotency_key = :archiveIdempotencyKey
				""")
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.query((resultSet, rowNumber) -> new ArchiveTargetState(
						resultSet.getTimestamp("source_update_time").toInstant(),
						ArchiveType.valueOf(resultSet.getString("archive_type"))))
				.optional()
				.orElse(null);
		if (archive == null
				|| archive.sourceUpdateTime().isBefore(partition.startAt())
				|| !archive.sourceUpdateTime().isBefore(partition.endAt())
				|| !archive.matches(binding)) {
			return ArchiveProcessingClaimStatus.OWNERSHIP_LOST;
		}

		boolean maintenanceOpen = jdbcClient.sql("""
				select exists (
				    select 1
				    from index_maintenance_operations
				    where partition_key = :partitionKey
				      and phase not in ('COMPLETED', 'FAILED')
				)
				""")
				.param(PARAM_PARTITION_KEY, binding.partitionKey())
				.query(Boolean.class)
				.single();
		if (maintenanceOpen || partition.activeGenerationId() == null) {
			return ArchiveProcessingClaimStatus.MAINTENANCE_DEFERRED;
		}
		if (partition.stateVersion() != binding.partitionStateVersion()
				|| partition.activeGenerationId() != binding.generationId()) {
			return ArchiveProcessingClaimStatus.OWNERSHIP_LOST;
		}

		GenerationTargetState generation = jdbcClient.sql("""
				select
				    generation_uuid,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid
				from index_generations
				where id = :generationId
				  and partition_key = :partitionKey
				""")
				.param(PARAM_GENERATION_ID, binding.generationId())
				.param(PARAM_PARTITION_KEY, binding.partitionKey())
				.query((resultSet, rowNumber) -> new GenerationTargetState(
						resultSet.getObject("generation_uuid", UUID.class),
						resultSet.getString("state"),
						resultSet.getString("event_index_name"),
						resultSet.getString("event_index_uuid"),
						resultSet.getString("mention_index_name"),
						resultSet.getString("mention_index_uuid")))
				.optional()
				.orElse(null);
		if (generation == null
				|| !"ACTIVE".equals(generation.state())
				|| !binding.generationUuid().equals(generation.generationUuid())
				|| !generation.matches(binding)) {
			return ArchiveProcessingClaimStatus.OWNERSHIP_LOST;
		}
		return ArchiveProcessingClaimStatus.CLAIMED;
	}

	private boolean ownsCurrentTarget(ArchiveProcessingAttempt attempt) {
		return lockProcessingRow(attempt.archiveIdempotencyKey())
				&& validateAndLockTarget(
						attempt.archiveIdempotencyKey(),
						attempt.targetBinding()) == ArchiveProcessingClaimStatus.CLAIMED;
	}

	private boolean lockProcessingRow(String archiveIdempotencyKey) {
		return jdbcClient.sql("""
				select archive_idempotency_key
				from ingestion_archive_processing
				where archive_idempotency_key = :archiveIdempotencyKey
				for update
				""")
				.param(PARAM_ARCHIVE_KEY, archiveIdempotencyKey)
				.query(String.class)
				.optional()
				.isPresent();
	}

	private static boolean isClaimable(ArchiveProcessingState state, Instant now) {
		return switch (state.status()) {
			case PENDING -> true;
			case FAILED -> state.failure() != null
					&& state.failure().failure().retryable()
					&& !state.attempt().retry().exhausted()
					&& !state.attempt().retry().retryNotBefore().isAfter(now);
			case PROCESSING -> !state.attempt().retry().exhausted()
					&& !state.attempt().leaseExpiresAt().isAfter(now);
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
				.param(PARAM_FIRST_FAILED_LINE, progress.firstFailedLine(), Types.BIGINT);
	}

	private static JdbcClient.StatementSpec bindReceiptVerification(
			JdbcClient.StatementSpec statement,
			ArchiveReceiptVerification verification,
			ArchiveProcessingTargetBinding targetBinding,
			Instant verifiedAt
	) {
		if (verification == null) {
			return statement
					.param("expectedDocumentCount", null, Types.BIGINT)
					.param("receiptDigestAlgorithm", null, Types.VARCHAR)
					.param("expectedIdentityDigest", null, Types.VARCHAR)
					.param("verifiedGenerationId", null, Types.BIGINT)
					.param("verifiedIndexUuid", null, Types.VARCHAR)
					.param("actualDocumentCount", null, Types.BIGINT)
					.param("actualIdentityDigest", null, Types.VARCHAR)
					.param("receiptVerifiedAt", null, Types.TIMESTAMP_WITH_TIMEZONE);
		}
		return statement
				.param("expectedDocumentCount", verification.expectedDocumentCount())
				.param("receiptDigestAlgorithm", verification.algorithm())
				.param("expectedIdentityDigest", verification.expectedDigest().value())
				.param("verifiedGenerationId", targetBinding.generationId())
				.param("verifiedIndexUuid", targetBinding.indexUuid())
				.param("actualDocumentCount", verification.actualDocumentCount())
				.param("actualIdentityDigest", verification.actualDigest().value())
				.param("receiptVerifiedAt", Timestamp.from(verifiedAt));
	}

	private static JdbcClient.StatementSpec bindTarget(
			JdbcClient.StatementSpec statement,
			ArchiveProcessingTargetBinding binding
	) {
		return statement
				.param(PARAM_PARTITION_KEY, binding.partitionKey())
				.param(PARAM_PARTITION_VERSION, binding.partitionStateVersion())
				.param(PARAM_GENERATION_ID, binding.generationId())
				.param(PARAM_GENERATION_UUID, binding.generationUuid())
				.param(PARAM_INDEX_KIND, binding.indexKind().name())
				.param(PARAM_INDEX_NAME, binding.indexName())
				.param(PARAM_INDEX_UUID, binding.indexUuid());
	}

	private static JdbcClient.StatementSpec bindStoredTarget(
			JdbcClient.StatementSpec statement,
			ArchiveProcessingTargetBinding binding
	) {
		return statement
				.param("expectedPartitionKey", binding.partitionKey())
				.param(
						"expectedPartitionStateVersion",
						binding.partitionStateVersion())
				.param("expectedGenerationId", binding.generationId())
				.param("expectedGenerationUuid", binding.generationUuid())
				.param("expectedIndexKind", binding.indexKind().name())
				.param("expectedIndexName", binding.indexName())
				.param("expectedIndexUuid", binding.indexUuid());
	}

	private record PartitionTargetState(
			long stateVersion,
			Long activeGenerationId,
			Instant startAt,
			Instant endAt
	) {
	}

	private record ArchiveTargetState(Instant sourceUpdateTime, ArchiveType archiveType) {
		private boolean matches(ArchiveProcessingTargetBinding binding) {
			return switch (binding.indexKind()) {
				case EVENT -> archiveType == ArchiveType.TRANSLATION_EVENTS;
				case MENTION -> archiveType == ArchiveType.TRANSLATION_MENTIONS;
			};
		}
	}

	private record GenerationTargetState(
			UUID generationUuid,
			String state,
			String eventIndexName,
			String eventIndexUuid,
			String mentionIndexName,
			String mentionIndexUuid
	) {
		private boolean matches(ArchiveProcessingTargetBinding binding) {
			return switch (binding.indexKind()) {
				case EVENT -> binding.indexName().equals(eventIndexName)
						&& binding.indexUuid().equals(eventIndexUuid);
				case MENTION -> binding.indexName().equals(mentionIndexName)
						&& binding.indexUuid().equals(mentionIndexUuid);
			};
		}
	}
}
