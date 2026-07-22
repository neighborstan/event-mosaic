package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Выполняет регистрацию, claim и conditional transitions archive rows.
 */
@Repository
class JdbcIngestionArchiveRepository {

	private final JdbcClient jdbcClient;

	JdbcIngestionArchiveRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	void register(long runId, DiscoveredArchive archive, Instant now) {
		int inserted = jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key, run_id, source_update_time, archive_name,
				    metadata_url, expected_md5, archive_type,
				    status, file_size_bytes, first_seen_at, created_at, updated_at
				)
				values (
				    :idempotencyKey, :runId, :sourceUpdateTime, :archiveName,
				    :metadataUrl, :expectedMd5, :archiveType,
				    :status, :fileSizeBytes, :firstSeenAt, :createdAt, :updatedAt
				)
				on conflict do nothing
				""")
				.param("idempotencyKey", archive.idempotencyKey())
				.param("runId", runId)
				.param("sourceUpdateTime", Timestamp.from(archive.sourceUpdateTime()))
				.param("archiveName", archive.archiveName())
				.param("metadataUrl", archive.metadataUri().toString())
				.param("expectedMd5", archive.expectedMd5())
				.param("archiveType", archive.archiveType().name())
				.param("status", IngestionArchiveStatus.DISCOVERED.name())
				.param("fileSizeBytes", archive.expectedSizeBytes())
				.param("firstSeenAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.update();
		if (inserted == 1) {
			return;
		}

		IngestionArchiveState existing = findByRunAndType(runId, archive.archiveType())
				.orElseThrow(() -> new SourceDataViolationException(
						IngestionErrorCode.ARCHIVE_METADATA_CONFLICT));
		verifyMetadata(existing.archive(), archive);
	}

	Optional<ClaimedArchive> claim(String idempotencyKey, Duration leaseDuration, Instant now) {
		Optional<IngestionArchiveState> locked = jdbcClient.sql("""
				select *
				from ingestion_archives
				where idempotency_key = :idempotencyKey
				for update
				""")
				.param("idempotencyKey", idempotencyKey)
				.query(IngestionJdbcMappers.ARCHIVE)
				.optional();
		if (locked.isEmpty() || !isClaimable(locked.orElseThrow(), now)) {
			return Optional.empty();
		}

		IngestionArchiveState state = locked.orElseThrow();
		boolean recovered = state.status() == IngestionArchiveStatus.PROCESSING;
		UUID token = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		jdbcClient.sql("""
				update ingestion_archives
				set status = :status,
				    failed_at = null,
				    attempt_token = :attemptToken,
				    lease_expires_at = :leaseExpiresAt,
				    last_attempt_at = :lastAttemptAt,
				    attempt_count = attempt_count + 1,
				    last_error_code = null,
				    last_error_retryable = null,
				    updated_at = :updatedAt
				where idempotency_key = :idempotencyKey
				""")
				.param("status", IngestionArchiveStatus.PROCESSING.name())
				.param("attemptToken", token)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("lastAttemptAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("idempotencyKey", idempotencyKey)
				.update();

		return Optional.of(new ClaimedArchive(
				state.runId(),
				new ArchiveAttempt(
						state.archive(),
						token,
						leaseExpiresAt,
						state.attempt().count() + 1,
						recovered
				)
		));
	}

	ArchiveTransition markStaged(
			String idempotencyKey,
			UUID attemptToken,
			StagedArchive stagedArchive,
			Instant now
	) {
		long runId = requireRunId(idempotencyKey);
		int updated = jdbcClient.sql("""
				update ingestion_archives
				set status = :status,
				    failed_at = null,
				    actual_size_bytes = :actualSizeBytes,
				    actual_md5 = :actualMd5,
				    staged_archive_path = :stagedArchivePath,
				    staged_csv_path = :stagedCsvPath,
				    completed_at = :completedAt,
				    attempt_token = null,
				    lease_expires_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    updated_at = :updatedAt
				where idempotency_key = :idempotencyKey
				  and status = :processingStatus
				  and attempt_token = :attemptToken
				""")
				.param("status", IngestionArchiveStatus.STAGED.name())
				.param("actualSizeBytes", stagedArchive.actualSizeBytes())
				.param("actualMd5", stagedArchive.actualMd5())
				.param("stagedArchivePath", stagedArchive.archivePath().toString())
				.param("stagedCsvPath", stagedArchive.csvPath().toString())
				.param("completedAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("idempotencyKey", idempotencyKey)
				.param("processingStatus", IngestionArchiveStatus.PROCESSING.name())
				.param("attemptToken", attemptToken)
				.update();
		return new ArchiveTransition(runId, AttemptTransitionResult.fromUpdatedRows(updated));
	}

	ArchiveTransition markFailed(
			String idempotencyKey,
			UUID attemptToken,
			IngestionFailure failure,
			Instant now
	) {
		long runId = requireRunId(idempotencyKey);
		int updated = jdbcClient.sql("""
				update ingestion_archives
				set status = :status,
				    failed_at = :failedAt,
				    attempt_token = null,
				    lease_expires_at = null,
				    last_error_code = :errorCode,
				    last_error_retryable = :retryable,
				    updated_at = :updatedAt
				where idempotency_key = :idempotencyKey
				  and status = :processingStatus
				  and attempt_token = :attemptToken
				""")
				.param("status", IngestionArchiveStatus.FAILED.name())
				.param("failedAt", Timestamp.from(now))
				.param("errorCode", failure.code().code())
				.param("retryable", failure.retryable())
				.param("updatedAt", Timestamp.from(now))
				.param("idempotencyKey", idempotencyKey)
				.param("processingStatus", IngestionArchiveStatus.PROCESSING.name())
				.param("attemptToken", attemptToken)
				.update();
		return new ArchiveTransition(runId, AttemptTransitionResult.fromUpdatedRows(updated));
	}

	Optional<IngestionArchiveState> findByIdempotencyKey(String idempotencyKey) {
		return jdbcClient.sql("select * from ingestion_archives where idempotency_key = :idempotencyKey")
				.param("idempotencyKey", idempotencyKey)
				.query(IngestionJdbcMappers.ARCHIVE)
				.optional();
	}

	List<IngestionArchiveState> findByRunId(long runId) {
		return jdbcClient.sql("""
				select *
				from ingestion_archives
				where run_id = :runId
				order by archive_type
				""")
				.param("runId", runId)
				.query(IngestionJdbcMappers.ARCHIVE)
				.list();
	}

	private Optional<IngestionArchiveState> findByRunAndType(long runId, ArchiveType archiveType) {
		return jdbcClient.sql("""
				select *
				from ingestion_archives
				where run_id = :runId
				  and archive_type = :archiveType
				""")
				.param("runId", runId)
				.param("archiveType", archiveType.name())
				.query(IngestionJdbcMappers.ARCHIVE)
				.optional();
	}

	Optional<Instant> findCompletionProgress(ArchiveType archiveType) {
		return jdbcClient.sql("""
				select max(source_update_time)
				from ingestion_archives
				where archive_type = :archiveType
				  and status = :status
				""")
				.param("archiveType", archiveType.name())
				.param("status", IngestionArchiveStatus.STAGED.name())
				.query((resultSet, rowNumber) -> IngestionJdbcMappers.nullableInstant(resultSet, 1))
				.optional()
				.flatMap(Optional::ofNullable);
	}

	private static boolean isClaimable(IngestionArchiveState state, Instant now) {
		if (state.status() == IngestionArchiveStatus.STAGED) {
			return false;
		}
		if (state.status() == IngestionArchiveStatus.PROCESSING) {
			return state.attempt().leaseExpiresAt() == null || !state.attempt().leaseExpiresAt().isAfter(now);
		}
		if (state.status() == IngestionArchiveStatus.FAILED) {
			return state.failure() != null
					&& state.failure().failure().retryable();
		}
		return state.status() == IngestionArchiveStatus.DISCOVERED;
	}

	private static void verifyMetadata(DiscoveredArchive existing, DiscoveredArchive discovered) {
		if (!hasSameMetadata(existing, discovered)) {
			throw new SourceDataViolationException(IngestionErrorCode.ARCHIVE_METADATA_CONFLICT);
		}
	}

	private static boolean hasSameMetadata(DiscoveredArchive existing, DiscoveredArchive discovered) {
		return existing.archiveName().equals(discovered.archiveName())
				&& existing.metadataUri().equals(discovered.metadataUri())
				&& existing.expectedMd5().equals(discovered.expectedMd5())
				&& existing.expectedSizeBytes() == discovered.expectedSizeBytes();
	}

	private long requireRunId(String idempotencyKey) {
		return jdbcClient.sql("select run_id from ingestion_archives where idempotency_key = :idempotencyKey")
				.param("idempotencyKey", idempotencyKey)
				.query(Long.class)
				.optional()
				.orElseThrow(() -> new IllegalArgumentException("Unknown archive idempotency key"));
	}
}
