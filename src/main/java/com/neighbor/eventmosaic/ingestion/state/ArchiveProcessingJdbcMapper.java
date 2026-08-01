package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingReceipt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.RecordedArchiveProcessingFailure;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;

/**
 * Преобразует только rows отдельной processing state machine.
 */
final class ArchiveProcessingJdbcMapper {

	static final RowMapper<ArchiveProcessingState> STATE =
			ArchiveProcessingJdbcMapper::mapState;

	private ArchiveProcessingJdbcMapper() {
	}

	private static ArchiveProcessingState mapState(ResultSet resultSet, int rowNumber)
			throws SQLException {
		ArchiveProcessingStatus status = ArchiveProcessingStatus.valueOf(
				resultSet.getString("status"));
		return new ArchiveProcessingState(
				resultSet.getString("archive_idempotency_key"),
				new ArchiveProcessingFingerprint(
						resultSet.getString("source_fingerprint"),
						resultSet.getString("projection_revision"),
						resultSet.getString("processing_fingerprint")),
				status,
				new ArchiveProcessingAttemptState(
						resultSet.getInt("total_attempt_count"),
						resultSet.getObject("attempt_token", UUID.class),
						nullableInstant(resultSet, "last_attempt_at"),
						nullableInstant(resultSet, "lease_expires_at"),
						new AutomaticRetryState(
								resultSet.getInt("automatic_retries_used"),
								resultSet.getInt("consecutive_retryable_failures"),
								resultSet.getInt("automatic_retry_limit"),
								nullableInstant(resultSet, "retry_not_before"))),
				mapTargetBinding(resultSet),
				mapProgress(resultSet),
				mapReceipt(resultSet),
				mapFailure(resultSet),
				instant(resultSet, "first_seen_at"),
				nullableInstant(resultSet, "completed_at")
		);
	}

	private static ArchiveProcessingProgress mapProgress(ResultSet resultSet)
			throws SQLException {
		Long actualDocumentCount = resultSet.getObject("actual_document_count", Long.class);
		long succeededOperations = resultSet.getLong("succeeded_operations");
		long receiptDocuments = actualDocumentCount == null ? 0 : actualDocumentCount;
		return new ArchiveProcessingProgress(
				resultSet.getLong("delivered_records"),
				resultSet.getLong("source_invalid_records"),
				resultSet.getLong("mapping_rejected_records"),
				resultSet.getLong("submitted_operations"),
				succeededOperations,
				resultSet.getLong("failed_operations"),
				receiptDocuments,
				resultSet.getObject("first_failed_line", Long.class)
		);
	}

	private static ArchiveProcessingTargetBinding mapTargetBinding(ResultSet resultSet)
			throws SQLException {
		Long generationId = resultSet.getObject("bound_generation_id", Long.class);
		if (generationId == null) {
			return null;
		}
		return new ArchiveProcessingTargetBinding(
				GdeltIndexKind.valueOf(resultSet.getString("bound_index_kind")),
				resultSet.getString("logical_partition_key"),
				resultSet.getLong("bound_partition_state_version"),
				generationId,
				resultSet.getObject("bound_generation_uuid", UUID.class),
				resultSet.getString("bound_index_name"),
				resultSet.getString("bound_index_uuid"));
	}

	private static ArchiveProcessingReceipt mapReceipt(ResultSet resultSet)
			throws SQLException {
		Long expectedDocumentCount = resultSet.getObject("expected_document_count", Long.class);
		if (expectedDocumentCount == null) {
			return null;
		}
		return new ArchiveProcessingReceipt(
				expectedDocumentCount,
				resultSet.getLong("actual_document_count"),
				resultSet.getLong("verified_generation_id"),
				resultSet.getString("verified_index_uuid"),
				instant(resultSet, "receipt_verified_at"));
	}

	private static RecordedArchiveProcessingFailure mapFailure(ResultSet resultSet)
			throws SQLException {
		String code = resultSet.getString("last_error_code");
		if (code == null) {
			return null;
		}
		Boolean retryable = resultSet.getObject("last_error_retryable", Boolean.class);
		if (retryable == null) {
			throw new SQLException("Required processing retryable flag is null");
		}
		return new RecordedArchiveProcessingFailure(
				new ArchiveProcessingFailure(code, retryable),
				instant(resultSet, "failed_at"));
	}

	private static Instant instant(ResultSet resultSet, String column) throws SQLException {
		return resultSet.getTimestamp(column).toInstant();
	}

	private static Instant nullableInstant(ResultSet resultSet, String column)
			throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}
}
