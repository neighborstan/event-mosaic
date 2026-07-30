package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.RecordedArchiveProcessingFailure;
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
		return new ArchiveProcessingState(
				resultSet.getString("archive_idempotency_key"),
				new ArchiveProcessingFingerprint(
						resultSet.getString("source_fingerprint"),
						resultSet.getString("projection_revision"),
						resultSet.getString("processing_fingerprint")),
				ArchiveProcessingStatus.valueOf(resultSet.getString("status")),
				new ArchiveProcessingAttemptState(
						resultSet.getInt("attempt_count"),
						resultSet.getObject("attempt_token", UUID.class),
						nullableInstant(resultSet, "last_attempt_at"),
						nullableInstant(resultSet, "lease_expires_at")),
				mapProgress(resultSet),
				mapFailure(resultSet),
				instant(resultSet, "first_seen_at"),
				nullableInstant(resultSet, "completed_at")
		);
	}

	private static ArchiveProcessingProgress mapProgress(ResultSet resultSet)
			throws SQLException {
		return new ArchiveProcessingProgress(
				resultSet.getLong("delivered_records"),
				resultSet.getLong("source_invalid_records"),
				resultSet.getLong("mapping_rejected_records"),
				resultSet.getLong("submitted_operations"),
				resultSet.getLong("succeeded_operations"),
				resultSet.getLong("failed_operations"),
				resultSet.getLong("receipt_documents"),
				resultSet.getObject("first_failed_line", Long.class)
		);
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
