package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttemptState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.RecordedIngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import java.net.URI;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;

/**
 * Централизует только JDBC mapping ingestion rows, не SQL business operations.
 */
final class IngestionJdbcMappers {

	static final RowMapper<IngestionArchiveState> ARCHIVE = IngestionJdbcMappers::mapArchive;
	static final RowMapper<IngestionRunRow> RUN = IngestionJdbcMappers::mapRun;

	private IngestionJdbcMappers() {
	}

	static IngestionArchiveState mapArchive(ResultSet resultSet, int rowNumber) throws SQLException {
		DiscoveredArchive archive = new DiscoveredArchive(
				instant(resultSet, "source_update_time"),
				resultSet.getString("archive_name"),
				URI.create(resultSet.getString("metadata_url")),
				resultSet.getString("expected_md5"),
				ArchiveType.valueOf(resultSet.getString("archive_type")),
				resultSet.getLong("file_size_bytes")
		);
		return new IngestionArchiveState(
				resultSet.getLong("run_id"),
				archive,
				IngestionArchiveStatus.valueOf(resultSet.getString("status")),
				new ArchiveAttemptState(
						resultSet.getInt("total_attempt_count"),
						resultSet.getObject("attempt_token", UUID.class),
						nullableInstant(resultSet, "last_attempt_at"),
						nullableInstant(resultSet, "lease_expires_at"),
						new AutomaticRetryState(
								resultSet.getInt("automatic_retries_used"),
								resultSet.getInt("consecutive_retryable_failures"),
								resultSet.getInt("automatic_retry_limit"),
								nullableInstant(resultSet, "retry_not_before"))
				),
				mapStagedArchive(resultSet),
				mapRecordedFailure(resultSet, "failed_at"),
				instant(resultSet, "first_seen_at"),
				nullableInstant(resultSet, "completed_at")
		);
	}

	static IngestionRunRow mapRun(ResultSet resultSet, int rowNumber) throws SQLException {
		return new IngestionRunRow(
				resultSet.getLong("id"),
				instant(resultSet, "source_update_time"),
				IngestionRunStatus.valueOf(resultSet.getString("status")),
				instant(resultSet, "first_seen_at"),
				nullableInstant(resultSet, "completed_at"),
				mapRecordedFailure(resultSet, "last_failed_at")
		);
	}

	static RecordedIngestionFailure mapRecordedFailure(
			ResultSet resultSet,
			String occurredAtColumn
	) throws SQLException {
		String code = resultSet.getString("last_error_code");
		if (code == null) {
			return null;
		}
		return new RecordedIngestionFailure(
				new IngestionFailure(
						IngestionErrorCode.valueOf(code),
						requiredBoolean(resultSet, "last_error_retryable")
				),
				instant(resultSet, occurredAtColumn)
		);
	}

	static Instant instant(ResultSet resultSet, String column) throws SQLException {
		return resultSet.getTimestamp(column).toInstant();
	}

	static Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}

	static Instant nullableInstant(ResultSet resultSet, int column) throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}

	private static boolean requiredBoolean(ResultSet resultSet, String column) throws SQLException {
		Boolean value = resultSet.getObject(column, Boolean.class);
		if (value == null) {
			throw new SQLException("Required boolean column is null: " + column);
		}
		return value;
	}

	private static StagedArchive mapStagedArchive(ResultSet resultSet) throws SQLException {
		String archivePath = resultSet.getString("staged_archive_path");
		if (archivePath == null) {
			return null;
		}
		return new StagedArchive(
				Path.of(archivePath),
				Path.of(resultSet.getString("staged_csv_path")),
				resultSet.getLong("actual_size_bytes"),
				resultSet.getString("actual_md5")
		);
	}
}
