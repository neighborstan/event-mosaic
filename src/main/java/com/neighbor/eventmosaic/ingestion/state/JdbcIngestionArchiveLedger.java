package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcIngestionArchiveLedger implements IngestionArchiveLedger {

	private static final RowMapper<IngestionArchiveState> ROW_MAPPER = JdbcIngestionArchiveLedger::mapRow;

	private final JdbcClient jdbcClient;
	private final Clock clock;

	@Autowired
	public JdbcIngestionArchiveLedger(JdbcClient jdbcClient) {
		this(jdbcClient, Clock.systemUTC());
	}

	JdbcIngestionArchiveLedger(JdbcClient jdbcClient, Clock clock) {
		this.jdbcClient = jdbcClient;
		this.clock = clock;
	}

	@Override
	public IngestionArchiveState registerDiscovered(DiscoveredArchive archive) {
		Instant now = clock.instant();

		return jdbcClient.sql("""
				insert into ingestion_archives (
				    idempotency_key,
				    archive_name,
				    archive_url,
				    expected_md5,
				    archive_type,
				    status,
				    file_size_bytes,
				    first_seen_at,
				    created_at,
				    updated_at
				)
				values (
				    :idempotencyKey,
				    :archiveName,
				    :archiveUrl,
				    :expectedMd5,
				    :archiveType,
				    :status,
				    :fileSizeBytes,
				    :firstSeenAt,
				    :createdAt,
				    :updatedAt
				)
				on conflict (idempotency_key) do update set
				    archive_url = excluded.archive_url,
				    expected_md5 = excluded.expected_md5,
				    file_size_bytes = excluded.file_size_bytes,
				    updated_at = excluded.updated_at
				returning *
				""")
				.param("idempotencyKey", archive.idempotencyKey())
				.param("archiveName", archive.archiveName())
				.param("archiveUrl", archive.archiveUri().toString())
				.param("expectedMd5", archive.expectedMd5())
				.param("archiveType", archive.archiveType().name())
				.param("status", IngestionArchiveStatus.DISCOVERED.name())
				.param("fileSizeBytes", archive.fileSizeBytes())
				.param("firstSeenAt", Timestamp.from(now))
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.query(ROW_MAPPER)
				.single();
	}

	@Override
	public Optional<IngestionArchiveState> findByIdempotencyKey(String idempotencyKey) {
		return jdbcClient.sql("select * from ingestion_archives where idempotency_key = :idempotencyKey")
				.param("idempotencyKey", idempotencyKey)
				.query(ROW_MAPPER)
				.optional();
	}

	private static IngestionArchiveState mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
		return new IngestionArchiveState(
				resultSet.getString("idempotency_key"),
				resultSet.getString("archive_name"),
				URI.create(resultSet.getString("archive_url")),
				resultSet.getString("expected_md5"),
				resultSet.getString("actual_md5"),
				ArchiveType.valueOf(resultSet.getString("archive_type")),
				IngestionArchiveStatus.valueOf(resultSet.getString("status")),
				getNullableLong(resultSet, "file_size_bytes"),
				resultSet.getLong("raw_record_count"),
				resultSet.getLong("parsed_record_count"),
				resultSet.getLong("processed_record_count"),
				resultSet.getLong("indexed_record_count"),
				resultSet.getLong("failed_record_count"),
				getInstant(resultSet, "first_seen_at"),
				getNullableInstant(resultSet, "last_attempt_at"),
				getNullableInstant(resultSet, "completed_at"),
				resultSet.getInt("attempt_count"),
				resultSet.getString("last_error")
		);
	}

	private static Instant getInstant(ResultSet resultSet, String column) throws SQLException {
		return resultSet.getTimestamp(column).toInstant();
	}

	private static Instant getNullableInstant(ResultSet resultSet, String column) throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}

	private static Long getNullableLong(ResultSet resultSet, String column) throws SQLException {
		long value = resultSet.getLong(column);
		return resultSet.wasNull() ? null : value;
	}
}
