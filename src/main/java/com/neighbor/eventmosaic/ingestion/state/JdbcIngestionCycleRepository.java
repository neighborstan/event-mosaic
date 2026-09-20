package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleState;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleStatus;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/** Хранит состояние цикла в PostgreSQL и отклоняет завершение чужим или просроченным владельцем. */
@Repository
class JdbcIngestionCycleRepository {

	private static final RowMapper<IngestionCycleState> STATE =
			JdbcIngestionCycleRepository::mapState;
	private static final String SELECT_BY_SOURCE = """
			select
			    source_name,
			    status,
			    owner_token,
			    fencing_epoch,
			    lease_expires_at,
			    last_started_at,
			    last_progress_at,
			    last_terminal_at,
			    last_outcome,
			    state_version,
			    created_at,
			    updated_at
			from ingestion_cycle_state
			where source_name = :sourceName
			""";

	private final JdbcClient jdbcClient;

	JdbcIngestionCycleRepository(JdbcClient jdbcClient) {
		this.jdbcClient = jdbcClient;
	}

	Optional<IngestionCycleOwnership> claim(String sourceName, Duration leaseDuration) {
		register(sourceName);
		IngestionCycleState state = findForUpdate(sourceName).orElseThrow(
				() -> new IllegalStateException("Registered ingestion cycle state disappeared"));
		Instant databaseNow = databaseNow();
		if (state.status() == IngestionCycleStatus.ACTIVE
				&& state.ownership().leaseExpiresAt().isAfter(databaseNow)) {
			return Optional.empty();
		}

		long nextEpoch = Math.addExact(state.fencingEpoch(), 1);
		UUID token = UUID.randomUUID();
		Instant leaseExpiresAt = databaseNow.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update ingestion_cycle_state
				set status = :status,
				    owner_token = :ownerToken,
				    fencing_epoch = :fencingEpoch,
				    lease_expires_at = :leaseExpiresAt,
				    last_started_at = :lastStartedAt,
				    last_progress_at = null,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where source_name = :sourceName
				""")
				.param("status", IngestionCycleStatus.ACTIVE.name())
				.param("ownerToken", token)
				.param("fencingEpoch", nextEpoch)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("lastStartedAt", Timestamp.from(databaseNow))
				.param("updatedAt", Timestamp.from(databaseNow))
				.param("sourceName", sourceName)
				.update();
		if (updated != 1) {
			throw new IllegalStateException(
					"Claim changed an unexpected number of ingestion cycle rows: " + updated);
		}
		return Optional.of(new IngestionCycleOwnership(
				sourceName,
				token,
				nextEpoch,
				leaseExpiresAt));
	}

	Optional<Duration> remainingLease(IngestionCycleOwnership ownership) {
		return jdbcClient.sql("""
				with database_time as (
				    select clock_timestamp() as current_time
				)
				select extract(epoch from (
				    cycle.lease_expires_at - database_time.current_time
				)) as remaining_seconds
				from ingestion_cycle_state cycle
				cross join database_time
				where cycle.source_name = :sourceName
				  and cycle.status = :status
				  and cycle.owner_token = :ownerToken
				  and cycle.fencing_epoch = :fencingEpoch
				  and cycle.lease_expires_at > database_time.current_time
				""")
				.param("sourceName", ownership.sourceName())
				.param("status", IngestionCycleStatus.ACTIVE.name())
				.param("ownerToken", ownership.token())
				.param("fencingEpoch", ownership.fencingEpoch())
				.query((resultSet, rowNumber) -> duration(
						resultSet.getBigDecimal("remaining_seconds")))
				.optional();
	}

	AttemptTransitionResult complete(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome
	) {
		int updated = jdbcClient.sql("""
				with locked_cycle as materialized (
				    select source_name
				    from ingestion_cycle_state
				    where source_name = :sourceName
				      and status = :activeStatus
				      and owner_token = :ownerToken
				      and fencing_epoch = :fencingEpoch
				    for update
				),
				database_time as materialized (
				    select clock_timestamp() as current_time
				    from locked_cycle
				)
				update ingestion_cycle_state cycle
				set status = :idleStatus,
				    owner_token = null,
				    lease_expires_at = null,
				    last_terminal_at = database_time.current_time,
				    last_outcome = :outcome,
				    state_version = cycle.state_version + 1,
				    updated_at = database_time.current_time
				from locked_cycle, database_time
				where cycle.source_name = locked_cycle.source_name
				  and cycle.lease_expires_at > database_time.current_time
				""")
				.param("idleStatus", IngestionCycleStatus.IDLE.name())
				.param("outcome", outcome.name())
				.param("sourceName", ownership.sourceName())
				.param("activeStatus", IngestionCycleStatus.ACTIVE.name())
				.param("ownerToken", ownership.token())
				.param("fencingEpoch", ownership.fencingEpoch())
				.update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	Optional<IngestionCycleState> findBySourceName(String sourceName) {
		return jdbcClient.sql(SELECT_BY_SOURCE)
				.param("sourceName", sourceName)
				.query(STATE)
				.optional();
	}

	private void register(String sourceName) {
		jdbcClient.sql("""
				insert into ingestion_cycle_state (
				    source_name,
				    status,
				    created_at,
				    updated_at
				)
				values (
				    :sourceName,
				    :status,
				    clock_timestamp(),
				    clock_timestamp()
				)
				on conflict (source_name) do nothing
				""")
				.param("sourceName", sourceName)
				.param("status", IngestionCycleStatus.IDLE.name())
				.update();
	}

	private Optional<IngestionCycleState> findForUpdate(String sourceName) {
		return jdbcClient.sql(SELECT_BY_SOURCE + "for update")
				.param("sourceName", sourceName)
				.query(STATE)
				.optional();
	}

	private Instant databaseNow() {
		return jdbcClient.sql("select clock_timestamp()")
				.query((resultSet, rowNumber) -> resultSet.getTimestamp(1).toInstant())
				.single();
	}

	private static IngestionCycleState mapState(ResultSet resultSet, int rowNumber)
			throws SQLException {
		String sourceName = resultSet.getString("source_name");
		IngestionCycleStatus status = IngestionCycleStatus.valueOf(
				resultSet.getString("status"));
		long fencingEpoch = resultSet.getLong("fencing_epoch");
		Instant leaseExpiresAt = nullableInstant(resultSet, "lease_expires_at");
		UUID ownerToken = resultSet.getObject("owner_token", UUID.class);
		IngestionCycleOwnership ownership = status == IngestionCycleStatus.ACTIVE
				? new IngestionCycleOwnership(
						sourceName,
						ownerToken,
						fencingEpoch,
						leaseExpiresAt)
				: null;
		String outcome = resultSet.getString("last_outcome");
		return new IngestionCycleState(
				sourceName,
				status,
				fencingEpoch,
				ownership,
				nullableInstant(resultSet, "last_started_at"),
				nullableInstant(resultSet, "last_progress_at"),
				nullableInstant(resultSet, "last_terminal_at"),
				outcome == null ? null : IngestionCycleOutcome.valueOf(outcome),
				resultSet.getLong("state_version"),
				instant(resultSet, "created_at"),
				instant(resultSet, "updated_at"));
	}

	private static Instant instant(ResultSet resultSet, String column) throws SQLException {
		return resultSet.getTimestamp(column).toInstant();
	}

	private static Instant nullableInstant(ResultSet resultSet, String column)
			throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}

	private static Duration duration(BigDecimal seconds) {
		BigDecimal[] parts = seconds.divideAndRemainder(BigDecimal.ONE);
		long wholeSeconds = parts[0].longValueExact();
		int nanos = parts[1]
				.movePointRight(9)
				.intValueExact();
		return Duration.ofSeconds(wholeSeconds, nanos);
	}
}
