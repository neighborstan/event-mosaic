package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.RecordedIngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttempt;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttemptState;
import com.neighbor.eventmosaic.ingestion.api.SourcePollState;
import com.neighbor.eventmosaic.ingestion.api.SourcePollStatus;
import com.neighbor.eventmosaic.ingestion.retry.RetryDelayPolicy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

/**
 * Выполняет SQL registration, due claim и conditional transitions current
 * source-poll state.
 */
@Repository
class JdbcSourcePollRepository {

	private static final RowMapper<SourcePollState> STATE =
			JdbcSourcePollRepository::mapState;
	private static final String SELECT_BY_SOURCE = """
			select
			    source_name,
			    status,
			    attempt_token,
			    lease_expires_at,
			    total_attempt_count,
			    automatic_retries_used,
			    consecutive_retryable_failures,
			    automatic_retry_limit,
			    retry_not_before,
			    retry_sequence,
			    last_exhausted_at,
			    last_exhausted_error_code,
			    last_attempt_at,
			    last_succeeded_at,
			    failed_at,
			    last_error_code,
			    last_error_retryable,
			    created_at
			from ingestion_source_poll_state
			where source_name = :sourceName
			""";

	private final JdbcClient jdbcClient;
	private final RetryDelayPolicy retryDelayPolicy;

	JdbcSourcePollRepository(JdbcClient jdbcClient, RetryDelayPolicy retryDelayPolicy) {
		this.jdbcClient = jdbcClient;
		this.retryDelayPolicy = retryDelayPolicy;
	}

	SourcePollState register(String sourceName, int automaticRetryLimit, Instant now) {
		int inserted = jdbcClient.sql("""
				insert into ingestion_source_poll_state (
				    source_name,
				    status,
				    automatic_retry_limit,
				    created_at,
				    updated_at
				)
				values (
				    :sourceName,
				    :status,
				    :automaticRetryLimit,
				    :createdAt,
				    :updatedAt
				)
				on conflict (source_name) do nothing
				""")
				.param("sourceName", sourceName)
				.param("status", SourcePollStatus.IDLE.name())
				.param("automaticRetryLimit", automaticRetryLimit)
				.param("createdAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.update();
		SourcePollState state = findBySourceName(sourceName)
				.orElseThrow(() -> new IllegalStateException("Registered source-poll state disappeared"));
		if (inserted == 0 && state.attempt().retry().automaticRetryLimit() != automaticRetryLimit) {
			throw new IllegalStateException("Configured source-poll retry limit conflicts with durable state");
		}
		return state;
	}

	Optional<SourcePollAttempt> claim(
			String sourceName,
			Duration leaseDuration,
			Instant now
	) {
		SourcePollState state = findForUpdate(sourceName).orElse(null);
		if (state == null || !isClaimable(state, now)) {
			return Optional.empty();
		}

		boolean recovered = state.status() == SourcePollStatus.POLLING;
		boolean automaticRetry = state.status() != SourcePollStatus.IDLE;
		boolean newSequence = automaticRetry && state.attempt().retry().exhausted();
		UUID token = UUID.randomUUID();
		Instant leaseExpiresAt = now.plus(leaseDuration);
		int updated = jdbcClient.sql("""
				update ingestion_source_poll_state
				set status = :status,
				    attempt_token = :attemptToken,
				    lease_expires_at = :leaseExpiresAt,
				    total_attempt_count = total_attempt_count + 1,
				    automatic_retries_used = case when :newSequence then 0
				        else automatic_retries_used + :automaticRetryIncrement end,
				    consecutive_retryable_failures = case when :newSequence then 0
				        else consecutive_retryable_failures end,
				    retry_sequence = retry_sequence + case when :newSequence then 1 else 0 end,
				    last_exhausted_at = case when :expiredExhausted then lease_expires_at
				        when :newSequence then coalesce(last_exhausted_at, failed_at)
				        else last_exhausted_at end,
				    last_exhausted_error_code = case when :expiredExhausted then 'ATTEMPT_LEASE_EXPIRED'
				        when :newSequence then coalesce(last_exhausted_error_code, last_error_code)
				        else last_exhausted_error_code end,
				    retry_not_before = null,
				    last_attempt_at = :lastAttemptAt,
				    failed_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where source_name = :sourceName
				""")
				.param("status", SourcePollStatus.POLLING.name())
				.param("attemptToken", token)
				.param("leaseExpiresAt", Timestamp.from(leaseExpiresAt))
				.param("automaticRetryIncrement", automaticRetry ? 1 : 0)
				.param("newSequence", newSequence)
				.param("expiredExhausted", recovered && newSequence)
				.param("lastAttemptAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("sourceName", sourceName)
				.update();
		if (updated != 1) {
			throw new IllegalStateException("Claim changed an unexpected number of source-poll rows: " + updated);
		}

		return Optional.of(new SourcePollAttempt(
				sourceName,
				token,
				leaseExpiresAt,
				state.attempt().count() + 1,
				recovered));
	}

	AttemptTransitionResult markSucceeded(String sourceName, UUID attemptToken, Instant now) {
		int updated = jdbcClient.sql("""
				update ingestion_source_poll_state
				set status = :status,
				    attempt_token = null,
				    lease_expires_at = null,
				    automatic_retries_used = 0,
				    consecutive_retryable_failures = 0,
				    retry_not_before = null,
				    last_succeeded_at = :lastSucceededAt,
				    failed_at = null,
				    last_error_code = null,
				    last_error_retryable = null,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where source_name = :sourceName
				  and status = :pollingStatus
				  and attempt_token = :attemptToken
				""")
				.param("status", SourcePollStatus.IDLE.name())
				.param("lastSucceededAt", Timestamp.from(now))
				.param("updatedAt", Timestamp.from(now))
				.param("sourceName", sourceName)
				.param("pollingStatus", SourcePollStatus.POLLING.name())
				.param("attemptToken", attemptToken)
				.update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	AttemptTransitionResult markFailed(
			String sourceName,
			UUID attemptToken,
			IngestionFailure failure,
			Duration retryAfter,
			Instant now
	) {
		SourcePollState state = findForUpdate(sourceName).orElse(null);
		if (state == null
				|| state.status() != SourcePollStatus.POLLING
				|| !attemptToken.equals(state.attempt().token())) {
			return AttemptTransitionResult.OWNERSHIP_LOST;
		}
		OffsetDateTime retryNotBefore = failure.retryable()
				? OffsetDateTime.ofInstant(
						retryDelayPolicy.retryNotBefore(
								now,
								state.attempt().retry(),
								retryAfter),
						ZoneOffset.UTC)
				: null;
		int updated = jdbcClient.sql("""
				update ingestion_source_poll_state
				set status = :status,
				    attempt_token = null,
				    lease_expires_at = null,
				    consecutive_retryable_failures = case
				        when :retryable then consecutive_retryable_failures + 1
				        else 0
				    end,
				    retry_not_before = :retryNotBefore,
				    failed_at = :failedAt,
				    last_error_code = :errorCode,
				    last_error_retryable = :retryable,
				    last_exhausted_at = case
				        when :retryable and automatic_retries_used >= automatic_retry_limit
				        then :failedAt else last_exhausted_at end,
				    last_exhausted_error_code = case
				        when :retryable and automatic_retries_used >= automatic_retry_limit
				        then :errorCode else last_exhausted_error_code end,
				    state_version = state_version + 1,
				    updated_at = :updatedAt
				where source_name = :sourceName
				  and status = :pollingStatus
				  and attempt_token = :attemptToken
				""")
				.param("status", SourcePollStatus.FAILED.name())
				.param("retryable", failure.retryable())
				.param("retryNotBefore", retryNotBefore, Types.TIMESTAMP_WITH_TIMEZONE)
				.param("failedAt", Timestamp.from(now))
				.param("errorCode", failure.code().code())
				.param("updatedAt", Timestamp.from(now))
				.param("sourceName", sourceName)
				.param("pollingStatus", SourcePollStatus.POLLING.name())
				.param("attemptToken", attemptToken)
				.update();
		return AttemptTransitionResult.fromUpdatedRows(updated);
	}

	Optional<SourcePollState> findBySourceName(String sourceName) {
		return jdbcClient.sql(SELECT_BY_SOURCE)
				.param("sourceName", sourceName)
				.query(STATE)
				.optional();
	}

	private Optional<SourcePollState> findForUpdate(String sourceName) {
		return jdbcClient.sql(SELECT_BY_SOURCE + "for update")
				.param("sourceName", sourceName)
				.query(STATE)
				.optional();
	}

	private boolean isClaimable(SourcePollState state, Instant now) {
		return switch (state.status()) {
			case IDLE -> true;
			case FAILED -> state.failure().failure().retryable()
					&& !state.attempt().retry().retryNotBefore().isAfter(now);
			case POLLING -> !state.attempt().leaseExpiresAt().isAfter(now)
					&& (!state.attempt().retry().exhausted()
						|| !retryDelayPolicy.cooldownNotBefore(state.attempt().leaseExpiresAt()).isAfter(now));
		};
	}

	private static SourcePollState mapState(ResultSet resultSet, int rowNumber) throws SQLException {
		SourcePollAttemptState attempt = new SourcePollAttemptState(
				resultSet.getInt("total_attempt_count"),
				resultSet.getObject("attempt_token", UUID.class),
				nullableInstant(resultSet, "last_attempt_at"),
				nullableInstant(resultSet, "lease_expires_at"),
				new AutomaticRetryState(
						resultSet.getInt("automatic_retries_used"),
						resultSet.getInt("consecutive_retryable_failures"),
						resultSet.getInt("automatic_retry_limit"),
						nullableInstant(resultSet, "retry_not_before"),
								resultSet.getLong("retry_sequence"),
								nullableInstant(resultSet, "last_exhausted_at"),
								resultSet.getString("last_exhausted_error_code")));
		return new SourcePollState(
				resultSet.getString("source_name"),
				SourcePollStatus.valueOf(resultSet.getString("status")),
				attempt,
				mapFailure(resultSet),
				instant(resultSet, "created_at"),
				nullableInstant(resultSet, "last_succeeded_at"));
	}

	private static RecordedIngestionFailure mapFailure(ResultSet resultSet) throws SQLException {
		String errorCode = resultSet.getString("last_error_code");
		if (errorCode == null) {
			return null;
		}
		Boolean retryable = resultSet.getObject("last_error_retryable", Boolean.class);
		if (retryable == null) {
			throw new SQLException("Required source-poll retryable flag is null");
		}
		return new RecordedIngestionFailure(
				new IngestionFailure(IngestionErrorCode.valueOf(errorCode), retryable),
				instant(resultSet, "failed_at"));
	}

	private static Instant instant(ResultSet resultSet, String column) throws SQLException {
		return resultSet.getTimestamp(column).toInstant();
	}

	private static Instant nullableInstant(ResultSet resultSet, String column) throws SQLException {
		Timestamp value = resultSet.getTimestamp(column);
		return value == null ? null : value.toInstant();
	}
}
