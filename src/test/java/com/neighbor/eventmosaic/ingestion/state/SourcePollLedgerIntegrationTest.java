package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.api.SourcePollStatus;
import java.sql.Timestamp;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Интеграция source-poll ledger с PostgreSQL")
class SourcePollLedgerIntegrationTest {

	private static final String SOURCE_NAME = GdeltSourceContract.SOURCE_NAME;
	private static final Duration LEASE = Duration.ofMinutes(10);

	@Autowired
	private SourcePollLedger sourcePollLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanLedger() {
		jdbcClient.sql("truncate table ingestion_source_poll_state").update();
	}

	@Test
	@DisplayName("Retryable poll получает due retry, а успех сбрасывает retry sequence")
	void retryablePollRetriesAndSuccessResetsSequence() {
		var registered = sourcePollLedger.register(SOURCE_NAME);
		assertThat(registered.status()).isEqualTo(SourcePollStatus.IDLE);
		assertThat(registered.attempt().retry().automaticRetryLimit()).isEqualTo(3);

		var first = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		assertThat(sourcePollLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		var failure = new IngestionFailure(IngestionErrorCode.MANIFEST_HTTP_ERROR, true);

		assertThat(sourcePollLedger.markFailed(SOURCE_NAME, first.token(), failure))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(SourcePollStatus.FAILED);
					assertThat(state.failure().failure()).isEqualTo(failure);
					assertThat(state.attempt().retry().retryNotBefore())
							.isEqualTo(FixedClockTestConfiguration.NOW);
					assertThat(state.attempt().retry().consecutiveRetryableFailures()).isEqualTo(1);
				});

		var retry = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		assertThat(retry.attemptCount()).isEqualTo(2);
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow()
				.attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(sourcePollLedger.markSucceeded(SOURCE_NAME, first.token()))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(sourcePollLedger.markSucceeded(SOURCE_NAME, retry.token()))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(SourcePollStatus.IDLE);
					assertThat(state.attempt().count()).isEqualTo(2);
					assertThat(state.attempt().retry().automaticRetriesUsed()).isZero();
					assertThat(state.attempt().retry().consecutiveRetryableFailures()).isZero();
					assertThat(state.lastSucceededAt()).isEqualTo(FixedClockTestConfiguration.NOW);
				});
	}

	@Test
	@DisplayName("Future retry и исчерпанный budget не создают poll token")
	void futureRetryAndExhaustedBudgetDoNotCreateToken() {
		sourcePollLedger.register(SOURCE_NAME);
		var attempt = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		sourcePollLedger.markFailed(
				SOURCE_NAME,
				attempt.token(),
				new IngestionFailure(IngestionErrorCode.MANIFEST_TIMEOUT, true));

		jdbcClient.sql("""
				update ingestion_source_poll_state
				set retry_not_before = :retryNotBefore
				where source_name = :sourceName
				""")
				.param(
						"retryNotBefore",
						Timestamp.from(FixedClockTestConfiguration.NOW.plusSeconds(60)))
				.param("sourceName", SOURCE_NAME)
				.update();

		assertThat(sourcePollLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow()
				.attempt().count()).isEqualTo(1);

		jdbcClient.sql("""
				update ingestion_source_poll_state
				set retry_not_before = :retryNotBefore,
				    automatic_retries_used = automatic_retry_limit,
				    total_attempt_count = automatic_retry_limit + 1
				where source_name = :sourceName
				""")
				.param("retryNotBefore", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("sourceName", SOURCE_NAME)
				.update();

		assertThat(sourcePollLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.attempt().count()).isEqualTo(4);
					assertThat(state.attempt().token()).isNull();
					assertThat(state.attempt().retry().exhausted()).isTrue();
				});
	}

	@Test
	@DisplayName("Просроченный poll lease восстанавливается в пределах automatic budget")
	void expiredLeaseIsRecoveredWithinAutomaticBudget() {
		sourcePollLedger.register(SOURCE_NAME);
		var first = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		jdbcClient.sql("""
				update ingestion_source_poll_state
				set last_attempt_at = :lastAttemptAt,
				    lease_expires_at = :leaseExpiresAt
				where source_name = :sourceName
				""")
				.param(
						"lastAttemptAt",
						Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(2)))
				.param(
						"leaseExpiresAt",
						Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(1)))
				.param("sourceName", SOURCE_NAME)
				.update();

		var recovered = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(recovered.recovered()).isTrue();
		assertThat(recovered.attemptCount()).isEqualTo(2);
		assertThat(recovered.token()).isNotEqualTo(first.token());
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow()
				.attempt().retry().automaticRetriesUsed()).isEqualTo(1);
	}
}
