package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttempt;
import com.neighbor.eventmosaic.ingestion.api.SourcePollStatus;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import java.time.Clock;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

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

	@Autowired
	private JdbcSourcePollRepository sourcePollRepository;

	@Autowired
	private BackendDataProperties backendDataProperties;

	@Autowired
	private Clock clock;

	@Autowired
	private PlatformTransactionManager transactionManager;

	@BeforeEach
	void cleanLedger() {
		jdbcClient.sql("truncate table ingestion_source_poll_state").update();
	}

	@Test
	@DisplayName("Длительный отказ проходит две серии повторов и восстанавливается после перезапуска без потери истории")
	void repeatedOutageSequencesKeepEvidenceAcrossRestart() {
		sourcePollLedger.register(SOURCE_NAME);
		Instant now = FixedClockTestConfiguration.NOW;
		var failure = new IngestionFailure(IngestionErrorCode.MANIFEST_TIMEOUT, true);
		var transaction = new TransactionTemplate(transactionManager);
		for (int sequence = 1; sequence <= 2; sequence++) {
			for (int retry = 0; retry <= 3; retry++) {
				SourcePollLedger runtime = at(now);
				var attempt = transaction.execute(_ -> runtime.claim(SOURCE_NAME, LEASE).orElseThrow());
				assertThat(attempt).isNotNull();
				assertThat(attempt.attemptCount()).isEqualTo((sequence - 1) * 4 + retry + 1);
				assertThat(runtime.findBySourceName(SOURCE_NAME).orElseThrow().attempt().retry().automaticRetriesUsed())
						.isEqualTo(retry);
				transaction.executeWithoutResult(_ -> runtime.markFailed(SOURCE_NAME, attempt.token(), failure));
				var retryState = runtime.findBySourceName(SOURCE_NAME).orElseThrow().attempt().retry();
				Duration expectedDelay = Duration.ofMinutes(retry == 3 ? 15 : 1L << retry);
				assertThat(retryState.retryNotBefore()).isEqualTo(now.plus(expectedDelay));
				if (retry == 3) {
					assertThat(retryState.lastExhaustedAt()).isEqualTo(now);
					assertThat(retryState.lastExhaustedErrorCode()).isEqualTo("MANIFEST_TIMEOUT");
				}
				SourcePollLedger restartedBeforeBoundary = at(retryState.retryNotBefore().minusNanos(1000));
				var earlyClaim = transaction.execute(_ -> restartedBeforeBoundary.claim(SOURCE_NAME, LEASE));
				assertThat(earlyClaim).isEmpty();
				now = retryState.retryNotBefore();
			}
		}
		SourcePollLedger restarted = at(now);
		var exhaustedEvidence = restarted.findBySourceName(SOURCE_NAME).orElseThrow().attempt().retry();
		var recovered = transaction.execute(_ -> restarted.claim(SOURCE_NAME, LEASE).orElseThrow());
		assertThat(recovered).isNotNull();
		assertThat(recovered.attemptCount()).isEqualTo(9);
		transaction.executeWithoutResult(_ -> restarted.markSucceeded(SOURCE_NAME, recovered.token()));
		var healthy = restarted.findBySourceName(SOURCE_NAME).orElseThrow();
		assertThat(healthy.status()).isEqualTo(SourcePollStatus.IDLE);
		assertThat(healthy.attempt().retry().retrySequence()).isEqualTo(3);
		assertThat(healthy.attempt().retry().automaticRetriesUsed()).isZero();
		assertThat(healthy.attempt().retry().lastExhaustedAt()).isEqualTo(exhaustedEvidence.lastExhaustedAt());
		assertThat(healthy.attempt().retry().lastExhaustedErrorCode()).isEqualTo("MANIFEST_TIMEOUT");
	}

	@Test
	@DisplayName("Остановка последнего повтора восстанавливается после паузы от конца аренды и отклоняет старого владельца")
	void expiredExhaustedSourceLeaseRearmsAfterMaximumCooldown() {
		sourcePollLedger.register(SOURCE_NAME);
		var abandoned = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		jdbcClient.sql("""
				update ingestion_source_poll_state
				set total_attempt_count = 4, automatic_retries_used = 3
				where source_name = :sourceName
				""").param("sourceName", SOURCE_NAME).update();
		var transaction = new TransactionTemplate(transactionManager);
		Instant boundary = abandoned.leaseExpiresAt().plus(Duration.ofMinutes(15));
		var early = at(boundary.minusNanos(1000));
		var earlyClaim = transaction.execute(_ -> early.claim(SOURCE_NAME, LEASE));
		assertThat(earlyClaim).isEmpty();
		var restarted = at(boundary);
		var recovered = transaction.execute(_ -> restarted.claim(SOURCE_NAME, LEASE).orElseThrow());
		assertThat(recovered).isNotNull();
		assertThat(recovered.recovered()).isTrue();
		assertThat(recovered.attemptCount()).isEqualTo(5);
		var state = restarted.findBySourceName(SOURCE_NAME).orElseThrow();
		assertThat(state.attempt().retry().retrySequence()).isEqualTo(2);
		assertThat(state.attempt().retry().lastExhaustedAt()).isEqualTo(abandoned.leaseExpiresAt());
		assertThat(state.attempt().retry().lastExhaustedErrorCode()).isEqualTo("ATTEMPT_LEASE_EXPIRED");
		var staleResult = transaction.execute(_ -> restarted.markSucceeded(SOURCE_NAME, abandoned.token()));
		assertThat(staleResult).isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		transaction.executeWithoutResult(_ -> restarted.markSucceeded(SOURCE_NAME, recovered.token()));
	}

	private SourcePollLedger at(Instant now) {
		return new JdbcSourcePollLedger(sourcePollRepository, backendDataProperties, Clock.fixed(now, ZoneOffset.UTC));
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
							.isEqualTo(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(1)));
					assertThat(state.attempt().retry().consecutiveRetryableFailures()).isEqualTo(1);
				});

		assertThat(sourcePollLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		jdbcClient.sql("""
				update ingestion_source_poll_state
				set retry_not_before = :retryNotBefore
				where source_name = :sourceName
				""")
				.param("retryNotBefore", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("sourceName", SOURCE_NAME)
				.update();

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
	@DisplayName("Будущая задержка запрещает запрос, а наступившая пауза открывает новую серию")
	void futureRetryDefersAndElapsedCooldownStartsNewSequence() {
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

		assertThat(sourcePollLedger.claim(SOURCE_NAME, LEASE)).isPresent();
		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.attempt().count()).isEqualTo(5);
					assertThat(state.attempt().retry().automaticRetriesUsed()).isZero();
					assertThat(state.attempt().retry().retrySequence()).isEqualTo(2);
					assertThat(state.attempt().retry().lastExhaustedErrorCode()).isEqualTo("MANIFEST_TIMEOUT");
				});
	}

	@Test
	@DisplayName("Retry schedule сохраняется после повторного создания ledger facade")
	void retryScheduleSurvivesFacadeRecreation() {
		sourcePollLedger.register(SOURCE_NAME);
		var attempt = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		sourcePollLedger.markFailed(
				SOURCE_NAME,
				attempt.token(),
				new IngestionFailure(IngestionErrorCode.MANIFEST_TIMEOUT, true));

		SourcePollLedger recreated = new JdbcSourcePollLedger(
				sourcePollRepository,
				backendDataProperties,
				clock);

		assertThat(recreated.findBySourceName(SOURCE_NAME).orElseThrow()
				.attempt().retry().retryNotBefore())
				.isEqualTo(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(1)));
		assertThat(recreated.claim(SOURCE_NAME, LEASE)).isEmpty();
	}

	@Test
	@DisplayName("Retry-After увеличивает durable delay только до configured maximum")
	void retryAfterIsPersistedWithinConfiguredMaximum() {
		sourcePollLedger.register(SOURCE_NAME);
		var attempt = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		sourcePollLedger.markFailed(
				SOURCE_NAME,
				attempt.token(),
				new IngestionFailure(IngestionErrorCode.MANIFEST_HTTP_ERROR, true),
				Duration.ofHours(1));

		assertThat(sourcePollLedger.findBySourceName(SOURCE_NAME).orElseThrow()
				.attempt().retry().retryNotBefore())
				.isEqualTo(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(15)));
	}

	@Test
	@DisplayName("Параллельный claim на due boundary выдает только один poll token")
	void concurrentDueClaimIssuesSingleToken() throws Exception {
		sourcePollLedger.register(SOURCE_NAME);
		var initial = sourcePollLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		sourcePollLedger.markFailed(
				SOURCE_NAME,
				initial.token(),
				new IngestionFailure(IngestionErrorCode.MANIFEST_TIMEOUT, true));
		jdbcClient.sql("""
				update ingestion_source_poll_state
				set retry_not_before = :retryNotBefore
				where source_name = :sourceName
				""")
				.param("retryNotBefore", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("sourceName", SOURCE_NAME)
				.update();

		var executor = Executors.newFixedThreadPool(2);
		try {
			var results = executor.invokeAll(List.<Callable<Optional<SourcePollAttempt>>>of(
					() -> sourcePollLedger.claim(SOURCE_NAME, LEASE),
					() -> sourcePollLedger.claim(SOURCE_NAME, LEASE)));
			long claimed = 0;
			for (var result : results) {
				if (result.get(10, TimeUnit.SECONDS).isPresent()) {
					claimed++;
				}
			}
			assertThat(claimed).isEqualTo(1);
		}
		finally {
			executor.shutdownNow();
		}
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
