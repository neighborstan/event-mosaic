package com.neighbor.eventmosaic.ingestion.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Интеграция processing ledger с PostgreSQL")
class ArchiveProcessingLedgerIntegrationTest {

	private static final Duration ACQUISITION_LEASE = Duration.ofMinutes(15);
	private static final String RECEIPT_MISMATCH = "INDEX_RECEIPT_MISMATCH";
	private static final String RECEIPT_SURPLUS = "INDEX_RECEIPT_SURPLUS";
	private static final String BULK_PARTIAL_FAILURE = "BULK_PARTIAL_FAILURE";
	private static final String MAPPING_FAILURE = "MAPPING_FAILURE";

	@Autowired
	private ArchiveProcessingLedger processingLedger;

	@Autowired
	private IngestionArchiveLedger archiveLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@BeforeEach
	void cleanLedger() {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("Регистрирует processing state только для STAGED archive и сверяет fingerprint")
	void registersOnlyStagedArchiveAndVerifiesFingerprint() {
		DiscoveredUpdate discovered = registerUpdate();
		DiscoveredArchive events = archive(discovered, ArchiveType.TRANSLATION_EVENTS);
		String eventArchiveKey = events.idempotencyKey();
		ArchiveProcessingFingerprint fingerprint = fingerprint("a");

		assertThatThrownBy(() -> processingLedger.register(eventArchiveKey, fingerprint))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("STAGED");
		assertThat(processingLedger.findByArchiveIdempotencyKey(eventArchiveKey))
				.isEmpty();

		stage(events);
		ArchiveProcessingState registered = processingLedger.register(
				eventArchiveKey,
				fingerprint);
		ArchiveProcessingState repeated = processingLedger.register(
				eventArchiveKey,
				fingerprint);

		assertThat(registered.status()).isEqualTo(ArchiveProcessingStatus.PENDING);
		assertThat(registered.attempt().count()).isZero();
		assertThat(registered.progress()).isEqualTo(ArchiveProcessingProgress.empty());
		assertThat(repeated).isEqualTo(registered);
		ArchiveProcessingFingerprint conflictingFingerprint = fingerprint("b");
		assertThatThrownBy(() ->
				processingLedger.register(eventArchiveKey, conflictingFingerprint))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("fingerprint");
	}

	@Test
	@DisplayName("Параллельный claim выдает processing ownership только одному worker")
	void concurrentClaimIssuesSingleOwnership() throws Exception {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));

		List<Optional<ArchiveProcessingAttempt>> claims = runConcurrently(
				() -> processingLedger.claim(events.idempotencyKey(), Duration.ofMinutes(10)),
				() -> processingLedger.claim(events.idempotencyKey(), Duration.ofMinutes(10))
		);

		assertThat(claims).filteredOn(Optional::isPresent).hasSize(1);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.PROCESSING);
					assertThat(state.attempt().count()).isEqualTo(1);
					assertThat(state.attempt().token()).isNotNull();
				});
	}

	@Test
	@DisplayName("Просроченный lease восстанавливается и stale token не меняет progress")
	void recoversExpiredLeaseAndRejectsStaleToken() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingAttempt first = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		expireLease(events);

		ArchiveProcessingAttempt recovered = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		ArchiveProcessingProgress checkpoint = progress(
				2, 1, 1, 1, 1, 0, 0, null);

		assertThat(recovered.recovered()).isTrue();
		assertThat(recovered.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(processingLedger.checkpoint(
				events.idempotencyKey(),
				first.token(),
				checkpoint,
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.markFailed(
				events.idempotencyKey(),
				first.token(),
				new ArchiveProcessingFailure(MAPPING_FAILURE, false),
				ArchiveProcessingProgress.empty()))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThat(processingLedger.checkpoint(
				events.idempotencyKey(),
				recovered.token(),
				checkpoint,
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.checkpoint(
				events.idempotencyKey(),
				recovered.token(),
				ArchiveProcessingProgress.empty(),
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);

		ArchiveProcessingState state = processingLedger.findByArchiveIdempotencyKey(
				events.idempotencyKey()).orElseThrow();
		assertThat(state.progress()).isEqualTo(checkpoint);
		assertThat(state.attempt().leaseExpiresAt())
				.isEqualTo(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(20)));
	}

	@Test
	@DisplayName("Retryable failure сохраняет partial counters, а новая attempt начинает их заново")
	void retryableFailurePersistsCountersAndRetryResetsThem() {
		DiscoveredArchive mentions = stagedArchive(ArchiveType.TRANSLATION_MENTIONS);
		processingLedger.register(mentions.idempotencyKey(), fingerprint("b"));
		ArchiveProcessingAttempt first = processingLedger.claim(
				mentions.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		ArchiveProcessingProgress partial = progress(
				2, 1, 0, 2, 1, 1, 0, 4L);
		ArchiveProcessingFailure retryable = new ArchiveProcessingFailure(
				BULK_PARTIAL_FAILURE,
				true);

		assertThat(processingLedger.markFailed(
				mentions.idempotencyKey(),
				first.token(),
				retryable,
				partial))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
					assertThat(state.progress()).isEqualTo(partial);
					assertThat(state.failure().failure()).isEqualTo(retryable);
					assertThat(state.failure().occurredAt())
							.isEqualTo(FixedClockTestConfiguration.NOW);
					assertThat(state.attempt().retry().consecutiveRetryableFailures())
							.isEqualTo(1);
					assertThat(state.attempt().retry().retryNotBefore())
							.isEqualTo(FixedClockTestConfiguration.NOW);
				});

		ArchiveProcessingAttempt retry = processingLedger.claim(
				mentions.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		assertThat(retry.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow().attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow().progress())
				.isEqualTo(ArchiveProcessingProgress.empty());

		assertThat(processingLedger.markFailed(
				mentions.idempotencyKey(),
				retry.token(),
				new ArchiveProcessingFailure(MAPPING_FAILURE, false),
				ArchiveProcessingProgress.empty()))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.claim(
				mentions.idempotencyKey(),
				Duration.ofMinutes(10)))
				.isEmpty();
	}

	@Test
	@DisplayName("Terminal state требует exact receipt только для INDEXED")
	void terminalReceiptInvariantDistinguishesIndexedAndFailed() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingAttempt attempt = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		ArchiveProcessingProgress unverified = progress(
				2, 0, 0, 2, 2, 0, 1, null);
		String eventArchiveKey = events.idempotencyKey();
		var attemptToken = attempt.token();

		assertThatThrownBy(() ->
				processingLedger.markIndexed(eventArchiveKey, attemptToken, unverified))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("receipt");
		assertThat(processingLedger.findByArchiveIdempotencyKey(eventArchiveKey)
				.orElseThrow().status())
				.isEqualTo(ArchiveProcessingStatus.PROCESSING);

		ArchiveProcessingProgress extraDocuments = progress(
				2, 0, 0, 2, 2, 0, 3, null);
		assertThat(processingLedger.markFailed(
				events.idempotencyKey(),
				attempt.token(),
				new ArchiveProcessingFailure(RECEIPT_MISMATCH, true),
				extraDocuments))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().progress())
				.satisfies(progress -> {
					assertThat(progress.deliveredRecords()).isEqualTo(2);
					assertThat(progress.submittedOperations()).isEqualTo(2);
					assertThat(progress.succeededOperations()).isEqualTo(2);
				});
	}

	@Test
	@DisplayName("Future retry и исчерпанный budget не создают processing token")
	void futureRetryAndExhaustedBudgetDoNotCreateProcessingToken() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("e"));
		ArchiveProcessingAttempt attempt = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		assertThat(processingLedger.markFailed(
				events.idempotencyKey(),
				attempt.token(),
				new ArchiveProcessingFailure(BULK_PARTIAL_FAILURE, true),
				ArchiveProcessingProgress.empty()))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		jdbcClient.sql("""
				update ingestion_archive_processing
				set retry_not_before = :retryNotBefore
				where archive_idempotency_key = :archiveIdempotencyKey
				""")
				.param(
						"retryNotBefore",
						Timestamp.from(FixedClockTestConfiguration.NOW.plusSeconds(60)))
				.param("archiveIdempotencyKey", events.idempotencyKey())
				.update();

		assertThat(processingLedger.claim(events.idempotencyKey(), Duration.ofMinutes(10)))
				.isEmpty();
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().attempt().count()).isEqualTo(1);

		jdbcClient.sql("""
				update ingestion_archive_processing
				set retry_not_before = :retryNotBefore,
				    automatic_retries_used = automatic_retry_limit,
				    total_attempt_count = automatic_retry_limit + 1
				where archive_idempotency_key = :archiveIdempotencyKey
				""")
				.param("retryNotBefore", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("archiveIdempotencyKey", events.idempotencyKey())
				.update();

		assertThat(processingLedger.claim(events.idempotencyKey(), Duration.ofMinutes(10)))
				.isEmpty();
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.attempt().count()).isEqualTo(4);
					assertThat(state.attempt().token()).isNull();
					assertThat(state.attempt().retry().exhausted()).isTrue();
				});
	}

	@Test
	@DisplayName("Receipt mismatch условно открывает INDEXED archive для полного reindex")
	void receiptMismatchConditionallyReopensIndexedArchive() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		ArchiveProcessingFingerprint fingerprint = fingerprint("a");
		processingLedger.register(events.idempotencyKey(), fingerprint);
		ArchiveProcessingAttempt attempt = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		ArchiveProcessingProgress completed = progress(
				3, 1, 1, 2, 2, 0, 2, null);

		assertThat(processingLedger.markIndexed(
				events.idempotencyKey(),
				attempt.token(),
				completed))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)))
				.isEmpty();

		ArchiveProcessingFailure mismatch = new ArchiveProcessingFailure(
				RECEIPT_MISMATCH,
				true);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				"c".repeat(64),
				1,
				mismatch))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				mismatch))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
					assertThat(state.progress()).isEqualTo(completed);
					assertThat(state.completedAt()).isNull();
					assertThat(state.failure().failure()).isEqualTo(mismatch);
				});
		ArchiveProcessingAttempt reindex = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		assertThat(reindex.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().progress())
				.isEqualTo(ArchiveProcessingProgress.empty());
		assertThat(processingLedger.markIndexed(
				events.idempotencyKey(),
				reindex.token(),
				completed))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				mismatch))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.INDEXED);
					assertThat(state.attempt().count()).isEqualTo(2);
				});
	}

	@Test
	@DisplayName("Лишний receipt переводит INDEXED в non-retryable FAILED")
	void recordsNonRetryableReceiptSurplus() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		ArchiveProcessingFingerprint fingerprint = fingerprint("d");
		processingLedger.register(events.idempotencyKey(), fingerprint);
		ArchiveProcessingAttempt attempt = processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)).orElseThrow();
		ArchiveProcessingProgress completed = progress(
				2, 0, 0, 2, 2, 0, 2, null);
		processingLedger.markIndexed(events.idempotencyKey(), attempt.token(), completed);
		ArchiveProcessingFailure surplus = new ArchiveProcessingFailure(
				RECEIPT_SURPLUS,
				false);

		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				surplus))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
					assertThat(state.failure().failure()).isEqualTo(surplus);
				});
		assertThat(processingLedger.claim(
				events.idempotencyKey(),
				Duration.ofMinutes(10)))
				.isEmpty();
	}

	private DiscoveredUpdate registerUpdate() {
		DiscoveredUpdate discovered = update(Instant.parse("2026-07-20T12:00:00Z"));
		archiveLedger.registerDiscoveredUpdate(discovered, FirstRunPolicy.LATEST, null);
		return discovered;
	}

	private DiscoveredArchive stagedArchive(ArchiveType type) {
		DiscoveredUpdate discovered = registerUpdate();
		DiscoveredArchive archive = archive(discovered, type);
		stage(archive);
		return archive;
	}

	private void stage(DiscoveredArchive archive) {
		var attempt = archiveLedger.claimArchive(
				archive.idempotencyKey(),
				ACQUISITION_LEASE).orElseThrow();
		assertThat(archiveLedger.markStaged(
				archive.idempotencyKey(),
				attempt.token(),
				new StagedArchive(
						Path.of("staging", archive.archiveName()),
						Path.of("staging", archive.archiveName().replaceFirst("\\.zip$", "")),
						archive.expectedSizeBytes(),
						archive.expectedMd5())))
				.isEqualTo(AttemptTransitionResult.APPLIED);
	}

	private void expireLease(DiscoveredArchive archive) {
		jdbcClient.sql("""
				update ingestion_archive_processing
				set last_attempt_at = :lastAttemptAt,
				    lease_expires_at = :leaseExpiresAt
				where archive_idempotency_key = :archiveIdempotencyKey
				""")
				.param(
						"lastAttemptAt",
						Timestamp.from(FixedClockTestConfiguration.NOW.minus(Duration.ofMinutes(11))))
				.param(
						"leaseExpiresAt",
						Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(1)))
				.param("archiveIdempotencyKey", archive.idempotencyKey())
				.update();
	}

	private static DiscoveredArchive archive(DiscoveredUpdate update, ArchiveType type) {
		return update.archives().stream()
				.filter(archive -> archive.archiveType() == type)
				.findFirst()
				.orElseThrow();
	}

	private static ArchiveProcessingFingerprint fingerprint(String hexDigit) {
		return new ArchiveProcessingFingerprint(
				"0123456789abcdef0123456789abcdef",
				"gdelt-index-v1",
				hexDigit.repeat(64)
		);
	}

	private static ArchiveProcessingProgress progress(
			long delivered,
			long sourceInvalid,
			long mappingRejected,
			long submitted,
			long succeeded,
			long failed,
			long receipt,
			Long firstFailedLine
	) {
		return new ArchiveProcessingProgress(
				delivered,
				sourceInvalid,
				mappingRejected,
				submitted,
				succeeded,
				failed,
				receipt,
				firstFailedLine
		);
	}

	private static <T> List<T> runConcurrently(Callable<T> first, Callable<T> second)
			throws Exception {
		var executor = Executors.newFixedThreadPool(2);
		var ready = new CountDownLatch(2);
		var start = new CountDownLatch(1);
		try {
			var firstResult = executor.submit(() -> awaitStartAndCall(ready, start, first));
			var secondResult = executor.submit(() -> awaitStartAndCall(ready, start, second));
			if (!ready.await(10, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Concurrent test workers did not become ready");
			}
			start.countDown();
			return List.of(
					firstResult.get(30, TimeUnit.SECONDS),
					secondResult.get(30, TimeUnit.SECONDS)
			);
		} finally {
			start.countDown();
			executor.shutdownNow();
		}
	}

	private static <T> T awaitStartAndCall(
			CountDownLatch ready,
			CountDownLatch start,
			Callable<T> action
	) throws Exception {
		ready.countDown();
		if (!start.await(10, TimeUnit.SECONDS)) {
			throw new IllegalStateException("Concurrent test start was not released");
		}
		return action.call();
	}
}
