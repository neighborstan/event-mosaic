package com.neighbor.eventmosaic.ingestion.state;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimResult;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
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
	private static final String PARTITION_KEY = "p20260720";
	private static final String EVENT_INDEX_NAME = "gdelt-events-v1-p20260720-g0001";
	private static final String MENTION_INDEX_NAME = "gdelt-mentions-v1-p20260720-g0001";
	private static final String EVENT_INDEX_UUID = "event-index-uuid";
	private static final String MENTION_INDEX_UUID = "mention-index-uuid";

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
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions,
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
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());

		List<ArchiveProcessingClaimResult> claims = runConcurrently(
				() -> claimResult(events, target),
				() -> claimResult(events, target)
		);

		assertThat(claims)
				.filteredOn(claim -> claim.status() == ArchiveProcessingClaimStatus.CLAIMED)
				.hasSize(1);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.PROCESSING);
					assertThat(state.attempt().count()).isEqualTo(1);
					assertThat(state.attempt().token()).isNotNull();
				});
	}

	@Test
	@DisplayName("Claim возвращает typed outcomes для stale target и открытого maintenance")
	void claimReturnsTypedTargetOutcomes() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingTargetBinding staleTarget = new ArchiveProcessingTargetBinding(
				target.indexKind(),
				target.partitionKey(),
				target.partitionStateVersion() + 1,
				target.generationId(),
				target.generationUuid(),
				target.indexName(),
				target.indexUuid());

		assertThat(claimResult(events, staleTarget).status())
				.isEqualTo(ArchiveProcessingClaimStatus.OWNERSHIP_LOST);
		updatePartitionBoundaries(
				Instant.parse("2026-07-13T00:00:00Z"),
				Instant.parse("2026-07-20T00:00:00Z"));
		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.OWNERSHIP_LOST);
		updatePartitionBoundaries(
				Instant.parse("2026-07-20T00:00:00Z"),
				Instant.parse("2026-07-27T00:00:00Z"));

		insertOpenMaintenance(target);
		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.MAINTENANCE_DEFERRED);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().status()).isEqualTo(ArchiveProcessingStatus.PENDING);
	}

	@Test
	@DisplayName("Claim откладывается, пока partition не имеет ACTIVE generation")
	void claimDefersWithoutActiveGeneration() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		jdbcClient.sql("""
				update index_generations
				set state = 'SUPERSEDED',
				    superseded_at = :supersededAt,
				    updated_at = :updatedAt
				where id = :generationId
				""")
				.param("supersededAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("generationId", target.generationId())
				.update();

		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.MAINTENANCE_DEFERRED);
	}

	@Test
	@DisplayName("Captured partition version и generation UUID fence progress и terminal receipt")
	void targetBindingFencesProgressAndStoresCountReceipt() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("f"));
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingAttempt attempt = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		ArchiveProcessingProgress completed = progress(
				2, 0, 0, 2, 2, 0, 2, null);
		ArchiveProcessingTargetBinding wrongGeneration = new ArchiveProcessingTargetBinding(
				target.indexKind(),
				target.partitionKey(),
				target.partitionStateVersion(),
				target.generationId(),
				UUID.randomUUID(),
				target.indexName(),
				target.indexUuid());
		ArchiveProcessingAttempt staleAttempt = new ArchiveProcessingAttempt(
				attempt.archiveIdempotencyKey(),
				attempt.fingerprint(),
				wrongGeneration,
				attempt.token(),
				attempt.leaseExpiresAt(),
				attempt.attemptCount(),
				attempt.recovered());

		assertThat(processingLedger.checkpoint(
				staleAttempt,
				ArchiveProcessingProgress.empty(),
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.markIndexed(staleAttempt, completed))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(current -> {
					assertThat(current.status()).isEqualTo(ArchiveProcessingStatus.PROCESSING);
					assertThat(current.receipt()).isNull();
					assertThat(current.completedAt()).isNull();
				});
		assertThat(processingLedger.markIndexed(attempt, completed))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		ArchiveProcessingState state = processingLedger.findByArchiveIdempotencyKey(
				events.idempotencyKey()).orElseThrow();
		assertThat(state.targetBinding()).isEqualTo(target);
		assertThat(state.receipt()).satisfies(receipt -> {
			assertThat(receipt.expectedDocumentCount()).isEqualTo(2);
			assertThat(receipt.actualDocumentCount()).isEqualTo(2);
			assertThat(receipt.verifiedGenerationId()).isEqualTo(target.generationId());
			assertThat(receipt.verifiedIndexUuid()).isEqualTo(target.indexUuid());
		});
		assertThat(jdbcClient.sql("""
				select count(*)
				from ingestion_archive_processing
				where archive_idempotency_key = :archiveIdempotencyKey
				  and bound_generation_uuid = :generationUuid
				  and bound_index_name = :indexName
				  and receipt_digest_algorithm is null
				  and expected_identity_digest is null
				  and actual_identity_digest is null
				""")
				.param("archiveIdempotencyKey", events.idempotencyKey())
				.param("generationUuid", target.generationUuid())
				.param("indexName", target.indexName())
				.query(Integer.class)
				.single()).isEqualTo(1);

		DiscoveredArchive mentions = stagedArchive(ArchiveType.TRANSLATION_MENTIONS);
		processingLedger.register(mentions.idempotencyKey(), fingerprint("b"));
		ArchiveProcessingAttempt mentionAttempt = claim(mentions);
		jdbcClient.sql("""
				update index_logical_partitions
				set state_version = state_version + 1,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				""")
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("partitionKey", PARTITION_KEY)
				.update();
		assertThat(processingLedger.checkpoint(
				mentionAttempt,
				ArchiveProcessingProgress.empty(),
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.markIndexed(mentionAttempt, completed))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow())
				.satisfies(current -> {
					assertThat(current.status()).isEqualTo(ArchiveProcessingStatus.PROCESSING);
					assertThat(current.receipt()).isNull();
					assertThat(current.completedAt()).isNull();
				});
	}

	@Test
	@DisplayName("Просроченный lease восстанавливается и stale token не меняет progress")
	void recoversExpiredLeaseAndRejectsStaleToken() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingAttempt first = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		expireLease(events);

		ArchiveProcessingAttempt recovered = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		ArchiveProcessingProgress checkpoint = progress(
				2, 1, 1, 1, 1, 0, 0, null);

		assertThat(recovered.recovered()).isTrue();
		assertThat(recovered.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(processingLedger.checkpoint(
				first,
				checkpoint,
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.markFailed(
				first,
				new ArchiveProcessingFailure(MAPPING_FAILURE, false),
				ArchiveProcessingProgress.empty()))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThat(processingLedger.checkpoint(
				recovered,
				checkpoint,
				Duration.ofMinutes(20)))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.checkpoint(
				recovered,
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
		ArchiveProcessingAttempt first = claim(mentions);
		ArchiveProcessingProgress partial = progress(
				2, 1, 0, 2, 1, 1, 0, 4L);
		ArchiveProcessingFailure retryable = new ArchiveProcessingFailure(
				BULK_PARTIAL_FAILURE,
				true);

		assertThat(processingLedger.markFailed(
				first,
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

		ArchiveProcessingAttempt retry = claim(mentions);
		assertThat(retry.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow().attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(processingLedger.findByArchiveIdempotencyKey(mentions.idempotencyKey())
				.orElseThrow().progress())
				.isEqualTo(ArchiveProcessingProgress.empty());

		assertThat(processingLedger.markFailed(
				retry,
				new ArchiveProcessingFailure(MAPPING_FAILURE, false),
				ArchiveProcessingProgress.empty()))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(claimResult(mentions, activeTarget(mentions.archiveType())).status())
				.isEqualTo(ArchiveProcessingClaimStatus.NOT_CLAIMABLE);
	}

	@Test
	@DisplayName("Terminal state требует exact receipt только для INDEXED")
	void terminalReceiptInvariantDistinguishesIndexedAndFailed() {
		DiscoveredArchive events = stagedArchive(ArchiveType.TRANSLATION_EVENTS);
		processingLedger.register(events.idempotencyKey(), fingerprint("a"));
		ArchiveProcessingAttempt attempt = claim(events);
		ArchiveProcessingProgress unverified = progress(
				2, 0, 0, 2, 2, 0, 1, null);
		String eventArchiveKey = events.idempotencyKey();

		assertThatThrownBy(() ->
				processingLedger.markIndexed(attempt, unverified))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("receipt");
		assertThat(processingLedger.findByArchiveIdempotencyKey(eventArchiveKey)
				.orElseThrow().status())
				.isEqualTo(ArchiveProcessingStatus.PROCESSING);

		ArchiveProcessingProgress extraDocuments = progress(
				2, 0, 0, 2, 2, 0, 3, null);
		assertThat(processingLedger.markFailed(
				attempt,
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
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingAttempt attempt = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		assertThat(processingLedger.markFailed(
				attempt,
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

		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.NOT_CLAIMABLE);
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

		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.NOT_CLAIMABLE);
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
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingAttempt attempt = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		ArchiveProcessingProgress completed = progress(
				3, 1, 1, 2, 2, 0, 2, null);

		assertThat(processingLedger.markIndexed(
				attempt,
				completed))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.NOT_CLAIMABLE);

		ArchiveProcessingFailure mismatch = new ArchiveProcessingFailure(
				RECEIPT_MISMATCH,
				true);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				"c".repeat(64),
				1,
				target,
				mismatch))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				target,
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
		ArchiveProcessingAttempt reindex = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		assertThat(reindex.attemptCount()).isEqualTo(2);
		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow().progress())
				.isEqualTo(ArchiveProcessingProgress.empty());
		assertThat(processingLedger.markIndexed(
				reindex,
				completed))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				target,
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
		ArchiveProcessingTargetBinding target = activeTarget(events.archiveType());
		ArchiveProcessingAttempt attempt = claimResult(events, target)
				.claimedAttempt().orElseThrow();
		ArchiveProcessingProgress completed = progress(
				2, 0, 0, 2, 2, 0, 2, null);
		processingLedger.markIndexed(attempt, completed);
		ArchiveProcessingFailure surplus = new ArchiveProcessingFailure(
				RECEIPT_SURPLUS,
				false);

		assertThat(processingLedger.recordReceiptMismatch(
				events.idempotencyKey(),
				fingerprint.processingFingerprint(),
				1,
				target,
				surplus))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(processingLedger.findByArchiveIdempotencyKey(events.idempotencyKey())
				.orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(ArchiveProcessingStatus.FAILED);
					assertThat(state.failure().failure()).isEqualTo(surplus);
				});
		assertThat(claimResult(events, target).status())
				.isEqualTo(ArchiveProcessingClaimStatus.NOT_CLAIMABLE);
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

	private ArchiveProcessingAttempt claim(DiscoveredArchive archive) {
		return claimResult(archive, activeTarget(archive.archiveType()))
				.claimedAttempt()
				.orElseThrow();
	}

	private ArchiveProcessingClaimResult claimResult(
			DiscoveredArchive archive,
			ArchiveProcessingTargetBinding target
	) {
		return processingLedger.claim(
				archive.idempotencyKey(),
				target,
				Duration.ofMinutes(10));
	}

	private ArchiveProcessingTargetBinding activeTarget(ArchiveType archiveType) {
		Instant partitionStart = Instant.parse("2026-07-20T00:00:00Z");
		jdbcClient.sql("""
				insert into index_logical_partitions (
				    partition_key,
				    partition_start_at,
				    partition_end_at,
				    partition_interval,
				    state_version,
				    created_at,
				    updated_at
				)
				values (
				    :partitionKey,
				    :partitionStartAt,
				    :partitionEndAt,
				    'P7D',
				    1,
				    :createdAt,
				    :updatedAt
				)
				on conflict (partition_key) do nothing
				""")
				.param("partitionKey", PARTITION_KEY)
				.param("partitionStartAt", Timestamp.from(partitionStart))
				.param("partitionEndAt", Timestamp.from(partitionStart.plus(Duration.ofDays(7))))
				.param("createdAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.update();
		jdbcClient.sql("""
				insert into index_generations (
				    generation_uuid,
				    partition_key,
				    generation_number,
				    state,
				    event_index_name,
				    event_index_uuid,
				    mention_index_name,
				    mention_index_uuid,
				    heartbeat_at,
				    activated_at,
				    created_at,
				    updated_at
				)
				values (
				    :generationUuid,
				    :partitionKey,
				    1,
				    'ACTIVE',
				    :eventIndexName,
				    :eventIndexUuid,
				    :mentionIndexName,
				    :mentionIndexUuid,
				    :heartbeatAt,
				    :activatedAt,
				    :createdAt,
				    :updatedAt
				)
				on conflict (partition_key, generation_number) do nothing
				""")
				.param("generationUuid", UUID.randomUUID())
				.param("partitionKey", PARTITION_KEY)
				.param("eventIndexName", EVENT_INDEX_NAME)
				.param("eventIndexUuid", EVENT_INDEX_UUID)
				.param("mentionIndexName", MENTION_INDEX_NAME)
				.param("mentionIndexUuid", MENTION_INDEX_UUID)
				.param("heartbeatAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("activatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("createdAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.update();
		return jdbcClient.sql("""
				select
				    partition_state.state_version,
				    generation.id,
				    generation.generation_uuid
				from index_logical_partitions partition_state
				join index_generations generation
				  on generation.partition_key = partition_state.partition_key
				 and generation.state = 'ACTIVE'
				where partition_state.partition_key = :partitionKey
				""")
				.param("partitionKey", PARTITION_KEY)
				.query((resultSet, rowNumber) -> {
					GdeltIndexKind kind = archiveType == ArchiveType.TRANSLATION_EVENTS
							? GdeltIndexKind.EVENT
							: GdeltIndexKind.MENTION;
					return new ArchiveProcessingTargetBinding(
							kind,
							PARTITION_KEY,
							resultSet.getLong("state_version"),
							resultSet.getLong("id"),
							resultSet.getObject("generation_uuid", UUID.class),
							kind == GdeltIndexKind.EVENT
									? EVENT_INDEX_NAME : MENTION_INDEX_NAME,
							kind == GdeltIndexKind.EVENT
									? EVENT_INDEX_UUID : MENTION_INDEX_UUID);
				})
				.single();
	}

	private void insertOpenMaintenance(ArchiveProcessingTargetBinding target) {
		jdbcClient.sql("""
				insert into index_maintenance_operations (
				    operation_token,
				    partition_key,
				    operation_kind,
				    phase,
				    expected_partition_state_version,
				    cleanup_generation_id,
				    lease_expires_at,
				    heartbeat_at,
				    created_at,
				    updated_at
				)
				values (
				    :operationToken,
				    :partitionKey,
				    'CLEANUP',
				    'PLANNED',
				    :partitionStateVersion,
				    :cleanupGenerationId,
				    :leaseExpiresAt,
				    :heartbeatAt,
				    :createdAt,
				    :updatedAt
				)
				""")
				.param("operationToken", UUID.randomUUID())
				.param("partitionKey", target.partitionKey())
				.param("partitionStateVersion", target.partitionStateVersion())
				.param("cleanupGenerationId", target.generationId())
				.param(
						"leaseExpiresAt",
						Timestamp.from(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(10))))
				.param("heartbeatAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("createdAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.update();
	}

	private void updatePartitionBoundaries(Instant startAt, Instant endAt) {
		jdbcClient.sql("""
				update index_logical_partitions
				set partition_start_at = :startAt,
				    partition_end_at = :endAt,
				    updated_at = :updatedAt
				where partition_key = :partitionKey
				""")
				.param("startAt", Timestamp.from(startAt))
				.param("endAt", Timestamp.from(endAt))
				.param("updatedAt", Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("partitionKey", PARTITION_KEY)
				.update();
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
