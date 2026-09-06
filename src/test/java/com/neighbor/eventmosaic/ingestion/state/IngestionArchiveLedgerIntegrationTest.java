package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;

import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveStatus;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.Clock;
import java.time.ZoneOffset;
import java.sql.Timestamp;
import java.util.Set;
import java.util.List;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Import({PostgreSqlTestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Интеграция журнала загрузки с PostgreSQL")
class IngestionArchiveLedgerIntegrationTest {

	@Autowired
	private IngestionArchiveLedger archiveLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private RecentRecoveryPlanLedger recentPlanLedger;

	@Autowired
	private JdbcIngestionRunRepository runRepository;

	@Autowired
	private JdbcIngestionArchiveRepository archiveRepository;

	@Autowired
	private JdbcIngestionContinuityRepository continuityRepository;

	@Autowired
	private PlatformTransactionManager transactionManager;

	@BeforeEach
	void cleanLedger() {
		jdbcClient.sql("""
				truncate table ingestion_gaps, ingestion_source_state, ingestion_archives, ingestion_runs
				restart identity cascade
				""").update();
	}

	@Test
	@DisplayName("После длительного отказа загрузки новая серия сохраняет историю и разрешает только одного владельца")
	void recentAcquisitionRearmsAtCooldownBoundaryAndKeepsEvidence() throws Exception {
		Instant frontier = Instant.parse("2026-07-20T12:00:00Z");
		recentPlanLedger.activate(update(frontier), RecentWindowPlan.fromBoundaries(frontier.minus(Duration.ofHours(24)), frontier, frontier));
		DiscoveredArchive events = update(frontier).archives().getFirst();
		Instant now = FixedClockTestConfiguration.NOW;
		var transaction = new TransactionTemplate(transactionManager);
		for (int retry = 0; retry <= 3; retry++) {
			IngestionArchiveLedger runtime = at(now);
			var attempt = transaction.execute(_ -> runtime.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10)).orElseThrow());
			assertThat(attempt).isNotNull();
			assertThat(attempt.attemptCount()).isEqualTo(retry + 1);
			transaction.executeWithoutResult(_ -> runtime.markFailed(events.idempotencyKey(), attempt.token(),
					new IngestionFailure(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, true)));
			var state = runtime.findByIdempotencyKey(events.idempotencyKey()).orElseThrow();
			assertThat(state.attempt().retry().retryNotBefore()).isEqualTo(now.plus(Duration.ofMinutes(retry == 3 ? 15 : 1L << retry)));
			now = state.attempt().retry().retryNotBefore();
		}
		Instant boundary = now;
		var early = at(boundary.minusNanos(1000));
		var earlyClaim = transaction.execute(_ -> early.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10)));
		assertThat(earlyClaim).isEmpty();
		IngestionArchiveLedger restarted = at(boundary);
		var evidence = restarted.findByIdempotencyKey(events.idempotencyKey()).orElseThrow().attempt().retry();
		var claims = runConcurrently(
				() -> new TransactionTemplate(transactionManager).execute(_ -> restarted.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10))),
				() -> new TransactionTemplate(transactionManager).execute(_ -> restarted.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10))));
		assertThat(claims).filteredOn(java.util.Optional::isPresent).hasSize(1);
		var owner = claims.stream().flatMap(java.util.Optional::stream).findFirst().orElseThrow();
		assertThat(owner.attemptCount()).isEqualTo(5);
		transaction.executeWithoutResult(_ -> restarted.markStaged(events.idempotencyKey(), owner.token(), staged(events)));
		var completed = restarted.findByIdempotencyKey(events.idempotencyKey()).orElseThrow();
		assertThat(completed.status()).isEqualTo(IngestionArchiveStatus.STAGED);
		assertThat(completed.attempt().retry().retrySequence()).isEqualTo(2);
		assertThat(completed.attempt().retry().lastExhaustedAt()).isEqualTo(evidence.lastExhaustedAt());
		assertThat(completed.attempt().retry().lastExhaustedErrorCode()).isEqualTo("DOWNLOAD_HTTP_ERROR");
	}

	@Test
	@DisplayName("Выборка недавних загрузок ограничена, упорядочена и пропускает будущие повторы и постоянные ошибки")
	void eligibleRecentWorkIsBoundedOrderedAndExcludesUnavailableArchives() {
		Instant frontier = Instant.parse("2026-07-20T12:00:00Z");
		recentPlanLedger.activate(update(frontier), RecentWindowPlan.fromBoundaries(frontier.minus(Duration.ofHours(24)), frontier, frontier));
		DiscoveredUpdate older = update(frontier.minusSeconds(900));
		archiveLedger.registerDiscoveredUpdate(older, FirstRunPolicy.LATEST, null);
		DiscoveredUpdate outside = update(frontier.minus(Duration.ofHours(24)).minusSeconds(900));
		archiveLedger.registerDiscoveredUpdate(outside, FirstRunPolicy.LATEST, null);
		var newestEvent = update(frontier).archives().getFirst();
		var newestMention = update(frontier).archives().getLast();
		var oldEvent = older.archives().getFirst();
		var oldMention = older.archives().getLast();
		assertThat(archiveLedger.findEligibleRecentWork(3, Set.of()))
				.extracting(state -> state.archive().idempotencyKey())
				.containsExactly(newestEvent.idempotencyKey(), oldEvent.idempotencyKey(), newestMention.idempotencyKey());
		assertThat(archiveLedger.findEligibleRecentWork(1, Set.of(newestEvent.idempotencyKey())))
				.extracting(state -> state.archive().idempotencyKey()).containsExactly(oldEvent.idempotencyKey());
		var eventAttempt = archiveLedger.claimArchive(newestEvent.idempotencyKey(), Duration.ofMinutes(10)).orElseThrow();
		archiveLedger.markFailed(newestEvent.idempotencyKey(), eventAttempt.token(), new IngestionFailure(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, true));
		var mentionAttempt = archiveLedger.claimArchive(newestMention.idempotencyKey(), Duration.ofMinutes(10)).orElseThrow();
		archiveLedger.markFailed(newestMention.idempotencyKey(), mentionAttempt.token(), new IngestionFailure(IngestionErrorCode.STAGING_ARTIFACT_CONFLICT, false));
		var active = archiveLedger.claimArchive(oldEvent.idempotencyKey(), Duration.ofMinutes(10)).orElseThrow();
		assertThat(archiveLedger.findEligibleRecentWork(10, Set.of())).extracting(state -> state.archive().idempotencyKey()).containsExactly(oldMention.idempotencyKey());
		jdbcClient.sql("update ingestion_archives set retry_not_before = :now where idempotency_key = :key")
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW)).param("key", newestEvent.idempotencyKey()).update();
		jdbcClient.sql("update ingestion_archives set last_attempt_at = :start, lease_expires_at = :now where idempotency_key = :key")
				.param("start", Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(60)))
				.param("now", Timestamp.from(FixedClockTestConfiguration.NOW)).param("key", oldEvent.idempotencyKey()).update();
		assertThat(archiveLedger.findEligibleRecentWork(10, Set.of())).extracting(state -> state.archive().idempotencyKey())
				.containsExactly(newestEvent.idempotencyKey(), oldEvent.idempotencyKey(), oldMention.idempotencyKey());
		assertThat(archiveLedger.claimArchive(oldEvent.idempotencyKey(), Duration.ofMinutes(10))).isPresent();
		assertThat(archiveLedger.markStaged(oldEvent.idempotencyKey(), active.token(), staged(oldEvent))).isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThatThrownBy(() -> archiveLedger.findEligibleRecentWork(0, Set.of())).isInstanceOf(IllegalArgumentException.class);
		assertThatThrownBy(() -> archiveLedger.findEligibleRecentWork(1025, Set.of())).isInstanceOf(IllegalArgumentException.class);
	}

	@Test
	@DisplayName("Исчерпанный архив после выхода из суточного плана сохраняется для диагностики без нового владельца")
	void exhaustedAcquisitionAgedOutOfPlanCannotRearm() {
		Instant frontier = Instant.parse("2026-07-20T12:00:00Z");
		recentPlanLedger.activate(update(frontier), RecentWindowPlan.fromBoundaries(frontier.minus(Duration.ofHours(24)), frontier, frontier));
		var events = update(frontier).archives().getFirst();
		var attempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10)).orElseThrow();
		archiveLedger.markFailed(events.idempotencyKey(), attempt.token(), new IngestionFailure(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, true));
		jdbcClient.sql("""
				update ingestion_archives set automatic_retries_used = 3, total_attempt_count = 4,
					retry_not_before = :now, last_exhausted_at = failed_at, last_exhausted_error_code = last_error_code
				where idempotency_key = :key
				""").param("now", Timestamp.from(FixedClockTestConfiguration.NOW)).param("key", events.idempotencyKey()).update();
		Instant next = frontier.plus(Duration.ofHours(25));
		recentPlanLedger.activate(update(next), RecentWindowPlan.fromBoundaries(next.minus(Duration.ofHours(24)), next, next));
		assertThat(archiveLedger.findEligibleRecentWork(10, Set.of())).noneMatch(state -> state.archive().idempotencyKey().equals(events.idempotencyKey()));
		assertThat(archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(10))).isEmpty();
		var state = archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow();
		assertThat(state.attempt().count()).isEqualTo(4);
		assertThat(state.attempt().retry().retrySequence()).isEqualTo(1);
		assertThat(state.attempt().retry().lastExhaustedErrorCode()).isEqualTo("DOWNLOAD_HTTP_ERROR");
	}

	private IngestionArchiveLedger at(Instant now) {
		return new JdbcIngestionArchiveLedger(runRepository, archiveRepository, continuityRepository, Clock.fixed(now, ZoneOffset.UTC));
	}

	@Test
	@DisplayName("Повторная регистрация идемпотентна, а пропуск обновлений создает наблюдаемый разрыв")
	void registersUpdatesIdempotentlyAndCreatesObservableGap() {
		Instant firstTime = Instant.parse("2026-07-20T12:00:00Z");
		Instant nextTime = Instant.parse("2026-07-20T12:45:00Z");

		var first = archiveLedger.registerDiscoveredUpdate(
				update(firstTime),
				FirstRunPolicy.LATEST,
				null
		);
		var repeated = archiveLedger.registerDiscoveredUpdate(
				update(firstTime),
				FirstRunPolicy.LATEST,
				null
		);
		var next = archiveLedger.registerDiscoveredUpdate(
				update(nextTime),
				FirstRunPolicy.LATEST,
				null
		);

		assertThat(first).isZero();
		assertThat(repeated).isZero();
		assertThat(archiveLedger.findRunByUpdateTime(firstTime).orElseThrow().archives()).hasSize(2);
		assertThat(next).isEqualTo(1);
		assertThat(archiveLedger.findOpenGaps()).singleElement().satisfies(gap -> {
			assertThat(gap.firstMissingUpdateTime()).isEqualTo(Instant.parse("2026-07-20T12:15:00Z"));
			assertThat(gap.lastMissingUpdateTime()).isEqualTo(Instant.parse("2026-07-20T12:30:00Z"));
		});
	}

	@Test
	@DisplayName("Ledger отклоняет несогласованные параметры первого запуска до сохранения state")
	void rejectsInvalidFirstRunArgumentsAtLedgerBoundary() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		DiscoveredUpdate discovered = update(updateTime);
		Instant unexpectedStart = updateTime.minus(Duration.ofMinutes(15));
		Instant offBoundaryStart = updateTime.minusSeconds(1);

		assertThatThrownBy(() -> archiveLedger.registerDiscoveredUpdate(
				discovered,
				FirstRunPolicy.LATEST,
				unexpectedStart))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("must be absent");
		assertThatThrownBy(() -> archiveLedger.registerDiscoveredUpdate(
				discovered,
				FirstRunPolicy.FIXED,
				offBoundaryStart))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("15-minute UTC boundary");
		assertThat(jdbcClient.sql("select count(*) from ingestion_runs").query(Integer.class).single())
				.isZero();
	}

	@Test
	@DisplayName("Параллельная регистрация одного обновления остается идемпотентной")
	void concurrentRegistrationOfSameUpdateIsIdempotent() throws Exception {
		DiscoveredUpdate discovered = update(Instant.parse("2026-07-20T12:00:00Z"));

		var registrations = runConcurrently(
				() -> archiveLedger.registerDiscoveredUpdate(discovered, FirstRunPolicy.LATEST, null),
				() -> archiveLedger.registerDiscoveredUpdate(discovered, FirstRunPolicy.LATEST, null)
		);

		assertThat(registrations).containsOnly(0);
		assertThat(archiveLedger.findRunByUpdateTime(discovered.sourceUpdateTime()).orElseThrow().archives())
				.hasSize(2);
		assertThat(jdbcClient.sql("select count(*) from ingestion_runs").query(Integer.class).single())
				.isEqualTo(1);
		assertThat(jdbcClient.sql("select count(*) from ingestion_archives").query(Integer.class).single())
				.isEqualTo(2);
		assertThat(jdbcClient.sql("select count(*) from ingestion_source_state").query(Integer.class).single())
				.isEqualTo(1);
	}

	@Test
	@DisplayName("Фиксированная стартовая точка создает разрыв после заданной границы")
	void fixedFirstRunCreatesGapAfterExplicitBaseline() {
		Instant updateTime = Instant.parse("2026-07-20T12:45:00Z");

		var registered = archiveLedger.registerDiscoveredUpdate(
				update(updateTime),
				FirstRunPolicy.FIXED,
				Instant.parse("2026-07-20T12:00:00Z")
		);

		assertThat(registered).isEqualTo(1);
		assertThat(archiveLedger.findOpenGaps()).hasSize(1);
	}

	@Test
	@DisplayName("Изменение метаданных существующего типа архива отклоняется")
	void rejectsMetadataChangeForExistingRunType() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		archiveLedger.registerDiscoveredUpdate(update(updateTime), FirstRunPolicy.LATEST, null);
		DiscoveredUpdate conflicting = update(updateTime, "11111111111111111111111111111111");

		assertThatExceptionOfType(SourceDataViolationException.class)
				.isThrownBy(() -> archiveLedger.registerDiscoveredUpdate(
				conflicting,
				FirstRunPolicy.LATEST,
				null))
				.extracting(SourceDataViolationException::errorCode)
				.isEqualTo(IngestionErrorCode.ARCHIVE_METADATA_CONFLICT);
	}

	@Test
	@DisplayName("Аренда блокирует параллельный захват и отклоняет устаревший токен попытки")
	void claimUsesLeaseAndRejectsStaleAttemptToken() {
		DiscoveredArchive events = update(Instant.parse("2026-07-20T12:00:00Z")).archives().getFirst();
		archiveLedger.registerDiscoveredUpdate(
				new DiscoveredUpdate(
						events.sourceUpdateTime(),
						List.of(events, update(events.sourceUpdateTime()).archives().getLast()),
						List.of()),
				FirstRunPolicy.LATEST,
				null
		);

		var firstClaim = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		assertThat(firstClaim.leaseExpiresAt())
				.isEqualTo(FixedClockTestConfiguration.NOW.plus(Duration.ofMinutes(15)));
		assertThat(archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).isEmpty();

		jdbcClient.sql("""
				update ingestion_archives
				set last_attempt_at = :lastAttemptAt,
				    lease_expires_at = :expiredAt
				where idempotency_key = :idempotencyKey
				""")
				.param(
						"lastAttemptAt",
						java.sql.Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(2)))
				.param("expiredAt", java.sql.Timestamp.from(FixedClockTestConfiguration.NOW.minusSeconds(1)))
				.param("idempotencyKey", events.idempotencyKey())
				.update();
		var recovered = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();

		assertThat(recovered.recovered()).isTrue();
		assertThat(recovered.attemptCount()).isEqualTo(2);
		assertThat(archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow()
				.attempt().retry().automaticRetriesUsed()).isEqualTo(1);
		assertThat(archiveLedger.markStaged(
				events.idempotencyKey(),
				firstClaim.token(),
				staged(events))).isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(archiveLedger.markFailed(
				events.idempotencyKey(),
				firstClaim.token(),
				new IngestionFailure(
						IngestionErrorCode.DOWNLOAD_MD5_MISMATCH,
						true)))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionArchiveStatus.PROCESSING);
					assertThat(state.attempt().token()).isEqualTo(recovered.token());
					assertThat(state.failure()).isNull();
					assertThat(state.stagedArchive()).isNull();
				});
		assertThat(archiveLedger.markStaged(
				events.idempotencyKey(),
				recovered.token(),
				staged(events))).isEqualTo(AttemptTransitionResult.APPLIED);
	}

	@Test
	@DisplayName("Future retry и исчерпанный budget не создают acquisition token")
	void futureRetryAndExhaustedBudgetDoNotCreateAcquisitionToken() {
		DiscoveredArchive events = update(Instant.parse("2026-07-20T12:00:00Z")).archives().getFirst();
		archiveLedger.registerDiscoveredUpdate(update(events.sourceUpdateTime()), FirstRunPolicy.LATEST, null);
		var attempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		assertThat(archiveLedger.markFailed(
				events.idempotencyKey(),
				attempt.token(),
				new IngestionFailure(IngestionErrorCode.DOWNLOAD_HTTP_ERROR, true)))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		jdbcClient.sql("""
				update ingestion_archives
				set retry_not_before = :retryNotBefore
				where idempotency_key = :idempotencyKey
				""")
				.param(
						"retryNotBefore",
						java.sql.Timestamp.from(FixedClockTestConfiguration.NOW.plusSeconds(60)))
				.param("idempotencyKey", events.idempotencyKey())
				.update();

		assertThat(archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).isEmpty();
		assertThat(archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow()
				.attempt().count()).isEqualTo(1);

		jdbcClient.sql("""
				update ingestion_archives
				set retry_not_before = :retryNotBefore,
				    automatic_retries_used = automatic_retry_limit,
				    total_attempt_count = automatic_retry_limit + 1
				where idempotency_key = :idempotencyKey
				""")
				.param("retryNotBefore", java.sql.Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("idempotencyKey", events.idempotencyKey())
				.update();

		assertThat(archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).isEmpty();
		assertThat(archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow())
				.satisfies(state -> {
					assertThat(state.attempt().count()).isEqualTo(4);
					assertThat(state.attempt().token()).isNull();
					assertThat(state.attempt().retry().exhausted()).isTrue();
				});
	}

	@Test
	@DisplayName("Частичный прогресс сохраняется по типам и завершается после повторной попытки")
	void keepsPerTypeCompletionWhenRunIsPartialAndFinishesAfterRetry() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		DiscoveredUpdate update = update(updateTime);
		archiveLedger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null);
		DiscoveredArchive events = archive(update, ArchiveType.TRANSLATION_EVENTS);
		DiscoveredArchive mentions = archive(update, ArchiveType.TRANSLATION_MENTIONS);

		var eventAttempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		assertThat(archiveLedger.markStaged(
				events.idempotencyKey(),
				eventAttempt.token(),
				staged(events))).isEqualTo(AttemptTransitionResult.APPLIED);
		var mentionAttempt = archiveLedger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		IngestionFailure mentionFailure = new IngestionFailure(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH,
				true);
		assertThat(archiveLedger.markFailed(
				mentions.idempotencyKey(),
				mentionAttempt.token(),
				mentionFailure))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(archiveLedger.findRunByUpdateTime(updateTime).orElseThrow().status())
				.isEqualTo(IngestionRunStatus.PARTIAL);
		assertThat(archiveLedger.findByIdempotencyKey(mentions.idempotencyKey()).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionArchiveStatus.FAILED);
					assertThat(state.failure().failure()).isEqualTo(mentionFailure);
					assertThat(state.failure().occurredAt()).isEqualTo(FixedClockTestConfiguration.NOW);
				});
		assertThat(archiveLedger.findCompletionProgress(ArchiveType.TRANSLATION_EVENTS)).contains(updateTime);
		assertThat(archiveLedger.findCompletionProgress(ArchiveType.TRANSLATION_MENTIONS)).isEmpty();

		assertThat(archiveLedger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15)))
				.isEmpty();
		jdbcClient.sql("""
				update ingestion_archives
				set retry_not_before = :retryNotBefore
				where idempotency_key = :idempotencyKey
				""")
				.param("retryNotBefore", java.sql.Timestamp.from(FixedClockTestConfiguration.NOW))
				.param("idempotencyKey", mentions.idempotencyKey())
				.update();
		var retry = archiveLedger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		assertThat(archiveLedger.markStaged(mentions.idempotencyKey(), retry.token(), staged(mentions)))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(archiveLedger.findRunByUpdateTime(updateTime).orElseThrow().status())
				.isEqualTo(IngestionRunStatus.STAGED);
		assertThat(archiveLedger.findCompletionProgress(ArchiveType.TRANSLATION_MENTIONS)).contains(updateTime);
	}

	@Test
	@DisplayName("Параллельное завершение архивов атомарно переводит запуск в STAGED")
	void concurrentArchiveCompletionRecalculatesRunToStaged() throws Exception {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		DiscoveredUpdate update = update(updateTime);
		archiveLedger.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null);
		DiscoveredArchive events = archive(update, ArchiveType.TRANSLATION_EVENTS);
		DiscoveredArchive mentions = archive(update, ArchiveType.TRANSLATION_MENTIONS);
		var eventAttempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		var mentionAttempt = archiveLedger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();

		var transitions = runConcurrently(
				() -> archiveLedger.markStaged(events.idempotencyKey(), eventAttempt.token(), staged(events)),
				() -> archiveLedger.markStaged(mentions.idempotencyKey(), mentionAttempt.token(), staged(mentions))
		);

		assertThat(transitions).containsExactlyInAnyOrder(
				AttemptTransitionResult.APPLIED,
				AttemptTransitionResult.APPLIED);
		assertThat(archiveLedger.findRunByUpdateTime(updateTime).orElseThrow().status())
				.isEqualTo(IngestionRunStatus.STAGED);
	}

	@Test
	@DisplayName("Неповторяемая ошибка сохраняется и не захватывается автоматически")
	void nonRetryableFailurePreventsAutomaticClaim() {
		DiscoveredArchive events = update(Instant.parse("2026-07-20T12:00:00Z")).archives().getFirst();
		archiveLedger.registerDiscoveredUpdate(update(events.sourceUpdateTime()), FirstRunPolicy.LATEST, null);
		var attempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		IngestionFailure failure = new IngestionFailure(
				IngestionErrorCode.STAGING_ARTIFACT_CONFLICT,
				false);

		assertThat(archiveLedger.markFailed(events.idempotencyKey(), attempt.token(), failure))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15))).isEmpty();
		assertThat(archiveLedger.findByIdempotencyKey(events.idempotencyKey()).orElseThrow().failure())
				.satisfies(recorded -> {
					assertThat(recorded.failure()).isEqualTo(failure);
					assertThat(recorded.failure().retryable()).isFalse();
					assertThat(recorded.occurredAt()).isEqualTo(FixedClockTestConfiguration.NOW);
				});
	}

	@Test
	@DisplayName("При одинаковом времени последняя ошибка run выбирается по стабильному ключу архива")
	void latestRunFailureUsesStableArchiveKeyAsTimestampTieBreaker() {
		Instant updateTime = Instant.parse("2026-07-20T12:00:00Z");
		DiscoveredUpdate canonicalUpdate = update(updateTime);
		DiscoveredArchive events = archive(canonicalUpdate, ArchiveType.TRANSLATION_EVENTS);
		DiscoveredArchive mentions = archive(canonicalUpdate, ArchiveType.TRANSLATION_MENTIONS);
		DiscoveredUpdate reverseRegistrationOrder = new DiscoveredUpdate(
				updateTime,
				List.of(mentions, events),
				List.of());
		archiveLedger.registerDiscoveredUpdate(reverseRegistrationOrder, FirstRunPolicy.LATEST, null);

		var eventAttempt = archiveLedger.claimArchive(events.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		var mentionAttempt = archiveLedger.claimArchive(mentions.idempotencyKey(), Duration.ofMinutes(15)).orElseThrow();
		IngestionFailure eventFailure = new IngestionFailure(
				IngestionErrorCode.DOWNLOAD_MD5_MISMATCH,
				true);
		IngestionFailure mentionFailure = new IngestionFailure(
				IngestionErrorCode.STAGING_ARTIFACT_CONFLICT,
				false);
		assertThat(archiveLedger.markFailed(events.idempotencyKey(), eventAttempt.token(), eventFailure))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(archiveLedger.markFailed(mentions.idempotencyKey(), mentionAttempt.token(), mentionFailure))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		IngestionFailure expectedFailure = events.idempotencyKey().compareTo(mentions.idempotencyKey()) < 0
				? eventFailure
				: mentionFailure;

		assertThat(jdbcClient.sql("""
				select count(distinct failed_at)
				from ingestion_archives
				where status = 'FAILED'
				""").query(Integer.class).single()).isEqualTo(1);
		assertThat(archiveLedger.findRunByUpdateTime(updateTime).orElseThrow().lastFailure())
				.isNotNull()
				.satisfies(recorded -> {
					assertThat(recorded.failure()).isEqualTo(expectedFailure);
					assertThat(recorded.occurredAt()).isEqualTo(FixedClockTestConfiguration.NOW);
				});
	}

	private static <T> List<T> runConcurrently(Callable<T> first, Callable<T> second) throws Exception {
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
					secondResult.get(30, TimeUnit.SECONDS));
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

	private static DiscoveredArchive archive(DiscoveredUpdate update, ArchiveType type) {
		return update.archives().stream()
				.filter(archive -> archive.archiveType() == type)
				.findFirst()
				.orElseThrow();
	}

	private static StagedArchive staged(DiscoveredArchive archive) {
		return new StagedArchive(
				Path.of("staging", archive.archiveName()),
				Path.of("staging", archive.archiveName().replaceFirst("\\.zip$", "")),
				archive.expectedSizeBytes(),
				archive.expectedMd5()
		);
	}

}
