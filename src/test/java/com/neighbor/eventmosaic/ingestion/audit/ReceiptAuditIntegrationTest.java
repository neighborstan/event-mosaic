package com.neighbor.eventmosaic.ingestion.audit;

import static com.neighbor.eventmosaic.ingestion.GdeltTestFixtures.update;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.recovery.RecentRecoveryPlanLedger;
import com.neighbor.eventmosaic.ingestion.recovery.RecentWindowPlan;
import com.neighbor.eventmosaic.processing.api.ProcessingFingerprintFactory;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest
@Import(PostgreSqlTestcontainersConfiguration.class)
@DisplayName("Фоновая проверка сохраненных архивов с отдельным журналом PostgreSQL")
class ReceiptAuditIntegrationTest {

	private static final Instant START = Instant.parse("2026-07-22T13:00:00Z");
	private static final Duration TICK = Duration.ofMinutes(15);
	private static final String PARTITION = "p20260720";
	private static final ArchiveIdentityDigest DIGEST = new ArchiveIdentityDigest("a".repeat(64));
	@Autowired private JdbcReceiptAuditRepository repository;
	@Autowired private JdbcClient jdbc;
	@Autowired private ArchiveProcessingLedger processing;
	@Autowired private IngestionArchiveLedger archives;
	@Autowired private RecentRecoveryPlanLedger plans;
	@Autowired private GdeltIngestionProperties properties;
	@Autowired private BackendDataProperties backendProperties;
	@Autowired private ProcessingFingerprintFactory fingerprints;
	@Autowired private PlatformTransactionManager transactionManager;
	@MockitoBean private Clock clock;
	private GdeltIndexWriter writer;
	private ReceiptAuditService service;
	private Instant now;
	private UUID generationUuid;
	private final List<String> checkedKeys = new ArrayList<>();

	@BeforeEach
	void prepare() {
		setTime(START);
		when(clock.getZone()).thenReturn(ZoneOffset.UTC);
		jdbc.sql("""
				truncate ingestion_source_state, ingestion_runs, index_logical_partitions restart identity cascade
				""").update();
		generationUuid = UUID.randomUUID();
		jdbc.sql("""
				insert into index_logical_partitions(partition_key, partition_start_at, partition_end_at, partition_interval, state_version)
				values ('p20260720', '2026-07-20T00:00:00Z', '2026-07-27T00:00:00Z', 'P7D', 1)
				""").update();
		insertGeneration(1, generationUuid);
		writer = mock(GdeltIndexWriter.class);
		matchingWriter();
		service = new ReceiptAuditService(repository, writer, backendProperties, properties);
		checkedKeys.clear();
	}

	@Test
	@DisplayName("Проверяет все 196 архивов неподвижного окна за 24 с половиной часа и не обращается к ним каждую минуту")
	void sweepsFrozenWindowWithoutFullScanPerPoll() {
		RecentWindowPlan plan = plan(now);
		plans.activate(update(plan.sourceFrontier()), plan);
		for (Instant slot : plan.targetUpdateTimes()) {
			seedPair(slot);
		}
		ageReceipts();
		for (int tick = 0; tick < 98; tick++) {
			setTime(START.plus(TICK.multipliedBy(tick)));
			assertThat(service.auditDue(budget()).checked()).isEqualTo(2);
			assertThat(service.auditDue(budget()).checked()).isZero();
		}
		assertThat(checkedKeys).hasSize(196).doesNotHaveDuplicates();
		assertThat(Duration.between(START, now).plus(TICK)).isEqualTo(Duration.ofHours(24).plusMinutes(30));
	}

	@Test
	@DisplayName("В непрерывно движущемся окне проверяет старейшие архивы до их выхода из окна и не теряет новые пары")
	void sweepsContinuouslyRollingWindow() {
		RecentWindowPlan initial = plan(now);
		plans.activate(update(initial.sourceFrontier()), initial);
		for (Instant slot : initial.targetUpdateTimes()) {
			seedPair(slot);
		}
		ageReceipts();
		Set<String> audited = new HashSet<>();
		for (int tick = 0; tick < 196; tick++) {
			setTime(START.plus(TICK.multipliedBy(tick)));
			RecentWindowPlan current = plan(now);
			if (tick > 0) {
				var expired = update(current.windowFrom().minus(TICK));
				assertThat(audited).containsAll(expired.archives().stream().map(DiscoveredArchive::idempotencyKey).toList());
				plans.activate(update(current.sourceFrontier()), current);
				seedPair(current.sourceFrontier());
			}
			assertThat(service.auditDue(budget()).checked()).isEqualTo(2);
			audited.addAll(checkedKeys);
		}
		assertThat(checkedKeys).hasSize(392).doesNotHaveDuplicates();
	}

	@Test
	@DisplayName("Недостающие документы открывают повторную обработку только проверенных архивов, а лишние требуют восстановления раздела")
	void shortagesReplayGraduallyAndSurplusRequestsRepair() {
		seedThreePairs();
		doAnswer(invocation -> {
			ArchiveReceiptQuery query = invocation.getArgument(0);
			return verification(query.kind(), ArchiveReceiptStatus.SHORTAGE);
		}).when(writer).verifyReceipt(any(), any());
		assertThat(service.auditDue(budget()).mismatched()).isEqualTo(2);
		assertThat(countStatus("FAILED")).isEqualTo(2);
		assertThat(countStatus("INDEXED")).isEqualTo(4);
		setTime(now.plus(TICK));
		doAnswer(invocation -> {
			ArchiveReceiptQuery query = invocation.getArgument(0);
			return verification(query.kind(), ArchiveReceiptStatus.SURPLUS);
		}).when(writer).verifyReceipt(any(), any());
		assertThat(service.auditDue(budget()).mismatched()).isOne();
		assertThat(jdbc.sql("select repair_cause from index_logical_partitions").query(String.class).single()).isEqualTo("SURPLUS");
		assertThat(jdbc.sql("select count(*) from ingestion_archive_processing where last_error_code = 'INDEX_RECEIPT_SURPLUS' and not last_error_retryable")
				.query(Integer.class).single()).isOne();
	}

	@Test
	@DisplayName("Одинаковое число документов с другим составом открывает повторную обработку")
	void identityMismatchRequestsReplay() {
		seedThreePairs();
		doAnswer(invocation -> {
			ArchiveReceiptQuery query = invocation.getArgument(0);
			return verification(query.kind(), ArchiveReceiptStatus.IDENTITY_MISMATCH);
		}).when(writer).verifyReceipt(any(), any());
		assertThat(service.auditDue(budget()).mismatched()).isEqualTo(2);
		assertThat(jdbc.sql("select count(*) from ingestion_archive_processing where last_error_code = 'INDEX_RECEIPT_MISMATCH' and last_error_retryable")
				.query(Integer.class).single()).isEqualTo(2);
	}

	@Test
	@DisplayName("Потеря физического индекса открывает восстановление раздела, сохраняя предыдущую квитанцию архива")
	void missingGenerationRequestsRepair() {
		seedThreePairs();
		doThrow(new IndexTargetUnavailableException(IndexTargetUnavailableReason.MISSING)).when(writer).verifyReceipt(any(), any());
		assertThat(service.auditDue(budget()).mismatched()).isOne();
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		assertThat(jdbc.sql("select repair_cause from index_logical_partitions").query(String.class).single()).isEqualTo("MISSING_CURRENT");
		assertThat(auditOutcome()).isEqualTo("MISSING_GENERATION");
		setTime(now.plus(TICK));
		assertThat(service.auditDue(budget()).checked()).isZero();
	}

	@Test
	@DisplayName("Ошибка чтения Elasticsearch завершает пакет после одной попытки и оставляет все архивы успешно обработанными")
	void serviceKeepsIndexedOnInfrastructureFailure() {
		seedThreePairs();
		doThrow(new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE)).when(writer).verifyReceipt(any(), any());
		assertThat(service.auditDue(budget()).infrastructureFailure()).isTrue();
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		assertThat(auditOutcome()).isEqualTo("INFRASTRUCTURE_FAILURE");
		assertThat(repository.claim(null)).isEmpty();
	}

	@Test
	@DisplayName("Недоступность Elasticsearch не меняет успешную обработку и после трех повторов сохраняет паузу и новую последовательность")
	void infrastructureFailureUsesOwnCooldownAcrossRestart() {
		seedThreePairs();
		var before = jdbc.sql("select sum(total_attempt_count) from ingestion_archive_processing").query(Long.class).single();
		for (int attempt = 0; attempt < 4; attempt++) {
			var claim = repository.claim(null).orElseThrow();
			repository.fail(claim, "INDEXING_UNAVAILABLE");
			assertThat(repository.claim(null)).isEmpty();
			if (attempt < 3) {
				setTime(now.plus(TICK));
			}
		}
		assertThat(jdbc.sql("select retry_sequence from ingestion_receipt_audit_state").query(Long.class).single()).isOne();
		Instant exhausted = now;
		assertThat(jdbc.sql("select retry_not_before from ingestion_receipt_audit_state").query(Timestamp.class).single().toInstant())
				.isEqualTo(exhausted.plus(backendProperties.retry().maximumDelay()));
		setTime(exhausted.plus(backendProperties.retry().maximumDelay()).minusNanos(1));
		assertThat(repository.claim(null)).isEmpty();
		setTime(exhausted.plus(backendProperties.retry().maximumDelay()));
		var restartedService = new ReceiptAuditService(repository, writer, backendProperties, properties);
		assertThat(restartedService.auditDue(budget()).checked()).isEqualTo(2);
		assertThat(jdbc.sql("select retry_sequence from ingestion_receipt_audit_state").query(Long.class).single()).isEqualTo(2);
		assertThat(jdbc.sql("select last_exhausted_at from ingestion_receipt_audit_state").query(Timestamp.class).single().toInstant()).isEqualTo(exhausted);
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		assertThat(jdbc.sql("select sum(total_attempt_count) from ingestion_archive_processing").query(Long.class).single()).isEqualTo(before);
	}

	@Test
	@DisplayName("Аварийная остановка на последнем повторе сохраняет полную паузу после истечения срока и причину исчерпания")
	void exhaustedLeaseWaitsForCooldownAndRetainsEvidence() {
		seedThreePairs();
		for (int retry = 0; retry < 3; retry++) {
			var claim = repository.claim(null).orElseThrow();
			repository.fail(claim, "INDEXING_UNAVAILABLE");
			setTime(now.plus(TICK));
		}
		repository.claim(null).orElseThrow();
		Instant expiredAt = now.plus(properties.continuity().recoveryLease());
		setTime(expiredAt);
		assertThat(repository.claim(null)).isEmpty();
		setTime(expiredAt.plus(backendProperties.retry().maximumDelay()).minusNanos(1));
		assertThat(repository.claim(null)).isEmpty();
		setTime(expiredAt.plus(backendProperties.retry().maximumDelay()));
		assertThat(repository.claim(null)).isPresent();
		assertThat(jdbc.sql("select retry_sequence from ingestion_receipt_audit_state").query(Long.class).single()).isEqualTo(2);
		assertThat(jdbc.sql("select last_exhausted_at from ingestion_receipt_audit_state").query(Timestamp.class).single().toInstant()).isEqualTo(expiredAt);
		assertThat(jdbc.sql("select last_exhausted_error_code from ingestion_receipt_audit_state").query(String.class).single()).isEqualTo("ATTEMPT_LEASE_EXPIRED");
	}

	@Test
	@DisplayName("Запоздалый результат после смены поколения или перехвата проверки не меняет данные нового владельца")
	void rejectsCutoverAndOldAuditToken() {
		seedThreePairs();
		var first = repository.claim(null).orElseThrow();
		assertThat(repository.claim(null)).isEmpty();
		jdbc.sql("update index_generations set state = 'SUPERSEDED', superseded_at = :now where state = 'ACTIVE'")
				.param("now", Timestamp.from(now)).update();
		jdbc.sql("update index_logical_partitions set state_version = state_version + 1").update();
		insertGeneration(2, UUID.randomUUID());
		assertThat(repository.complete(first, verification(first.currentTarget().indexKind(), ArchiveReceiptStatus.SHORTAGE))).isEmpty();
		assertThat(auditOutcome()).isEqualTo("STALE_BINDING");
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		setTime(now.plus(TICK));
		var expired = repository.claim(null).orElseThrow();
		setTime(now.plus(properties.continuity().recoveryLease()));
		var successor = repository.claim(null).orElseThrow();
		assertThat(repository.complete(expired, verification(expired.currentTarget().indexKind(), ArchiveReceiptStatus.SHORTAGE))).isEmpty();
		assertThat(repository.complete(successor, verification(successor.currentTarget().indexKind(), ArchiveReceiptStatus.MATCHED))).isPresent();
		assertThat(countStatus("INDEXED")).isEqualTo(6);
	}

	@Test
	@DisplayName("Прерывание после ответа Elasticsearch не подтверждает результат и сохраняет возможность продолжить после истечения срока")
	void interruptionLeavesLeaseRecoverable() {
		seedThreePairs();
		doAnswer(invocation -> {
			Thread.currentThread().interrupt();
			ArchiveReceiptQuery query = invocation.getArgument(0);
			return verification(query.kind(), ArchiveReceiptStatus.MATCHED);
		}).when(writer).verifyReceipt(any(), any());
		try {
			assertThatThrownBy(() -> service.auditDue(budget())).isInstanceOf(RuntimeException.class);
		} finally {
			Thread.interrupted();
		}
		assertThat(jdbc.sql("select status from ingestion_receipt_audit_state").query(String.class).single()).isEqualTo("AUDITING");
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		setTime(now.plus(properties.continuity().recoveryLease()));
		matchingWriter();
		assertThat(service.auditDue(budget()).checked()).isEqualTo(2);
	}

	@ParameterizedTest
	@EnumSource(AuditAction.class)
	@DisplayName("Параллельная смена поколения завершается без взаимной блокировки с получением и сохранением проверки")
	void concurrentCutoverUsesSameLockOrder(AuditAction action) throws Exception {
		seedThreePairs();
		ReceiptAuditClaim claim = action == AuditAction.CLAIM ? null : repository.claim(null).orElseThrow();
		var partitionLocked = new CountDownLatch(1);
		var finishCutover = new CountDownLatch(1);
		var transactions = new TransactionTemplate(transactionManager);
		try (var workers = Executors.newFixedThreadPool(2)) {
			var cutover = workers.submit(() -> transactions.execute(_ -> {
				jdbc.sql("select partition_key from index_logical_partitions where partition_key = :partition for update")
						.param("partition", PARTITION).query(String.class).single();
				partitionLocked.countDown();
				await(finishCutover);
				jdbc.sql("update index_logical_partitions set state_version = state_version + 1").update();
				jdbc.sql("update ingestion_archive_processing set state_version = state_version + 1").update();
				return true;
			}));
			assertThat(partitionLocked.await(10, TimeUnit.SECONDS)).isTrue();
			var audit = workers.submit(() -> transactions.execute(_ -> {
				jdbc.sql("set local application_name = 'receipt-audit-lock-test'").update();
				switch (action) {
					case CLAIM -> assertThat(repository.claim(null)).isPresent();
					case COMPLETE -> assertThat(repository.complete(claim,
							verification(claim.currentTarget().indexKind(), ArchiveReceiptStatus.MATCHED))).isEmpty();
					case MISSING -> assertThat(repository.missing(claim, IndexTargetUnavailableReason.MISSING)).isEmpty();
					case FAILURE -> repository.fail(claim, "INDEXING_UNAVAILABLE");
				}
				return true;
			}));
			try {
				long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
				boolean waiting = false;
				while (!waiting && System.nanoTime() < waitUntil) {
					waiting = jdbc.sql("""
							select exists(select 1 from pg_stat_activity
							where application_name = 'receipt-audit-lock-test' and wait_event_type = 'Lock')
							""").query(Boolean.class).single();
				}
				assertThat(waiting).isTrue();
			} finally {
				finishCutover.countDown();
			}
			assertThat(cutover.get(10, TimeUnit.SECONDS)).isTrue();
			assertThat(audit.get(10, TimeUnit.SECONDS)).isTrue();
		}
		assertThat(countStatus("INDEXED")).isEqualTo(6);
		if (action != AuditAction.CLAIM) {
			assertThat(auditOutcome()).isEqualTo("STALE_BINDING");
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			if (!latch.await(15, TimeUnit.SECONDS)) {
				throw new AssertionError("Проверка не дошла до ожидания блокировки раздела");
			}
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new AssertionError(exception);
		}
	}

	private enum AuditAction {
		CLAIM, COMPLETE, MISSING, FAILURE
	}

	private void matchingWriter() {
		doAnswer(invocation -> {
			ArchiveReceiptQuery query = invocation.getArgument(0);
			checkedKeys.add(query.sourceArchiveKey());
			return verification(query.kind(), ArchiveReceiptStatus.MATCHED);
		}).when(writer).verifyReceipt(any(), any());
	}

	private void seedThreePairs() {
		RecentWindowPlan plan = plan(now);
		plans.activate(update(plan.sourceFrontier()), plan);
		seedPair(plan.sourceFrontier().minus(TICK.multipliedBy(2)));
		seedPair(plan.sourceFrontier().minus(TICK));
		seedPair(plan.sourceFrontier());
		ageReceipts();
	}

	private void seedPair(Instant slot) {
		var update = update(slot);
		archives.registerDiscoveredUpdate(update, FirstRunPolicy.LATEST, null);
		for (DiscoveredArchive archive : update.archives()) {
			var acquired = archives.claimArchive(archive.idempotencyKey(), TICK).orElseThrow();
			archives.markStaged(archive.idempotencyKey(), acquired.token(), new StagedArchive(
					Path.of("staging", archive.archiveName()), Path.of("staging", archive.archiveName() + ".csv"),
					archive.expectedSizeBytes(), archive.expectedMd5()));
			GdeltArchiveKind kind = archive.archiveType() == ArchiveType.TRANSLATION_EVENTS
					? GdeltArchiveKind.TRANSLATION_EVENTS : GdeltArchiveKind.TRANSLATION_MENTIONS;
			processing.register(archive.idempotencyKey(), new ArchiveProcessingFingerprint(archive.expectedMd5(), "test-v1",
					fingerprints.create(archive.idempotencyKey(), kind, "test-v1", "test-v1")));
			var target = target(archive.archiveType());
			var attempt = processing.claim(archive.idempotencyKey(), target, TICK).claimedAttempt().orElseThrow();
			assertThat(processing.markIndexed(attempt, new ArchiveProcessingProgress(2, 0, 0, 2, 2, 0, 2, null),
					verification(target.indexKind(), ArchiveReceiptStatus.MATCHED))).isEqualTo(AttemptTransitionResult.APPLIED);
		}
	}

	private ArchiveProcessingTargetBinding target(ArchiveType archiveType) {
		GdeltIndexKind kind = archiveType == ArchiveType.TRANSLATION_EVENTS ? GdeltIndexKind.EVENT : GdeltIndexKind.MENTION;
		return new ArchiveProcessingTargetBinding(kind, PARTITION, 1, 1, generationUuid,
				(kind == GdeltIndexKind.EVENT ? "gdelt-events-v1-" : "gdelt-mentions-v1-") + PARTITION + "-g0001",
				kind == GdeltIndexKind.EVENT ? "event-uuid-1" : "mention-uuid-1");
	}

	private void insertGeneration(int number, UUID uuid) {
		jdbc.sql("""
				insert into index_generations(generation_uuid, partition_key, generation_number, state,
				    event_index_name, event_index_uuid, mention_index_name, mention_index_uuid, heartbeat_at, activated_at)
				values (:uuid, 'p20260720', :number, 'ACTIVE', :eventName, :eventUuid, :mentionName, :mentionUuid, :now, :now)
				""").param("uuid", uuid).param("number", number)
				.param("eventName", "gdelt-events-v1-p20260720-g000" + number).param("eventUuid", "event-uuid-" + number)
				.param("mentionName", "gdelt-mentions-v1-p20260720-g000" + number).param("mentionUuid", "mention-uuid-" + number)
				.param("now", Timestamp.from(now)).update();
	}

	private void ageReceipts() {
		jdbc.sql("update ingestion_archive_processing set receipt_verified_at = :old").param("old", Timestamp.from(now.minus(TICK))).update();
	}

	private int countStatus(String status) {
		return jdbc.sql("select count(*) from ingestion_archive_processing where status = :status").param("status", status).query(Integer.class).single();
	}

	private String auditOutcome() {
		return jdbc.sql("select last_outcome from ingestion_receipt_audit_state").query(String.class).single();
	}

	private void setTime(Instant instant) {
		now = instant;
		when(clock.instant()).thenReturn(instant);
	}

	private static OperationBudget budget() {
		return OperationBudget.start(Duration.ofMinutes(12));
	}

	private static RecentWindowPlan plan(Instant now) {
		Instant to = now.minus(TICK.multipliedBy(2));
		return RecentWindowPlan.fromBoundaries(to.minus(Duration.ofHours(24)), to, now.minus(TICK));
	}

	private static ArchiveReceiptVerification verification(GdeltIndexKind kind, ArchiveReceiptStatus status) {
		long actual = switch (status) {
			case SHORTAGE -> 1;
			case SURPLUS -> 3;
			default -> 2;
		};
		return new ArchiveReceiptVerification(kind, 2, actual, DIGEST,
				status == ArchiveReceiptStatus.MATCHED ? DIGEST : new ArchiveIdentityDigest("b".repeat(64)), status);
	}
}
