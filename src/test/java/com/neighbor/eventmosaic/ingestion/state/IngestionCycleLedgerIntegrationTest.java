package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleState;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleStatus;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.JdbcProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionException;

@Import(PostgreSqlTestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("PostgreSQL разрешает выполнять цикл загрузки только одному владельцу")
class IngestionCycleLedgerIntegrationTest {

	private static final String SOURCE_NAME = GdeltSourceContract.SOURCE_NAME;
	private static final Duration LEASE = Duration.ofMinutes(15);

	@Autowired
	private IngestionCycleLedger cycleLedger;

	@Autowired
	private JdbcClient jdbcClient;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private JdbcIngestionCycleRepository cycleRepository;

	@Autowired
	private PlatformTransactionManager transactionManager;

	@Autowired
	private JdbcProperties jdbcProperties;

	@BeforeEach
	void cleanCycleState() {
		jdbcClient.sql("truncate table ingestion_cycle_state").update();
	}

	@Test
	@DisplayName("Настройки базы ограничивают ожидание соединения и запросов без общих параметров сеанса")
	void applicationJdbcTimeoutsAreBoundFromOneConfiguration() throws Exception {
		HikariDataSource hikari = dataSource.unwrap(HikariDataSource.class);

		assertThat(jdbcProperties.getTemplate().getQueryTimeout())
				.isEqualTo(Duration.ofSeconds(30));
		assertThat(hikari.getConnectionTimeout()).isEqualTo(30_000);
		assertThat(hikari.getDataSourceProperties())
				.containsEntry("connectTimeout", "10")
				.containsEntry("socketTimeout", "45")
				.containsEntry("cancelSignalTimeout", "5")
				.doesNotContainKey("options");
	}

	@Test
	@DisplayName("Первый захват разрешает загрузку на срок по часам PostgreSQL и запрещает повторный захват")
	void firstClaimUsesPostgreSqlTime() {
		Instant beforeClaim = databaseNow();

		IngestionCycleOwnership ownership = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		Instant afterClaim = databaseNow();

		assertThat(ownership.fencingEpoch()).isEqualTo(1);
		assertThat(ownership.leaseExpiresAt())
				.isBetween(beforeClaim.plus(LEASE), afterClaim.plus(LEASE));
		assertThat(cycleLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		assertThat(cycleLedger.remainingLease(ownership)).hasValueSatisfying(remaining -> {
			assertThat(remaining).isPositive();
			assertThat(remaining).isLessThanOrEqualTo(LEASE);
		});
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionCycleStatus.ACTIVE);
					assertThat(state.ownership()).isEqualTo(ownership);
					assertThat(state.stateVersion()).isEqualTo(1);
				});
	}

	@Test
	@DisplayName("Из двух одновременных попыток только одна получает право выполнять загрузку")
	void concurrentClaimsIssueOneOwnership() throws Exception {
		List<Optional<IngestionCycleOwnership>> claims = runConcurrently(
				() -> cycleLedger.claim(SOURCE_NAME, LEASE),
				() -> cycleLedger.claim(SOURCE_NAME, LEASE));

		assertThat(claims).filteredOn(Optional::isPresent).hasSize(1);
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().fencingEpoch())
				.isEqualTo(1);
	}

	@Test
	@DisplayName("После истечения срока новый владелец получает цикл, а прежний не может изменить его состояние")
	void expiredLeaseFencesPreviousOwner() {
		IngestionCycleOwnership first = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		expireLease();
		assertThat(cycleLedger.remainingLease(first)).isEmpty();

		IngestionCycleOwnership successor = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		IngestionCycleState successorState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();

		assertThat(successor.token()).isNotEqualTo(first.token());
		assertThat(successor.fencingEpoch()).isEqualTo(2);
		assertThat(cycleLedger.complete(first, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.remainingLease(first)).isEmpty();
		assertThat(cycleLedger.remainingLease(successor)).hasValueSatisfying(remaining ->
				assertThat(remaining).isPositive());
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME)).contains(successorState);
	}

	@Test
	@DisplayName("Подмена ключа владельца при прежнем номере захвата не дает права завершить загрузку")
	void forgedTokenCannotChangeCurrentCycle() {
		IngestionCycleOwnership current = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		IngestionCycleOwnership forged = new IngestionCycleOwnership(
				current.sourceName(),
				UUID.randomUUID(),
				current.fencingEpoch(),
				current.leaseExpiresAt());

		assertForgedOwnershipRejected(forged, current);
	}

	@Test
	@DisplayName("Подмена номера захвата при прежнем ключе владельца не дает права завершить загрузку")
	void forgedEpochCannotChangeCurrentCycle() {
		IngestionCycleOwnership current = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		IngestionCycleOwnership forged = new IngestionCycleOwnership(
				current.sourceName(),
				current.token(),
				Math.addExact(current.fencingEpoch(), 1),
				current.leaseExpiresAt());

		assertForgedOwnershipRejected(forged, current);
	}

	@Test
	@DisplayName("Завершение сохраняет результат и освобождает цикл, а повторное завершение ничего не меняет")
	void completionRecordsOutcomeAndReleasesCycleOnce() {
		IngestionCycleOwnership claimed = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		IngestionCycleState activeState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		Instant beforeCompletion = databaseNow();
		assertThat(cycleLedger.complete(claimed, IngestionCycleOutcome.UNCHANGED))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		Instant afterCompletion = databaseNow();

		IngestionCycleState completedState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		assertThat(completedState)
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionCycleStatus.IDLE);
					assertThat(state.ownership()).isNull();
					assertThat(state.lastProgressAt()).isNull();
					assertThat(state.lastStartedAt()).isEqualTo(activeState.lastStartedAt());
					assertThat(state.createdAt()).isEqualTo(activeState.createdAt());
					assertThat(state.fencingEpoch()).isEqualTo(claimed.fencingEpoch());
					assertThat(state.lastTerminalAt()).isBetween(beforeCompletion, afterCompletion);
					assertThat(state.updatedAt()).isEqualTo(state.lastTerminalAt());
					assertThat(state.lastOutcome()).isEqualTo(IngestionCycleOutcome.UNCHANGED);
					assertThat(state.stateVersion()).isEqualTo(activeState.stateVersion() + 1);
				});
		assertThat(cycleLedger.complete(claimed, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.remainingLease(claimed)).isEmpty();
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME)).contains(completedState);
	}

	@Test
	@DisplayName("После завершения следующий цикл получает новый ключ и больший номер захвата")
	void completionAllowsNextClaimWithHigherEpoch() {
		IngestionCycleOwnership first = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(cycleLedger.complete(first, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		IngestionCycleOwnership second = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(second.fencingEpoch()).isEqualTo(2);
		assertThat(second.token()).isNotEqualTo(first.token());
	}

	@ParameterizedTest(name = "Сохраненное время прогресса присутствует: {0}")
	@ValueSource(booleans = {false, true})
	@DisplayName("Прежнее время прогресса читается без изменений и сбрасывается при новом цикле")
	void storedProgressRemainsReadableAndResetsOnNextClaim(boolean hasProgress) {
		IngestionCycleOwnership ownership = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		IngestionCycleState initialState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		Instant progressAt = hasProgress ? initialState.lastStartedAt() : null;
		assertThat(jdbcClient.sql("""
				update ingestion_cycle_state
				set last_progress_at = :progressAt
				where source_name = :sourceName
				""")
				.param("progressAt", progressAt == null ? null : Timestamp.from(progressAt),
						Types.TIMESTAMP)
				.param("sourceName", SOURCE_NAME)
				.update()).isEqualTo(1);

		IngestionCycleState storedState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		assertThat(storedState.lastProgressAt()).isEqualTo(progressAt);
		assertThat(storedState).usingRecursiveComparison()
				.ignoringFields("lastProgressAt")
				.isEqualTo(initialState);
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME)).contains(storedState);

		assertThat(cycleLedger.complete(ownership, IngestionCycleOutcome.UNCHANGED))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().lastProgressAt())
				.isEqualTo(progressAt);
		IngestionCycleOwnership next = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		assertThat(next.token()).isNotEqualTo(ownership.token());
		assertThat(next.fencingEpoch()).isEqualTo(ownership.fencingEpoch() + 1);
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.ownership()).isEqualTo(next);
					assertThat(state.lastProgressAt()).isNull();
					assertThat(state.lastOutcome()).isEqualTo(IngestionCycleOutcome.UNCHANGED);
				});
	}

	@Test
	@DisplayName("Действующий владелец может один раз завершить цикл с результатом потери владения")
	void currentOwnerCanRecordOwnershipLostOutcome() {
		IngestionCycleOwnership current = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();

		assertThat(cycleLedger.complete(current, IngestionCycleOutcome.OWNERSHIP_LOST))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionCycleStatus.IDLE);
					assertThat(state.ownership()).isNull();
					assertThat(state.lastOutcome())
							.isEqualTo(IngestionCycleOutcome.OWNERSHIP_LOST);
				});
		assertThat(cycleLedger.complete(current, IngestionCycleOutcome.OWNERSHIP_LOST))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Если срок владения истек во время ожидания блокировки, завершение не меняет цикл")
	void blockedCompletionUsesDatabaseTimeAfterRowLock() throws Exception {
		assertBlockedCompletionRejected();
	}

	@Test
	@DisplayName("Ожидание заблокированной строки прерывается локальным секундным лимитом")
	void blockedClaimUsesBudgetedTransactionTimeout() throws Exception {
		cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		JdbcIngestionCycleLedger oneSecondLedger = new JdbcIngestionCycleLedger(
				cycleRepository,
				transactionManager,
				Duration.ofSeconds(1));
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (Connection holder = dataSource.getConnection()) {
			holder.setAutoCommit(false);
			lockCycleRow(holder);
			long startedAt = System.nanoTime();
			Future<Optional<IngestionCycleOwnership>> result = executor.submit(() ->
					oneSecondLedger.claim(
							SOURCE_NAME,
							LEASE,
							OperationBudget.start(Duration.ofSeconds(5))));

			Throwable failure = catchThrowable(() -> result.get(5, TimeUnit.SECONDS));
			Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

			assertThat(failure).isInstanceOf(java.util.concurrent.ExecutionException.class);
			assertThat(failure.getCause())
					.isInstanceOfAny(DataAccessException.class, TransactionException.class);
			assertThat(elapsed).isLessThan(Duration.ofSeconds(3));
			holder.rollback();
		}
		finally {
			executor.shutdownNow();
		}

		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionCycleStatus.ACTIVE);
					assertThat(state.fencingEpoch()).isEqualTo(1);
				});
	}

	private void assertBlockedCompletionRejected() throws Exception {
		IngestionCycleOwnership ownership = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		IngestionCycleState initialState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try (Connection holder = dataSource.getConnection();
				Connection worker = dataSource.getConnection()) {
			holder.setAutoCommit(false);
			lockCycleRow(holder);
			JdbcClient workerClient = JdbcClient.create(
					new SingleConnectionDataSource(worker, true));
			int workerPid = workerClient.sql("select pg_backend_pid()")
					.query(Integer.class)
					.single();
			JdbcIngestionCycleRepository workerRepository =
					new JdbcIngestionCycleRepository(workerClient);
			Future<AttemptTransitionResult> result = executor.submit(() ->
					workerRepository.complete(ownership, IngestionCycleOutcome.COMPLETED));

			awaitBlockedRowLock(workerPid);
			Instant expiresAt = databaseNow().plusMillis(200);
			setLeaseExpiry(holder, expiresAt);
			awaitDatabaseTimeAfter(expiresAt);
			holder.commit();

			assertThat(result.get(10, TimeUnit.SECONDS))
					.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
			assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
					.satisfies(state -> {
						assertThat(state).usingRecursiveComparison()
								.ignoringFields("ownership")
								.isEqualTo(initialState);
						assertThat(state.ownership()).isEqualTo(new IngestionCycleOwnership(
								SOURCE_NAME, ownership.token(), ownership.fencingEpoch(), expiresAt));
					});
		}
		finally {
			executor.shutdownNow();
		}
	}

	private static void lockCycleRow(Connection connection) throws Exception {
		try (PreparedStatement statement = connection.prepareStatement("""
				select source_name
				from ingestion_cycle_state
				where source_name = ?
				for update
				""")) {
			statement.setString(1, SOURCE_NAME);
			statement.executeQuery().close();
		}
	}

	private static void setLeaseExpiry(Connection connection, Instant expiresAt)
			throws Exception {
		try (PreparedStatement statement = connection.prepareStatement("""
				update ingestion_cycle_state
				set lease_expires_at = ?
				where source_name = ?
				""")) {
			statement.setTimestamp(1, Timestamp.from(expiresAt));
			statement.setString(2, SOURCE_NAME);
			assertThat(statement.executeUpdate()).isEqualTo(1);
		}
	}

	private void awaitBlockedRowLock(int workerPid) throws Exception {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			String waitType = jdbcClient.sql("""
					select coalesce(wait_event_type, '')
					from pg_stat_activity
					where pid = :pid
					""")
					.param("pid", workerPid)
					.query(String.class)
					.single();
			if ("Lock".equals(waitType)) {
				return;
			}
			TimeUnit.MILLISECONDS.sleep(10);
		}
		throw new IllegalStateException("Worker did not wait for the cycle row lock");
	}

	private void awaitDatabaseTimeAfter(Instant instant) throws Exception {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (System.nanoTime() < deadline) {
			if (databaseNow().isAfter(instant)) {
				return;
			}
			TimeUnit.MILLISECONDS.sleep(10);
		}
		throw new IllegalStateException("PostgreSQL time did not pass the test lease expiry");
	}

	private void assertForgedOwnershipRejected(
			IngestionCycleOwnership forged,
			IngestionCycleOwnership current
	) {
		IngestionCycleState initialState = cycleLedger.findBySourceName(SOURCE_NAME)
				.orElseThrow();
		assertThat(cycleLedger.remainingLease(forged)).isEmpty();
		assertThat(cycleLedger.complete(forged, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.remainingLease(current)).hasValueSatisfying(remaining ->
				assertThat(remaining).isPositive());
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME)).contains(initialState);
	}

	private void expireLease() {
		jdbcClient.sql("""
				update ingestion_cycle_state
				set last_started_at = clock_timestamp() - interval '2 seconds',
				    lease_expires_at = clock_timestamp() - interval '1 second'
				where source_name = :sourceName
				""")
				.param("sourceName", SOURCE_NAME)
				.update();
	}

	private Instant databaseNow() {
		return jdbcClient.sql("select clock_timestamp()")
				.query((resultSet, rowNumber) -> resultSet.getTimestamp(1).toInstant())
				.single();
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
					secondResult.get(30, TimeUnit.SECONDS));
		}
		finally {
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
