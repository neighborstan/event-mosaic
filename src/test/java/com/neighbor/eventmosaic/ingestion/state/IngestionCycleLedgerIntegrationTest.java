package com.neighbor.eventmosaic.ingestion.state;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleStatus;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
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
import java.util.function.BiFunction;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
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
@DisplayName("Общее владение одним ingestion cycle через PostgreSQL")
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
	@DisplayName("Общие JDBC-настройки ограничивают пул и запросы, но не задают statement options")
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
	@DisplayName("Первый claim выдает owner с lease от времени PostgreSQL")
	void firstClaimUsesPostgreSqlTime() {
		Instant beforeClaim = databaseNow();

		IngestionCycleOwnership ownership = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();
		Instant afterClaim = databaseNow();

		assertThat(ownership.fencingEpoch()).isEqualTo(1);
		assertThat(ownership.leaseExpiresAt())
				.isBetween(beforeClaim.plus(LEASE), afterClaim.plus(LEASE));
		assertThat(cycleLedger.claim(SOURCE_NAME, LEASE)).isEmpty();
		assertThat(cycleLedger.isCurrent(ownership)).isTrue();
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
	@DisplayName("Два одновременных claim выдают ownership только одному экземпляру")
	void concurrentClaimsIssueOneOwnership() throws Exception {
		List<Optional<IngestionCycleOwnership>> claims = runConcurrently(
				() -> cycleLedger.claim(SOURCE_NAME, LEASE),
				() -> cycleLedger.claim(SOURCE_NAME, LEASE));

		assertThat(claims).filteredOn(Optional::isPresent).hasSize(1);
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().fencingEpoch())
				.isEqualTo(1);
	}

	@Test
	@DisplayName("Истекший lease получает новый token и ограждает прежнего owner")
	void expiredLeaseFencesPreviousOwner() {
		IngestionCycleOwnership first = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
		expireLease();
		assertThat(cycleLedger.remainingLease(first)).isEmpty();

		IngestionCycleOwnership successor = cycleLedger.claim(SOURCE_NAME, LEASE)
				.orElseThrow();

		assertThat(successor.token()).isNotEqualTo(first.token());
		assertThat(successor.fencingEpoch()).isEqualTo(2);
		assertThat(cycleLedger.isCurrent(first)).isFalse();
		assertThat(cycleLedger.renew(first, LEASE)).isEmpty();
		assertThat(cycleLedger.markProgress(first))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.complete(first, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.release(first))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.remainingLease(first)).isEmpty();
		assertThat(cycleLedger.remainingLease(successor)).hasValueSatisfying(remaining ->
				assertThat(remaining).isPositive());
		assertThat(cycleLedger.isCurrent(successor)).isTrue();
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().ownership())
				.isEqualTo(successor);
	}

	@Test
	@DisplayName("Другой token при той же epoch не может изменить текущий cycle")
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
	@DisplayName("Другая epoch при том же token не может изменить текущий cycle")
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
	@DisplayName("Progress и renewal сохраняются только у owner, а completion освобождает cycle")
	void progressRenewalAndCompletionRemainFenced() {
		IngestionCycleOwnership claimed = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(cycleLedger.markProgress(claimed))
				.isEqualTo(AttemptTransitionResult.APPLIED);
		Instant beforeRenewal = databaseNow();
		IngestionCycleOwnership renewed = cycleLedger.renew(claimed, LEASE).orElseThrow();
		Instant afterRenewal = databaseNow();
		assertThat(renewed.token()).isEqualTo(claimed.token());
		assertThat(renewed.fencingEpoch()).isEqualTo(claimed.fencingEpoch());
		assertThat(renewed.leaseExpiresAt())
				.isBetween(beforeRenewal.plus(LEASE), afterRenewal.plus(LEASE));
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().ownership())
				.isEqualTo(renewed);

		assertThat(cycleLedger.complete(renewed, IngestionCycleOutcome.UNCHANGED))
				.isEqualTo(AttemptTransitionResult.APPLIED);

		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
				.satisfies(state -> {
					assertThat(state.status()).isEqualTo(IngestionCycleStatus.IDLE);
					assertThat(state.ownership()).isNull();
					assertThat(state.lastProgressAt()).isNotNull();
					assertThat(state.lastTerminalAt()).isNotNull();
					assertThat(state.lastOutcome()).isEqualTo(IngestionCycleOutcome.UNCHANGED);
					assertThat(state.stateVersion()).isEqualTo(4);
				});
		assertThat(cycleLedger.complete(renewed, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.remainingLease(renewed)).isEmpty();
	}

	@Test
	@DisplayName("Release без terminal outcome освобождает cycle и следующий claim повышает epoch")
	void releaseAllowsNextClaimWithHigherEpoch() {
		IngestionCycleOwnership first = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(cycleLedger.release(first)).isEqualTo(AttemptTransitionResult.APPLIED);
		IngestionCycleOwnership second = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();

		assertThat(second.fencingEpoch()).isEqualTo(2);
		assertThat(second.token()).isNotEqualTo(first.token());
	}

	@Test
	@DisplayName("Текущий owner условно сохраняет потерю ownership как terminal outcome")
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
	@DisplayName("Renew после ожидания блокировки не оживляет уже истекший lease")
	void blockedRenewUsesDatabaseTimeAfterRowLock() throws Exception {
		assertBlockedTransitionRejected((repository, ownership) ->
				repository.renew(ownership, LEASE).isEmpty());
	}

	@Test
	@DisplayName("Progress после ожидания блокировки не меняет уже истекший cycle")
	void blockedProgressUsesDatabaseTimeAfterRowLock() throws Exception {
		assertBlockedTransitionRejected((repository, ownership) ->
				repository.markProgress(ownership) == AttemptTransitionResult.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Completion после ожидания блокировки не закрывает уже истекший cycle")
	void blockedCompletionUsesDatabaseTimeAfterRowLock() throws Exception {
		assertBlockedTransitionRejected((repository, ownership) ->
				repository.complete(ownership, IngestionCycleOutcome.COMPLETED)
						== AttemptTransitionResult.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Release после ожидания блокировки не освобождает уже истекший cycle")
	void blockedReleaseUsesDatabaseTimeAfterRowLock() throws Exception {
		assertBlockedTransitionRejected((repository, ownership) ->
				repository.release(ownership) == AttemptTransitionResult.OWNERSHIP_LOST);
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

	private void assertBlockedTransitionRejected(
			BiFunction<JdbcIngestionCycleRepository, IngestionCycleOwnership, Boolean> transition
	) throws Exception {
		IngestionCycleOwnership ownership = cycleLedger.claim(SOURCE_NAME, LEASE).orElseThrow();
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
			Future<Boolean> result = executor.submit(
					() -> transition.apply(workerRepository, ownership));

			awaitBlockedRowLock(workerPid);
			Instant expiresAt = databaseNow().plusMillis(200);
			setLeaseExpiry(holder, expiresAt);
			awaitDatabaseTimeAfter(expiresAt);
			holder.commit();

			assertThat(result.get(10, TimeUnit.SECONDS)).isTrue();
			assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow())
					.satisfies(state -> {
						assertThat(state.status()).isEqualTo(IngestionCycleStatus.ACTIVE);
						assertThat(state.ownership()).isNotNull();
						assertThat(state.stateVersion()).isEqualTo(1);
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
		assertThat(cycleLedger.isCurrent(forged)).isFalse();
		assertThat(cycleLedger.remainingLease(forged)).isEmpty();
		assertThat(cycleLedger.renew(forged, LEASE)).isEmpty();
		assertThat(cycleLedger.markProgress(forged))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.complete(forged, IngestionCycleOutcome.COMPLETED))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.release(forged))
				.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		assertThat(cycleLedger.isCurrent(current)).isTrue();
		assertThat(cycleLedger.findBySourceName(SOURCE_NAME).orElseThrow().ownership())
				.isEqualTo(current);
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
