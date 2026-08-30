package com.neighbor.eventmosaic.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.PostgreSqlTestcontainersConfiguration;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleStatus;
import com.neighbor.eventmosaic.ingestion.state.JdbcIngestionCycleLedger;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.JdbcProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Import(PostgreSqlTestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("Два независимых контекста ingestion с общим PostgreSQL")
class IngestionCycleCoordinatorIntegrationTest {

	private static final String SOURCE_NAME = GdeltSourceContract.SOURCE_NAME;
	private static final String CYCLE_REPOSITORY_CLASS =
			"com.neighbor.eventmosaic.ingestion.state.JdbcIngestionCycleRepository";
	private static final Duration DEADLINE = Duration.ofMinutes(12);
	private static final Duration LEASE = Duration.ofMinutes(15);

	private final JdbcClient jdbcClient;
	private final PostgreSQLContainer postgresContainer;

	@Autowired
	IngestionCycleCoordinatorIntegrationTest(
			DataSource dataSource,
			PostgreSQLContainer postgresContainer
	) {
		this.jdbcClient = JdbcClient.create(dataSource);
		this.postgresContainer = postgresContainer;
	}

	@BeforeEach
	void cleanCycleState() {
		jdbcClient.sql("truncate table ingestion_cycle_state").update();
	}

	@Test
	@DisplayName("Одновременный trigger двух контекстов допускает один pipeline side effect")
	void concurrentApplicationContextsAllowOnePipelineOwner() throws Exception {
		CountDownLatch start = new CountDownLatch(1);
		CountDownLatch pipelineEntered = new CountDownLatch(1);
		CountDownLatch releasePipeline = new CountDownLatch(1);
		CountDownLatch oneInstanceFinished = new CountDownLatch(1);
		AtomicInteger pipelineSideEffects = new AtomicInteger();
		GdeltPipelineService firstPipeline = blockingPipeline(
				pipelineEntered,
				releasePipeline,
				pipelineSideEffects,
				IngestionOneShotOutcome.COMPLETED);
		GdeltPipelineService secondPipeline = blockingPipeline(
				pipelineEntered,
				releasePipeline,
				pipelineSideEffects,
				IngestionOneShotOutcome.COMPLETED);
		List<IngestionCycleOutcome> outcomes = Collections.synchronizedList(
				new ArrayList<>());

		try (ApplicationInstance first = applicationInstance(firstPipeline);
				ApplicationInstance second = applicationInstance(secondPipeline);
				var executor = Executors.newFixedThreadPool(2)) {
			assertIndependent(first, second);
			var firstFuture = executor.submit(() -> {
				run(first.coordinator(), start, oneInstanceFinished, outcomes);
				return null;
			});
			var secondFuture = executor.submit(() -> {
				run(second.coordinator(), start, oneInstanceFinished, outcomes);
				return null;
			});
			start.countDown();
			try {
				assertThat(pipelineEntered.await(5, TimeUnit.SECONDS)).isTrue();
				assertThat(oneInstanceFinished.await(5, TimeUnit.SECONDS)).isTrue();
				assertThat(outcomes).containsExactly(
						IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);
				assertThat(pipelineSideEffects).hasValue(1);
			}
			finally {
				releasePipeline.countDown();
			}
			firstFuture.get(5, TimeUnit.SECONDS);
			secondFuture.get(5, TimeUnit.SECONDS);
		}

		assertThat(outcomes).containsExactlyInAnyOrder(
				IngestionCycleOutcome.COMPLETED,
				IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);
		assertThat(pipelineSideEffects).hasValue(1);
	}

	@Test
	@DisplayName("Новый контекст восстанавливает cycle после crash и истечения lease")
	void newApplicationContextRecoversCrashedCycleAfterLeaseExpiry() {
		GdeltPipelineService crashedPipeline = mock(GdeltPipelineService.class);
		IngestionCycleOwnership abandonedOwnership;
		try (ApplicationInstance crashed = applicationInstance(crashedPipeline)) {
			abandonedOwnership = crashed.ledger().claim(SOURCE_NAME, LEASE)
					.orElseThrow();
		}

		AtomicInteger recoverySideEffects = new AtomicInteger();
		GdeltPipelineService recoveryPipeline = immediatePipeline(
				recoverySideEffects,
				IngestionOneShotOutcome.COMPLETED);
		try (ApplicationInstance recovery = applicationInstance(recoveryPipeline)) {
			assertThat(recovery.coordinator().runCycle())
					.isEqualTo(IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);
			verifyNoInteractions(recoveryPipeline);

			expireLease();

			assertThat(recovery.coordinator().runCycle())
					.isEqualTo(IngestionCycleOutcome.COMPLETED);
			assertThat(recoverySideEffects).hasValue(1);
			verify(recoveryPipeline).runCycle(any(OperationBudget.class));
			assertThat(recovery.ledger().complete(
					abandonedOwnership,
					IngestionCycleOutcome.INTERNAL_FAILURE))
					.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
			assertThat(recovery.ledger().release(abandonedOwnership))
					.isEqualTo(AttemptTransitionResult.OWNERSHIP_LOST);
		}

		assertThat(cycleState()).satisfies(state -> {
			assertThat(state.status()).isEqualTo(IngestionCycleStatus.IDLE);
			assertThat(state.fencingEpoch()).isEqualTo(2);
			assertThat(state.hasOwner()).isFalse();
			assertThat(state.lastOutcome()).isEqualTo(IngestionCycleOutcome.COMPLETED);
		});
	}

	@Test
	@DisplayName("Поздний результат прежнего owner не перезаписывает итог successor")
	void lateStaleOwnerResultDoesNotOverwriteSuccessorOutcome() throws Exception {
		CountDownLatch stalePipelineEntered = new CountDownLatch(1);
		CountDownLatch releaseStalePipeline = new CountDownLatch(1);
		AtomicInteger staleSideEffects = new AtomicInteger();
		AtomicInteger successorSideEffects = new AtomicInteger();
		GdeltPipelineService stalePipeline = blockingPipeline(
				stalePipelineEntered,
				releaseStalePipeline,
				staleSideEffects,
				IngestionOneShotOutcome.COMPLETED);
		GdeltPipelineService successorPipeline = immediatePipeline(
				successorSideEffects,
				IngestionOneShotOutcome.STORAGE_PRESSURE);

		try (ApplicationInstance stale = applicationInstance(stalePipeline);
				ApplicationInstance successor = applicationInstance(successorPipeline);
				var executor = Executors.newSingleThreadExecutor()) {
			var staleFuture = executor.submit(stale.coordinator()::runCycle);
			assertThat(stalePipelineEntered.await(5, TimeUnit.SECONDS)).isTrue();
			expireLease();

			assertThat(successor.coordinator().runCycle())
					.isEqualTo(IngestionCycleOutcome.STORAGE_PRESSURE);
			assertThat(successorSideEffects).hasValue(1);
			releaseStalePipeline.countDown();

			assertThatThrownBy(() -> staleFuture.get(5, TimeUnit.SECONDS))
					.isInstanceOf(ExecutionException.class)
					.hasCauseInstanceOf(OperationOwnershipLostException.class);
			assertThat(staleSideEffects).hasValue(1);
		}

		assertThat(cycleState()).satisfies(state -> {
			assertThat(state.status()).isEqualTo(IngestionCycleStatus.IDLE);
			assertThat(state.fencingEpoch()).isEqualTo(2);
			assertThat(state.hasOwner()).isFalse();
			assertThat(state.lastOutcome())
					.isEqualTo(IngestionCycleOutcome.STORAGE_PRESSURE);
		});
	}

	private ApplicationInstance applicationInstance(GdeltPipelineService pipeline) {
		AnnotationConfigApplicationContext context =
				new AnnotationConfigApplicationContext();
		context.register(TransactionConfiguration.class);
		context.registerBean(DataSource.class, this::dataSource);
		context.registerBean(PlatformTransactionManager.class, () ->
				new DataSourceTransactionManager(context.getBean(DataSource.class)));
		context.registerBean(JdbcClient.class, () ->
				JdbcClient.create(context.getBean(DataSource.class)));
		context.registerBean(JdbcProperties.class, () -> {
			JdbcProperties properties = new JdbcProperties();
			properties.getTemplate().setQueryTimeout(Duration.ofSeconds(30));
			return properties;
		});
		context.registerBean(loadClass(CYCLE_REPOSITORY_CLASS));
		context.registerBean(JdbcIngestionCycleLedger.class);
		context.registerBean(IngestionCycleCoordinator.class, () ->
				new IngestionCycleCoordinator(
						context.getBean(IngestionCycleLedger.class),
						pipeline,
						OperationBudget::start,
						DEADLINE,
						LEASE));
		context.refresh();
		return new ApplicationInstance(
				context,
				context.getBean(IngestionCycleCoordinator.class),
				context.getBean(IngestionCycleLedger.class));
	}

	private DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(postgresContainer.getDriverClassName());
		dataSource.setUrl(postgresContainer.getJdbcUrl());
		dataSource.setUsername(postgresContainer.getUsername());
		dataSource.setPassword(postgresContainer.getPassword());
		return dataSource;
	}

	private void expireLease() {
		assertThat(jdbcClient.sql("""
				update ingestion_cycle_state
				set last_started_at = clock_timestamp() - interval '2 seconds',
				    lease_expires_at = clock_timestamp() - interval '1 second'
				where source_name = :sourceName
				  and status = :status
				""")
				.param("sourceName", SOURCE_NAME)
				.param("status", IngestionCycleStatus.ACTIVE.name())
				.update()).isEqualTo(1);
	}

	private StoredCycleState cycleState() {
		return jdbcClient.sql("""
				select status, fencing_epoch, owner_token, last_outcome
				from ingestion_cycle_state
				where source_name = :sourceName
				""")
				.param("sourceName", SOURCE_NAME)
				.query((resultSet, rowNumber) -> new StoredCycleState(
						IngestionCycleStatus.valueOf(resultSet.getString("status")),
						resultSet.getLong("fencing_epoch"),
						resultSet.getObject("owner_token") != null,
						resultSet.getString("last_outcome") == null
								? null
								: IngestionCycleOutcome.valueOf(
										resultSet.getString("last_outcome"))))
				.single();
	}

	private static void assertIndependent(
			ApplicationInstance first,
			ApplicationInstance second
	) {
		assertThat(first.context()).isNotSameAs(second.context());
		assertThat(first.ledger()).isNotSameAs(second.ledger());
		assertThat(first.context().getBean(DataSource.class))
				.isNotSameAs(second.context().getBean(DataSource.class));
	}

	private static GdeltPipelineService blockingPipeline(
			CountDownLatch entered,
			CountDownLatch release,
			AtomicInteger sideEffects,
			IngestionOneShotOutcome outcome
	) {
		GdeltPipelineService pipeline = mock(GdeltPipelineService.class);
		when(pipeline.runCycle(any(OperationBudget.class))).thenAnswer(_ -> {
			sideEffects.incrementAndGet();
			entered.countDown();
			if (!release.await(5, TimeUnit.SECONDS)) {
				throw new IllegalStateException("Test pipeline was not released");
			}
			return outcome;
		});
		return pipeline;
	}

	private static GdeltPipelineService immediatePipeline(
			AtomicInteger sideEffects,
			IngestionOneShotOutcome outcome
	) {
		GdeltPipelineService pipeline = mock(GdeltPipelineService.class);
		when(pipeline.runCycle(any(OperationBudget.class))).thenAnswer(_ -> {
			sideEffects.incrementAndGet();
			return outcome;
		});
		return pipeline;
	}

	private static void run(
			IngestionCycleCoordinator coordinator,
			CountDownLatch start,
			CountDownLatch finished,
			List<IngestionCycleOutcome> outcomes
	) throws InterruptedException {
		start.await();
		try {
			outcomes.add(coordinator.runCycle());
		}
		finally {
			finished.countDown();
		}
	}

	@SuppressWarnings("unchecked")
	private static Class<Object> loadClass(String className) {
		try {
			return (Class<Object>) Class.forName(className);
		}
		catch (ClassNotFoundException exception) {
			throw new IllegalStateException("Cycle repository class is unavailable", exception);
		}
	}

	private record ApplicationInstance(
			AnnotationConfigApplicationContext context,
			IngestionCycleCoordinator coordinator,
			IngestionCycleLedger ledger
	) implements AutoCloseable {

		@Override
		public void close() {
			context.close();
		}
	}

	private record StoredCycleState(
			IngestionCycleStatus status,
			long fencingEpoch,
			boolean hasOwner,
			IngestionCycleOutcome lastOutcome
	) {
	}

	@TestConfiguration(proxyBeanMethods = false)
	@EnableTransactionManagement
	static class TransactionConfiguration {
	}
}
