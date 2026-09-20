package com.neighbor.eventmosaic.ingestion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.observability.IngestionCycleActivity;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationBudgetFactory;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.transaction.TransactionTimedOutException;

@DisplayName("Единая граница общего ingestion cycle")
class IngestionCycleCoordinatorTest {

	private static final Duration DEADLINE = Duration.ofMinutes(12);
	private static final Duration LEASE = Duration.ofMinutes(15);
	private static final IngestionCycleOwnership OWNERSHIP =
			new IngestionCycleOwnership(
					GdeltSourceContract.SOURCE_NAME,
					UUID.fromString("11111111-1111-1111-1111-111111111111"),
					7,
					Instant.parse("2026-08-19T12:15:00Z"));

	private final IngestionCycleLedger ledger = mock(IngestionCycleLedger.class);
	private final GdeltPipelineService pipeline = mock(GdeltPipelineService.class);
	private final OperationBudgetFactory budgetFactory = mock(OperationBudgetFactory.class);
	private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
	private final IngestionMetrics metrics = new IngestionMetrics(registry);
	private final IngestionCycleActivity activity = mock(IngestionCycleActivity.class);

	private IngestionCycleCoordinator coordinator;

	@BeforeEach
	void setUp() {
		when(budgetFactory.start(DEADLINE)).thenReturn(OperationBudget.start(DEADLINE));
		coordinator = new IngestionCycleCoordinator(
				ledger,
				pipeline,
				budgetFactory,
				DEADLINE,
				LEASE,
				metrics,
				activity);
	}

	@Test
	@DisplayName("Подтвержденный pipeline outcome условно завершает global cycle")
	void completedPipelineConditionallyCompletesCycle() {
		claimCurrent();
		when(pipeline.runCycle(any())).thenReturn(IngestionOneShotOutcome.COMPLETED);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.COMPLETED))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.COMPLETED);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.COMPLETED);
		assertThat(registry.get("event_mosaic.ingestion.cycle.duration").timer().count()).isEqualTo(1);
		assertThat(registry.get("event_mosaic.ingestion.cycles").tag("outcome", "completed")
				.counter().count()).isEqualTo(1);
		verify(activity).cycleStarted();
		verify(activity).cycleFinished();
	}

	@Test
	@DisplayName("Потеря внутреннего token условно сохраняет итог общего cycle")
	void innerOwnershipLossConditionallyCompletesGlobalCycle() {
		claimCurrent();
		when(pipeline.runCycle(any())).thenReturn(IngestionOneShotOutcome.OWNERSHIP_LOST);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.OWNERSHIP_LOST);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Занятый lease пропускает cycle до source и pipeline I/O")
	void heldLeaseSkipsCycleBeforePipeline() {
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.empty());

		assertThat(coordinator.runCycle())
				.isEqualTo(IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);

		verifyNoInteractions(pipeline);
		verify(ledger, never()).remainingLease(any(), any());
		assertThat(registry.get("event_mosaic.ingestion.cycles")
				.tag("outcome", "skipped_active_cycle").counter().count()).isEqualTo(1);
		verify(activity).cycleFinished();
	}

	@Test
	@DisplayName("Истекший до claim лимит завершает cycle без владения и durable перехода")
	void deadlineBeforeClaimReturnsLocalOutcome() {
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenThrow(new OperationDeadlineReachedException());

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.DEADLINE);

		verifyNoInteractions(pipeline);
		verify(ledger, never()).complete(any(), any());
	}

	@Test
	@DisplayName("Spring-конструктор берет срок владения из настроек automatic cycle")
	void springConstructorUsesAutomaticCycleLease() {
		Duration automaticCycleLease = Duration.ofMinutes(16);
		GdeltIngestionProperties defaults = GdeltTestFixtures.properties(
				Path.of(".local", "coordinator-test"),
				1024);
		GdeltIngestionProperties properties = new GdeltIngestionProperties(
				defaults.baseUri(),
				defaults.stagingRoot(),
				defaults.http(),
				defaults.zip(),
				new GdeltIngestionProperties.Continuity(
						Duration.ofMinutes(20),
						defaults.continuity().firstRunPolicy(),
						defaults.continuity().firstRunStartAt()),
				new GdeltIngestionProperties.Automatic(
						defaults.automatic().enabled(),
						defaults.automatic().pollDelay(),
						automaticCycleLease,
						defaults.automatic().shutdownGrace(),
						defaults.automatic().schedulerStaleBase(),
						defaults.automatic().sourceOutageThreshold(),
						defaults.automatic().dueWorkLimit(),
						defaults.automatic().receiptAudit()),
				defaults.oneShotEnabled());
		IngestionCycleCoordinator configuredCoordinator = new IngestionCycleCoordinator(
				ledger,
				pipeline,
				budgetFactory,
				properties,
				GdeltTestFixtures.backendDataProperties(),
				metrics,
				activity);
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(automaticCycleLease),
				any(OperationBudget.class)))
				.thenReturn(Optional.empty());

		assertThat(configuredCoordinator.runCycle())
				.isEqualTo(IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE);

		verify(ledger).claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(automaticCycleLease),
				any(OperationBudget.class));
		verify(ledger, never()).claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(properties.continuity().recoveryLease()),
				any(OperationBudget.class));
	}

	@Test
	@DisplayName("Ожидание claim расходует общий budget до запуска pipeline")
	void delayedClaimConsumesBudget() {
		AtomicLong nanoTime = new AtomicLong();
		when(budgetFactory.start(DEADLINE))
				.thenReturn(OperationBudget.start(DEADLINE, nanoTime::get));
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class))).thenAnswer(_ -> {
			nanoTime.set(DEADLINE.toNanos());
			return Optional.of(OWNERSHIP);
		});
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.DEADLINE))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.DEADLINE);

		verifyNoInteractions(pipeline);
		verify(ledger, never()).remainingLease(any(), any());
		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.DEADLINE);
	}

	@Test
	@DisplayName("Типизированная deadline pipeline сохраняет итог deadline общего cycle")
	void typedPipelineDeadlineCompletesCycleAsDeadline() {
		claimCurrent();
		when(pipeline.runCycle(any())).thenThrow(new OperationDeadlineExceededException());
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.DEADLINE))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.DEADLINE);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.DEADLINE);
	}

	@Test
	@DisplayName("Stale deadline transition возвращает потерю ownership вместо ложного terminal итога")
	void staleDeadlineTransitionDoesNotReturnDeadlineOutcome() {
		AtomicLong nanoTime = new AtomicLong();
		when(budgetFactory.start(DEADLINE))
				.thenReturn(OperationBudget.start(DEADLINE, nanoTime::get));
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class))).thenAnswer(_ -> {
			nanoTime.set(DEADLINE.toNanos());
			return Optional.of(OWNERSHIP);
		});
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.DEADLINE))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(coordinator::runCycle)
				.isInstanceOf(OperationOwnershipLostException.class)
				.satisfies(exception -> assertThat(exception.getSuppressed())
						.anyMatch(OperationDeadlineReachedException.class::isInstance));

		verifyNoInteractions(pipeline);
	}

	@Test
	@DisplayName("Child timeout учитывает безопасный остаток lease из PostgreSQL")
	void guardedBudgetCapsPipelineTimeoutByDatabaseLease() {
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenReturn(Optional.of(Duration.ofMinutes(3).plusSeconds(20)));
		when(pipeline.runCycle(any())).thenAnswer(invocation -> {
			OperationBudget guarded = invocation.getArgument(0);
			assertThat(guarded.cap(Duration.ofMinutes(2)))
					.isPositive()
					.isLessThanOrEqualTo(Duration.ofSeconds(20))
					.isGreaterThan(Duration.ofSeconds(19));
			return IngestionOneShotOutcome.COMPLETED;
		});
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.COMPLETED))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThat(coordinator.runCycle()).isEqualTo(IngestionCycleOutcome.COMPLETED);
	}

	@Test
	@DisplayName("Устаревший global owner не записывает terminal state и не запускает pipeline")
	void staleGlobalOwnerStopsWithoutTerminalTransition() {
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenReturn(Optional.empty());
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(coordinator::runCycle)
				.isInstanceOf(OperationOwnershipLostException.class);

		verifyNoInteractions(pipeline);
		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Потеря global ownership внутри pipeline сохраняется только текущим owner")
	void globalOwnershipSignalUsesConditionalTerminalTransition() {
		claimCurrent();
		OperationOwnershipLostException ownershipLost =
				new OperationOwnershipLostException();
		when(pipeline.runCycle(any())).thenThrow(ownershipLost);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(coordinator::runCycle).isSameAs(ownershipLost);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Stale terminal transition не подтверждает успешный результат pipeline")
	void staleTerminalTransitionRejectsPipelineResult() {
		claimCurrent();
		when(pipeline.runCycle(any())).thenReturn(IngestionOneShotOutcome.COMPLETED);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.COMPLETED))
				.thenReturn(AttemptTransitionResult.OWNERSHIP_LOST);

		assertThatThrownBy(coordinator::runCycle)
				.isInstanceOf(OperationOwnershipLostException.class);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.COMPLETED);
		verify(ledger, never()).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
		assertThat(registry.get("event_mosaic.ingestion.cycle.duration")
				.tag("outcome", "ownership_lost").timer().count()).isEqualTo(1);
		assertThat(registry.find("event_mosaic.ingestion.cycle.duration").tag("outcome", "completed")
				.timer()).isNull();
	}

	@Test
	@DisplayName("Ожидаемый отказ условно сохраняется и остается исходной ошибкой")
	void expectedFailureIsConditionallyRecordedAndRethrown() {
		claimCurrent();
		RemoteSourceAccessException failure = new RemoteSourceAccessException(
				IngestionErrorCode.MANIFEST_HTTP_ERROR);
		when(pipeline.runCycle(any())).thenThrow(failure);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.EXPECTED_FAILURE))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(coordinator::runCycle).isSameAs(failure);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.EXPECTED_FAILURE);
	}

	@Test
	@DisplayName("Неожиданный отказ получает bounded internal outcome")
	void unexpectedFailureIsConditionallyRecordedAndRethrown() {
		claimCurrent();
		IllegalStateException failure = new IllegalStateException("unexpected detail");
		when(pipeline.runCycle(any())).thenThrow(failure);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.INTERNAL_FAILURE))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(coordinator::runCycle).isSameAs(failure);

		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.INTERNAL_FAILURE);
	}

	@Test
	@DisplayName("Ошибка PostgreSQL ownership check закрывает доступ без internal terminal state")
	void databaseOwnershipCheckFailureFailsClosedWithoutInternalOutcome() {
		DataAccessResourceFailureException databaseFailure =
				new DataAccessResourceFailureException("database unavailable");
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenThrow(databaseFailure);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST))
				.thenThrow(databaseFailure);

		assertThatThrownBy(coordinator::runCycle)
				.isInstanceOfSatisfying(
						OperationOwnershipLostException.class,
						exception -> assertThat(exception.getSuppressed())
								.containsExactly(databaseFailure));

		verifyNoInteractions(pipeline);
		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
		verify(ledger, never()).complete(OWNERSHIP, IngestionCycleOutcome.INTERNAL_FAILURE);
	}

	@Test
	@DisplayName("Отказ транзакции при ownership check закрывает доступ к pipeline")
	void transactionOwnershipCheckFailureFailsClosed() {
		TransactionTimedOutException transactionFailure =
				new TransactionTimedOutException("transaction deadline reached");
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenThrow(transactionFailure);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(coordinator::runCycle)
				.isInstanceOfSatisfying(
						OperationOwnershipLostException.class,
						exception -> assertThat(exception.getSuppressed())
								.containsExactly(transactionFailure));

		verifyNoInteractions(pipeline);
		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
	}

	@Test
	@DisplayName("Программная ошибка ownership check не маскируется под потерю owner")
	void programmingFailureDuringOwnershipCheckRemainsInternalFailure() {
		IllegalStateException programmingFailure =
				new IllegalStateException("unexpected mapper defect");
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenThrow(programmingFailure);
		when(ledger.complete(OWNERSHIP, IngestionCycleOutcome.INTERNAL_FAILURE))
				.thenReturn(AttemptTransitionResult.APPLIED);

		assertThatThrownBy(coordinator::runCycle).isSameAs(programmingFailure);

		verifyNoInteractions(pipeline);
		verify(ledger).complete(OWNERSHIP, IngestionCycleOutcome.INTERNAL_FAILURE);
		verify(ledger, never()).complete(OWNERSHIP, IngestionCycleOutcome.OWNERSHIP_LOST);
	}

	private void claimCurrent() {
		when(ledger.claim(
				eq(GdeltSourceContract.SOURCE_NAME),
				eq(LEASE),
				any(OperationBudget.class)))
				.thenReturn(Optional.of(OWNERSHIP));
		when(ledger.remainingLease(eq(OWNERSHIP), any(OperationBudget.class)))
				.thenReturn(Optional.of(LEASE));
	}
}
