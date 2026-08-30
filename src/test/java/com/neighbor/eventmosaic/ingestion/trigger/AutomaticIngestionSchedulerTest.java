package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.scheduling.TaskScheduler;

@DisplayName("Последовательный automatic ingestion scheduler")
@ExtendWith(OutputCaptureExtension.class)
class AutomaticIngestionSchedulerTest {

	private static final Duration POLL_DELAY = Duration.ofMinutes(1);
	private static final Duration SHUTDOWN_GRACE = Duration.ofSeconds(1);
	private static final Instant INITIAL_TIME = Instant.parse("2026-08-19T12:00:00Z");

	private final IngestionCycleCoordinator coordinator = mock(IngestionCycleCoordinator.class);
	private final TaskScheduler taskScheduler = mock(TaskScheduler.class);
	private final MutableClock clock = new MutableClock(INITIAL_TIME);
	private final List<ScheduledCall> calls = new ArrayList<>();

	private AutomaticIngestionScheduler scheduler;

	@BeforeEach
	void setUp() {
		when(taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
				.thenAnswer(invocation -> {
					ScheduledFuture<?> future = mock(ScheduledFuture.class);
					calls.add(new ScheduledCall(
							invocation.getArgument(0),
							invocation.getArgument(1),
							future));
					return future;
				});
		scheduler = new AutomaticIngestionScheduler(
				coordinator,
				taskScheduler,
				clock,
				POLL_DELAY,
				SHUTDOWN_GRACE);
	}

	@Test
	@DisplayName("Ready event только регистрирует немедленный первый tick")
	void readyEventSchedulesImmediateTickWithoutInlineRun() {
		ready();

		assertThat(calls).hasSize(1);
		assertThat(calls.getFirst().startAt()).isEqualTo(INITIAL_TIME);
		verifyNoInteractions(coordinator);
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.RUNNING);
	}

	@Test
	@DisplayName("Следующий tick получает fixed delay от terminal завершения")
	void nextTickUsesFixedDelayAfterTerminalCompletion() {
		when(coordinator.runCycle()).thenReturn(IngestionCycleOutcome.COMPLETED);
		ready();
		clock.set(INITIAL_TIME.plusSeconds(20));

		runCall(0);

		assertThat(calls).hasSize(2);
		assertThat(calls.get(1).startAt())
				.isEqualTo(INITIAL_TIME.plusSeconds(20).plus(POLL_DELAY));
		verify(coordinator).runCycle();
	}

	@Test
	@DisplayName("Ожидаемая и неожиданная ошибки не останавливают будущий tick")
	void failuresStillScheduleNextTick() {
		doThrow(new IllegalStateException("unexpected detail"))
				.when(coordinator).runCycle();
		ready();

		runCall(0);

		assertThat(calls).hasSize(2);
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.RUNNING);
	}

	@Test
	@DisplayName("Потеря global ownership остается отдельным ожидаемым исходом")
	void ownershipLossIsNotReportedAsInternalFailure(CapturedOutput output) {
		doThrow(new OperationOwnershipLostException()).when(coordinator).runCycle();
		ready();

		assertThatNoException().isThrownBy(() -> runCall(0));

		assertThat(calls).hasSize(2);
		assertThat(output)
				.contains("Automatic GDELT cycle stopped after ownership loss")
				.contains("Ingestion cycle ownership was lost")
				.doesNotContain("Automatic GDELT cycle failed unexpectedly");
	}

	@Test
	@DisplayName("Повторный ready event не создает overlap или второй future")
	void repeatedReadyEventDoesNotCreateAnotherFuture() {
		ready();
		ready();

		assertThat(calls).hasSize(1);
		verifyNoInteractions(coordinator);
	}

	@Test
	@DisplayName("Повторный запуск того же tick не создает локальный overlap")
	void concurrentDuplicateTickDoesNotOverlapActiveWorker() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		doAnswer(_ -> {
			entered.countDown();
			release.await();
			return IngestionCycleOutcome.COMPLETED;
		}).when(coordinator).runCycle();
		ready();
		Runnable tick = calls.getFirst().task();
		Thread first = new Thread(tick, "automatic-first-worker");
		Thread duplicate = new Thread(tick, "automatic-duplicate-worker");

		first.start();
		assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();
		duplicate.start();
		duplicate.join(1_000);
		release.countDown();
		first.join(1_000);

		assertThat(first.isAlive()).isFalse();
		assertThat(duplicate.isAlive()).isFalse();
		verify(coordinator, times(1)).runCycle();
		assertThat(calls).hasSize(2);
	}

	@Test
	@DisplayName("Shutdown до ready запрещает первый schedule")
	void shutdownBeforeReadyPreventsFirstSchedule() {
		scheduler.stop();
		ready();

		assertThat(calls).isEmpty();
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
	}

	@Test
	@DisplayName("Shutdown отменяет только собственный будущий tick")
	void shutdownCancelsOwnedFuture() {
		ScheduledFuture<?> unrelated = mock(ScheduledFuture.class);
		ready();

		scheduler.stop();

		verify(calls.getFirst().future()).cancel(true);
		verifyNoInteractions(unrelated);
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
	}

	@Test
	@DisplayName("Shutdown во время регистрации tick отменяет только поздно возвращенный future")
	void shutdownRacingScheduleCancelsReturnedFuture() throws Exception {
		CountDownLatch scheduleEntered = new CountDownLatch(1);
		CountDownLatch releaseSchedule = new CountDownLatch(1);
		ScheduledFuture<?> future = mock(ScheduledFuture.class);
		when(taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
				.thenAnswer(_ -> {
					scheduleEntered.countDown();
					releaseSchedule.await();
					return future;
				});
		Thread readyThread = new Thread(this::ready, "automatic-ready-race");

		readyThread.start();
		assertThat(scheduleEntered.await(1, TimeUnit.SECONDS)).isTrue();
		scheduler.stop();
		releaseSchedule.countDown();
		readyThread.join(1_000);

		assertThat(readyThread.isAlive()).isFalse();
		verify(future).cancel(true);
		verifyNoInteractions(coordinator);
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
	}

	@Test
	@DisplayName("Shutdown прерывает active worker и не планирует продолжение")
	void shutdownInterruptsActiveWorkerWithoutReschedule() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		AtomicBoolean interrupted = new AtomicBoolean();
		doAnswer(_ -> {
			entered.countDown();
			try {
				new CountDownLatch(1).await();
				return IngestionCycleOutcome.COMPLETED;
			}
			catch (InterruptedException exception) {
				interrupted.set(true);
				Thread.currentThread().interrupt();
				throw new IngestionInterruptedException(exception);
			}
		}).when(coordinator).runCycle();
		ready();
		Thread worker = new Thread(calls.getFirst().task(), "automatic-test-worker");
		worker.start();
		assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();

		scheduler.stop();
		worker.join(1_000);

		assertThat(interrupted).isTrue();
		assertThat(worker.isAlive()).isFalse();
		assertThat(calls).hasSize(1);
		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
	}

	@Test
	@DisplayName("Shutdown завершает ожидание в заданный срок даже при зависшем worker")
	void shutdownWaitIsBoundedWhenWorkerIgnoresInterruption() throws Exception {
		CountDownLatch entered = new CountDownLatch(1);
		CountDownLatch release = new CountDownLatch(1);
		doAnswer(_ -> {
			entered.countDown();
			while (release.getCount() > 0) {
				try {
					release.await();
				}
				catch (InterruptedException ignored) {
					// Имитируем чужой blocking call, который не завершился по interrupt.
				}
			}
			return IngestionCycleOutcome.COMPLETED;
		}).when(coordinator).runCycle();
		scheduler = new AutomaticIngestionScheduler(
				coordinator,
				taskScheduler,
				clock,
				POLL_DELAY,
				Duration.ofMillis(50));
		ready();
		Thread worker = new Thread(calls.getFirst().task(), "automatic-stuck-worker");
		worker.start();
		assertThat(entered.await(1, TimeUnit.SECONDS)).isTrue();

		long startedAt = System.nanoTime();
		scheduler.stop();
		Duration elapsed = Duration.ofNanos(System.nanoTime() - startedAt);

		try {
			assertThat(elapsed).isLessThan(Duration.ofMillis(500));
			assertThat(worker.isAlive()).isTrue();
			assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
			assertThat(calls).hasSize(1);
		}
		finally {
			release.countDown();
			worker.join(1_000);
		}
		assertThat(worker.isAlive()).isFalse();
	}

	@Test
	@DisplayName("Отказ первого schedule переводит adapter в terminal state")
	void rejectedInitialScheduleDoesNotReturnToRunning() {
		when(taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
				.thenThrow(new RejectedExecutionException("stopped"));

		ready();

		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
		verifyNoInteractions(coordinator);
	}

	@Test
	@DisplayName("Отказ reschedule после cycle не возвращает adapter в running")
	void rejectedRescheduleDoesNotReturnToRunning() {
		AtomicBoolean first = new AtomicBoolean(true);
		when(taskScheduler.schedule(any(Runnable.class), any(Instant.class)))
				.thenAnswer(invocation -> {
					if (!first.getAndSet(false)) {
						throw new RejectedExecutionException("stopped");
					}
					ScheduledFuture<?> future = mock(ScheduledFuture.class);
					calls.add(new ScheduledCall(
							invocation.getArgument(0),
							invocation.getArgument(1),
							future));
					return future;
				});
		when(coordinator.runCycle()).thenReturn(IngestionCycleOutcome.UNCHANGED);
		ready();

		runCall(0);

		assertThat(scheduler.state()).isEqualTo(AutomaticIngestionSchedulerState.STOPPED);
		verify(coordinator).runCycle();
	}

	private void ready() {
		scheduler.onApplicationEvent(mock(ApplicationReadyEvent.class));
	}

	private void runCall(int index) {
		calls.get(index).task().run();
	}

	private record ScheduledCall(
			Runnable task,
			Instant startAt,
			ScheduledFuture<?> future
	) {
	}

	private static final class MutableClock extends Clock {

		private final AtomicReference<Instant> instant;

		private MutableClock(Instant initial) {
			this.instant = new AtomicReference<>(initial);
		}

		private void set(Instant value) {
			instant.set(value);
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(ZoneId zone) {
			if (!ZoneOffset.UTC.equals(zone)) {
				throw new IllegalArgumentException("test clock uses UTC");
			}
			return this;
		}

		@Override
		public Instant instant() {
			return instant.get();
		}
	}
}
