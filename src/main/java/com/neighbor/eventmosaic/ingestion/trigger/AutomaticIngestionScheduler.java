package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.error.SafeExceptionProjection;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.TaskScheduler;

/**
 * После готовности web-приложения последовательно планирует ingestion cycles.
 * Следующий tick появляется только после полного завершения предыдущего.
 */
final class AutomaticIngestionScheduler
		implements ApplicationListener<ApplicationReadyEvent>, SmartLifecycle {

	private static final Logger LOGGER = LoggerFactory.getLogger(
			AutomaticIngestionScheduler.class);

	private final IngestionCycleCoordinator coordinator;
	private final TaskScheduler taskScheduler;
	private final Clock clock;
	private final Duration pollDelay;
	private final Duration shutdownGrace;
	private final AtomicReference<AutomaticIngestionSchedulerState> state =
			new AtomicReference<>(AutomaticIngestionSchedulerState.STARTING);
	private final Object lifecycleMonitor = new Object();

	private ScheduledFuture<?> scheduledFuture;
	private Thread activeWorker;

	AutomaticIngestionScheduler(
			IngestionCycleCoordinator coordinator,
			TaskScheduler taskScheduler,
			Clock clock,
			Duration pollDelay,
			Duration shutdownGrace
	) {
		this.coordinator = Objects.requireNonNull(coordinator, "coordinator must not be null");
		this.taskScheduler = Objects.requireNonNull(
				taskScheduler, "taskScheduler must not be null");
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.pollDelay = requirePositive(pollDelay, "pollDelay");
		this.shutdownGrace = requirePositive(shutdownGrace, "shutdownGrace");
	}

	/** Регистрирует первый немедленный tick, не выполняя cycle в event callback. */
	@Override
	public void onApplicationEvent(ApplicationReadyEvent event) {
		Objects.requireNonNull(event, "event must not be null");
		if (!state.compareAndSet(
				AutomaticIngestionSchedulerState.STARTING,
				AutomaticIngestionSchedulerState.RUNNING)) {
			return;
		}
		schedule(clock.instant());
	}

	@Override
	public void start() {
		// Первый tick принадлежит ApplicationReadyEvent, а не ранней lifecycle phase.
	}

	@Override
	public void stop() {
		AutomaticIngestionSchedulerState previous = beginStopping();
		if (previous == AutomaticIngestionSchedulerState.STOPPING
				|| previous == AutomaticIngestionSchedulerState.STOPPED) {
			return;
		}

		long remainingNanos = shutdownGrace.toNanos();
		long startedAt = System.nanoTime();
		synchronized (lifecycleMonitor) {
			ScheduledFuture<?> future = scheduledFuture;
			scheduledFuture = null;
			if (future != null) {
				future.cancel(true);
			}
			Thread worker = activeWorker;
			if (worker != null && worker != Thread.currentThread()) {
				worker.interrupt();
				while (activeWorker != null && remainingNanos > 0) {
					try {
						long millis = Math.max(1L, Duration.ofNanos(remainingNanos).toMillis());
						lifecycleMonitor.wait(millis);
					}
					catch (InterruptedException exception) {
						Thread.currentThread().interrupt();
						break;
					}
					long elapsed = System.nanoTime() - startedAt;
					remainingNanos = shutdownGrace.toNanos() - elapsed;
				}
			}
		}
		state.set(AutomaticIngestionSchedulerState.STOPPED);
	}

	@Override
	public void stop(Runnable callback) {
		stop();
		callback.run();
	}

	@Override
	public boolean isRunning() {
		AutomaticIngestionSchedulerState current = state.get();
		return current == AutomaticIngestionSchedulerState.STARTING
				|| current == AutomaticIngestionSchedulerState.RUNNING;
	}

	AutomaticIngestionSchedulerState state() {
		return state.get();
	}

	private void schedule(Instant startAt) {
		if (state.get() != AutomaticIngestionSchedulerState.RUNNING) {
			return;
		}
		ScheduledTick tick = new ScheduledTick();
		ScheduledFuture<?> future;
		try {
			future = Objects.requireNonNull(
					taskScheduler.schedule(tick, startAt),
					"taskScheduler returned null future");
		}
		catch (RejectedExecutionException exception) {
			stopAfterRejection(exception);
			return;
		}

		synchronized (lifecycleMonitor) {
			if (state.get() != AutomaticIngestionSchedulerState.RUNNING) {
				future.cancel(true);
				tick.attach(future);
				return;
			}
			if (scheduledFuture != null) {
				future.cancel(true);
				tick.attach(future);
				throw new IllegalStateException("automatic ingestion already has a future tick");
			}
			scheduledFuture = future;
			tick.attach(future);
		}
	}

	private void runTick(ScheduledFuture<?> ownFuture) {
		synchronized (lifecycleMonitor) {
			if (scheduledFuture == ownFuture) {
				scheduledFuture = null;
			}
			if (state.get() != AutomaticIngestionSchedulerState.RUNNING
					|| activeWorker != null) {
				return;
			}
			activeWorker = Thread.currentThread();
		}

		try {
			coordinator.runCycle();
		}
		catch (ApplicationException exception) {
			LOGGER.atWarn()
					.setCause(SafeExceptionProjection.from(
							exception,
							exception.errorCode().safeMessage()))
					.addKeyValue("event", "gdelt.automatic_cycle.expected_failure")
					.addKeyValue("error_code", exception.errorCode().code())
					.log("Automatic GDELT cycle failed with an expected error");
		}
		catch (OperationOwnershipLostException exception) {
			LOGGER.atWarn()
					.setCause(SafeExceptionProjection.from(
							exception,
							"Ingestion cycle ownership was lost"))
					.addKeyValue("event", "gdelt.automatic_cycle.ownership_lost")
					.addKeyValue("outcome", IngestionCycleOutcome.OWNERSHIP_LOST)
					.log("Automatic GDELT cycle stopped after ownership loss");
		}
		catch (RuntimeException exception) {
			LOGGER.atError()
					.setCause(SafeExceptionProjection.from(
							exception,
							IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
					.addKeyValue("event", "gdelt.automatic_cycle.internal_failure")
					.addKeyValue("error_code", IngestionErrorCode.INTERNAL_ERROR.code())
					.log("Automatic GDELT cycle failed unexpectedly");
		}
		finally {
			boolean reschedule;
			synchronized (lifecycleMonitor) {
				if (activeWorker == Thread.currentThread()) {
					activeWorker = null;
					lifecycleMonitor.notifyAll();
				}
				reschedule = state.get() == AutomaticIngestionSchedulerState.RUNNING;
			}
			if (reschedule) {
				schedule(clock.instant().plus(pollDelay));
			}
		}
	}

	private AutomaticIngestionSchedulerState beginStopping() {
		while (true) {
			AutomaticIngestionSchedulerState current = state.get();
			if (current == AutomaticIngestionSchedulerState.STOPPING
					|| current == AutomaticIngestionSchedulerState.STOPPED) {
				return current;
			}
			if (state.compareAndSet(current, AutomaticIngestionSchedulerState.STOPPING)) {
				return current;
			}
		}
	}

	private void stopAfterRejection(RejectedExecutionException exception) {
		state.compareAndSet(
				AutomaticIngestionSchedulerState.RUNNING,
				AutomaticIngestionSchedulerState.STOPPED);
		LOGGER.atError()
				.setCause(SafeExceptionProjection.from(
						exception,
						IngestionErrorCode.INTERNAL_ERROR.safeMessage()))
				.addKeyValue("event", "gdelt.automatic_cycle.schedule_rejected")
				.addKeyValue("error_code", IngestionErrorCode.INTERNAL_ERROR.code())
				.log("Automatic GDELT cycle scheduling was rejected");
	}

	private static Duration requirePositive(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}

	private final class ScheduledTick implements Runnable {

		private final CountDownLatch attached = new CountDownLatch(1);
		private ScheduledFuture<?> ownFuture;

		private void attach(ScheduledFuture<?> future) {
			ownFuture = future;
			attached.countDown();
		}

		@Override
		public void run() {
			try {
				attached.await();
			}
			catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
				return;
			}
			runTick(ownFuture);
		}
	}
}
