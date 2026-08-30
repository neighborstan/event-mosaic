package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Хранит общий остаток времени одной operation по monotonic-источнику.
 * Изменения календарного времени на этот budget не влияют.
 */
public final class OperationBudget {

	private static final Duration MAX_LOOP_LEASE_REFRESH = Duration.ofSeconds(1);

	private final long startedAtNanos;
	private final long limitNanos;
	private final LongSupplier nanoTime;
	private final OperationLeaseGuard leaseGuard;
	private final Duration leaseSafetyMargin;
	private volatile LoopAllowance loopAllowance;

	private OperationBudget(
			Duration limit,
			LongSupplier nanoTime
	) {
		this(limit, nanoTime, nanoTime.getAsLong(), null, Duration.ZERO);
	}

	private OperationBudget(
			Duration limit,
			LongSupplier nanoTime,
			long startedAtNanos,
			OperationLeaseGuard leaseGuard,
			Duration leaseSafetyMargin
	) {
		Objects.requireNonNull(limit, "limit must not be null");
		if (limit.isZero() || limit.isNegative()) {
			throw new IllegalArgumentException("limit must be positive");
		}
		this.limitNanos = limit.toNanos();
		this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime must not be null");
		this.startedAtNanos = startedAtNanos;
		this.leaseGuard = leaseGuard;
		this.leaseSafetyMargin = Objects.requireNonNull(
				leaseSafetyMargin, "leaseSafetyMargin must not be null");
		if (leaseSafetyMargin.isNegative()) {
			throw new IllegalArgumentException("leaseSafetyMargin must not be negative");
		}
	}

	/** Создает production budget поверх {@link System#nanoTime()}. */
	public static OperationBudget start(Duration limit) {
		return new OperationBudget(limit, System::nanoTime);
	}

	/**
	 * Создает budget с управляемым monotonic-источником для deterministic tests.
	 *
	 * @param limit положительная общая длительность
	 * @param nanoTime monotonic source в наносекундах
	 * @return новый независимый budget
	 */
	public static OperationBudget start(Duration limit, LongSupplier nanoTime) {
		return new OperationBudget(limit, nanoTime);
	}

	/**
	 * Добавляет к уже начатой deadline внешнюю проверку lease. Начальное время
	 * не меняется, поэтому ожидание получения ownership остается частью budget.
	 *
	 * @param leaseGuard проверка текущего владельца перед новым шагом
	 * @param safetyMargin запас, после которого новый шаг уже не начинается
	 * @return новый guarded view того же начатого budget
	 */
	public OperationBudget withLeaseGuard(
			OperationLeaseGuard leaseGuard,
			Duration safetyMargin
	) {
		Objects.requireNonNull(leaseGuard, "leaseGuard must not be null");
		Objects.requireNonNull(safetyMargin, "safetyMargin must not be null");
		if (safetyMargin.isNegative()) {
			throw new IllegalArgumentException("safetyMargin must not be negative");
		}
		if (this.leaseGuard != null) {
			throw new IllegalStateException("operation budget already has a lease guard");
		}
		return new OperationBudget(
				Duration.ofNanos(limitNanos),
				nanoTime,
				startedAtNanos,
				leaseGuard,
				safetyMargin);
	}

	/** Возвращает неотрицательный остаток общей operation. */
	public Duration remaining() {
		return checkpoint().remaining();
	}

	/**
	 * Однократно проверяет monotonic deadline и, если он настроен, внешний lease.
	 * Ошибка хранилища lease распространяется вызывающему коду и тем самым
	 * запрещает следующий побочный эффект.
	 *
	 * @return статус и безопасный остаток для следующего шага
	 */
	public OperationBudgetCheckpoint checkpoint() {
		return freshState(false).toCheckpoint();
	}

	private FreshBudgetState freshState(boolean ownershipFirst) {
		long guardStartedAtNanos = nanoTime.getAsLong();
		Duration deadlineRemaining = deadlineRemaining(guardStartedAtNanos);
		if (leaseGuard == null) {
			return deadlineRemaining.isZero()
					? FreshBudgetState.deadlineExceeded()
					: FreshBudgetState.available(deadlineRemaining, null);
		}
		if (!ownershipFirst && deadlineRemaining.isZero()) {
			loopAllowance = null;
			return FreshBudgetState.deadlineExceeded();
		}

		loopAllowance = null;
		OperationLeaseSnapshot lease = Objects.requireNonNull(
				leaseGuard.check(), "lease guard returned null");
		long guardCompletedAtNanos = nanoTime.getAsLong();
		deadlineRemaining = deadlineRemaining(guardCompletedAtNanos);
		Duration safeLeaseRemaining = lease.current()
				? subtractSaturated(
						lease.remainingLease(),
						leaseSafetyMargin.plus(elapsedBetween(
								guardStartedAtNanos,
								guardCompletedAtNanos)))
				: Duration.ZERO;
		if (ownershipFirst) {
			if (!lease.current() || safeLeaseRemaining.isZero()) {
				return FreshBudgetState.ownershipLost();
			}
			if (deadlineRemaining.isZero()) {
				return FreshBudgetState.deadlineExceeded();
			}
		}
		else {
			if (deadlineRemaining.isZero()) {
				return FreshBudgetState.deadlineExceeded();
			}
			if (!lease.current() || safeLeaseRemaining.isZero()) {
				return FreshBudgetState.ownershipLost();
			}
		}
		loopAllowance = new LoopAllowance(
				guardCompletedAtNanos,
				safeLeaseRemaining,
				minimum(MAX_LOOP_LEASE_REFRESH, safeLeaseRemaining));
		return FreshBudgetState.available(deadlineRemaining, safeLeaseRemaining);
	}

	private Duration deadlineRemaining(long currentNanos) {
		long elapsed = currentNanos - startedAtNanos;
		if (elapsed <= 0) {
			return Duration.ofNanos(limitNanos);
		}
		if (elapsed >= limitNanos) {
			return Duration.ZERO;
		}
		return Duration.ofNanos(limitNanos - elapsed);
	}

	private static Duration elapsedBetween(long startedAt, long completedAt) {
		long elapsed = completedAt - startedAt;
		return elapsed <= 0 ? Duration.ZERO : Duration.ofNanos(elapsed);
	}

	/** Возвращает {@code true}, пока разрешено начать следующую фазу. */
	public boolean hasRemaining() {
		return checkpoint().available();
	}

	/**
	 * Требует разрешение на следующий шаг и возвращает его безопасный timeout.
	 * Deadline и потеря ownership остаются разными control-flow причинами.
	 *
	 * @return положительный безопасный остаток времени
	 * @throws OperationDeadlineReachedException если общая deadline исчерпана
	 * @throws OperationOwnershipLostException если lease больше не разрешает шаг
	 */
	public Duration requireAvailable() {
		FreshBudgetState state = freshState(false);
		return switch (state.status()) {
			case AVAILABLE -> state.remaining();
			case DEADLINE_EXCEEDED -> throw new OperationDeadlineReachedException();
			case OWNERSHIP_LOST -> throw new OperationOwnershipLostException();
		};
	}

	/**
	 * Повторно подтверждает ownership после уже завершившегося внешнего вызова.
	 * Проверка lease выполняется даже при одновременно истекшей общей deadline:
	 * потерянное право владельца важнее результата позднего ответа.
	 *
	 * @throws OperationOwnershipLostException если lease больше не принадлежит операции
	 * @throws OperationDeadlineReachedException если ownership еще актуален, но deadline истекла
	 */
	public void requireAvailableAfterExternalResult() {
		FreshBudgetState state = freshState(true);
		switch (state.status()) {
			case AVAILABLE -> {
				return;
			}
			case DEADLINE_EXCEEDED -> throw new OperationDeadlineReachedException();
			case OWNERSHIP_LOST -> throw new OperationOwnershipLostException();
		}
	}

	/**
	 * Подтверждает только ownership перед обязательной terminal-компенсацией.
	 * Истекшая operation deadline не запрещает условно завершить уже начатый
	 * durable attempt. Safety margin ограничивает начало нового I/O, но здесь не
	 * вычитается из еще положительного фактического lease. Потерянный lease
	 * по-прежнему запрещает чужой переход.
	 *
	 * @throws OperationOwnershipLostException если lease больше не принадлежит операции
	 */
	public void requireOwnershipForTerminalTransition() {
		if (leaseGuard == null) {
			return;
		}
		loopAllowance = null;
		OperationLeaseSnapshot lease = Objects.requireNonNull(
				leaseGuard.check(), "lease guard returned null");
		if (!lease.current()) {
			throw new OperationOwnershipLostException();
		}
	}

	/**
	 * Проверяет тесный потоковый цикл без обращения к хранилищу на каждом шаге.
	 * Состояние внешнего lease перепроверяется не реже раза в секунду и не позже
	 * достижения его безопасной границы. Сам lease этот метод не продлевает.
	 * Перед отдельным внешним или долговечным побочным эффектом вместо этого
	 * метода нужен {@link #requireAvailable()}.
	 *
	 * @return положительный безопасный остаток текущего цикла
	 */
	public Duration requireLoopAvailable() {
		long currentNanos = nanoTime.getAsLong();
		Duration deadlineRemaining = deadlineRemaining(currentNanos);
		if (deadlineRemaining.isZero()) {
			throw new OperationDeadlineReachedException();
		}
		if (leaseGuard == null) {
			return deadlineRemaining;
		}

		LoopAllowance allowance = loopAllowance;
		if (allowance == null) {
			return requireAvailable();
		}
		Duration elapsed = elapsedBetween(allowance.checkedAtNanos(), currentNanos);
		if (elapsed.compareTo(allowance.refreshAfter()) >= 0) {
			return requireAvailable();
		}
		Duration safeLeaseRemaining = subtractSaturated(
				allowance.safeLeaseRemaining(),
				elapsed);
		if (safeLeaseRemaining.isZero()) {
			return requireAvailable();
		}
		return minimum(deadlineRemaining, safeLeaseRemaining);
	}

	/**
	 * Получает свежий timeout внешнего вызова и объясняет, какое ограничение
	 * оказалось минимальным. При равенстве lease важнее общей deadline, а общая
	 * deadline важнее настройки отдельного вызова.
	 *
	 * @param configuredTimeout положительный timeout отдельного вызова
	 * @return положительный effective timeout вместе с его источником
	 */
	public OperationEffectiveTimeout effectiveTimeout(Duration configuredTimeout) {
		validateConfiguredTimeout(configuredTimeout);
		FreshBudgetState state = freshState(false);
		if (state.status() == OperationBudgetStatus.DEADLINE_EXCEEDED) {
			throw new OperationDeadlineReachedException();
		}
		if (state.status() == OperationBudgetStatus.OWNERSHIP_LOST) {
			throw new OperationOwnershipLostException();
		}

		Duration timeout = configuredTimeout;
		OperationTimeoutOrigin origin = OperationTimeoutOrigin.CONFIGURED_TIMEOUT;
		if (state.deadlineRemaining().compareTo(timeout) <= 0) {
			timeout = state.deadlineRemaining();
			origin = OperationTimeoutOrigin.OPERATION_DEADLINE;
		}
		if (state.safeLeaseRemaining() != null
				&& state.safeLeaseRemaining().compareTo(timeout) <= 0) {
			timeout = state.safeLeaseRemaining();
			origin = OperationTimeoutOrigin.LEASE_SAFETY;
		}
		return new OperationEffectiveTimeout(timeout, origin);
	}

	/**
	 * После transport timeout заново проверяет ownership и общую deadline.
	 * Фактическая потеря ownership важнее истекшей deadline; если оба состояния
	 * еще доступны, сохраняется источник timeout, выбранный до вызова.
	 *
	 * @param attemptedTimeout timeout, с которым был начат внешний вызов
	 * @return источник terminal timeout после свежей проверки
	 */
	public OperationTimeoutOrigin resolveTimeoutOrigin(
			OperationEffectiveTimeout attemptedTimeout
	) {
		Objects.requireNonNull(attemptedTimeout, "attemptedTimeout must not be null");
		FreshBudgetState state = freshState(true);
		return switch (state.status()) {
			case OWNERSHIP_LOST -> OperationTimeoutOrigin.LEASE_SAFETY;
			case DEADLINE_EXCEEDED -> OperationTimeoutOrigin.OPERATION_DEADLINE;
			case AVAILABLE -> attemptedTimeout.origin();
		};
	}

	/**
	 * Ограничивает timeout дочерней операции меньшим из ее лимита и остатка cycle.
	 *
	 * @param configuredTimeout положительный timeout дочерней операции
	 * @return положительный effective timeout либо {@link Duration#ZERO}
	 */
	public Duration cap(Duration configuredTimeout) {
		validateConfiguredTimeout(configuredTimeout);
		return minimum(checkpoint().remaining(), configuredTimeout);
	}

	private static void validateConfiguredTimeout(Duration configuredTimeout) {
		Objects.requireNonNull(configuredTimeout, "configuredTimeout must not be null");
		if (configuredTimeout.isZero() || configuredTimeout.isNegative()) {
			throw new IllegalArgumentException("configuredTimeout must be positive");
		}
	}

	private static Duration subtractSaturated(Duration value, Duration subtrahend) {
		return value.compareTo(subtrahend) <= 0
				? Duration.ZERO
				: value.minus(subtrahend);
	}

	private static Duration minimum(Duration first, Duration second) {
		return first.compareTo(second) < 0 ? first : second;
	}

	private record LoopAllowance(
			long checkedAtNanos,
			Duration safeLeaseRemaining,
			Duration refreshAfter
	) {
	}

	private record FreshBudgetState(
			OperationBudgetStatus status,
			Duration remaining,
			Duration deadlineRemaining,
			Duration safeLeaseRemaining
	) {

		private static FreshBudgetState available(
				Duration deadlineRemaining,
				Duration safeLeaseRemaining
		) {
			Duration remaining = safeLeaseRemaining == null
					? deadlineRemaining
					: minimum(deadlineRemaining, safeLeaseRemaining);
			return new FreshBudgetState(
					OperationBudgetStatus.AVAILABLE,
					remaining,
					deadlineRemaining,
					safeLeaseRemaining);
		}

		private static FreshBudgetState deadlineExceeded() {
			return unavailable(OperationBudgetStatus.DEADLINE_EXCEEDED);
		}

		private static FreshBudgetState ownershipLost() {
			return unavailable(OperationBudgetStatus.OWNERSHIP_LOST);
		}

		private static FreshBudgetState unavailable(OperationBudgetStatus status) {
			return new FreshBudgetState(status, Duration.ZERO, Duration.ZERO, null);
		}

		private OperationBudgetCheckpoint toCheckpoint() {
			return new OperationBudgetCheckpoint(status, remaining);
		}
	}
}
