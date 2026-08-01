package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Хранит общий остаток времени одной operation по monotonic-источнику.
 * Изменения календарного времени на этот budget не влияют.
 */
public final class OperationBudget {

	private final long startedAtNanos;
	private final long limitNanos;
	private final LongSupplier nanoTime;

	private OperationBudget(
			Duration limit,
			LongSupplier nanoTime
	) {
		Objects.requireNonNull(limit, "limit must not be null");
		if (limit.isZero() || limit.isNegative()) {
			throw new IllegalArgumentException("limit must be positive");
		}
		this.limitNanos = limit.toNanos();
		this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime must not be null");
		this.startedAtNanos = nanoTime.getAsLong();
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

	/** Возвращает неотрицательный остаток общей operation. */
	public Duration remaining() {
		long elapsed = nanoTime.getAsLong() - startedAtNanos;
		if (elapsed <= 0) {
			return Duration.ofNanos(limitNanos);
		}
		if (elapsed >= limitNanos) {
			return Duration.ZERO;
		}
		return Duration.ofNanos(limitNanos - elapsed);
	}

	/** Возвращает {@code true}, пока разрешено начать следующую фазу. */
	public boolean hasRemaining() {
		return !remaining().isZero();
	}

	/**
	 * Ограничивает timeout дочерней операции меньшим из ее лимита и остатка cycle.
	 *
	 * @param configuredTimeout положительный timeout дочерней операции
	 * @return положительный effective timeout либо {@link Duration#ZERO}
	 */
	public Duration cap(Duration configuredTimeout) {
		Objects.requireNonNull(configuredTimeout, "configuredTimeout must not be null");
		if (configuredTimeout.isZero() || configuredTimeout.isNegative()) {
			throw new IllegalArgumentException("configuredTimeout must be positive");
		}
		Duration remaining = remaining();
		return remaining.compareTo(configuredTimeout) < 0
				? remaining
				: configuredTimeout;
	}
}
