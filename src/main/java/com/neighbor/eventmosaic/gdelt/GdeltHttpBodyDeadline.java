package com.neighbor.eventmosaic.gdelt;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Прерывает чтение streaming HTTP body по общему monotonic deadline.
 */
public final class GdeltHttpBodyDeadline implements AutoCloseable {

	private final InputStream body;
	private final AtomicBoolean completed = new AtomicBoolean();
	private final AtomicBoolean expired = new AtomicBoolean();
	private CompletableFuture<Void> expiration;

	private GdeltHttpBodyDeadline(InputStream body) {
		this.body = body;
	}

	/**
	 * Вычисляет deadline относительно monotonic clock процесса.
	 *
	 * @param timeout полный timeout HTTP operation
	 * @return значение, совместимое с {@link System#nanoTime()}
	 */
	public static long deadlineAfter(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout must not be null");
		if (timeout.isZero() || timeout.isNegative()) {
			throw new IllegalArgumentException("timeout must be positive");
		}
		long now = System.nanoTime();
		try {
			return Math.addExact(now, timeout.toNanos());
		} catch (ArithmeticException _) {
			return Long.MAX_VALUE;
		}
	}

	/**
	 * Начинает наблюдение за body с ранее вычисленным общим deadline.
	 *
	 * @param body streaming response body
	 * @param deadlineNanos monotonic deadline
	 * @return lifecycle deadline guard
	 */
	public static GdeltHttpBodyDeadline start(InputStream body, long deadlineNanos) {
		GdeltHttpBodyDeadline guard = new GdeltHttpBodyDeadline(
				Objects.requireNonNull(body, "body must not be null"));
		long remaining;
		try {
			remaining = Math.subtractExact(deadlineNanos, System.nanoTime());
		} catch (ArithmeticException _) {
			remaining = Long.MAX_VALUE;
		}
		if (remaining <= 0) {
			guard.expire();
		} else {
			guard.expiration = CompletableFuture.runAsync(
					guard::expire,
					CompletableFuture.delayedExecutor(remaining, TimeUnit.NANOSECONDS));
		}
		return guard;
	}

	/**
	 * Показывает, что guard закрыл body по deadline.
	 *
	 * @return {@code true} после timeout
	 */
	public boolean expired() {
		return expired.get();
	}

	/**
	 * Завершает наблюдение после полного чтения body.
	 */
	@Override
	public void close() {
		completed.set(true);
		if (expiration != null) {
			expiration.cancel(false);
		}
	}

	private void expire() {
		if (!completed.compareAndSet(false, true)) {
			return;
		}
		expired.set(true);
		try {
			body.close();
		} catch (IOException _) {
			// Основной consumer классифицирует deadline, а не cleanup close failure.
		}
	}
}
