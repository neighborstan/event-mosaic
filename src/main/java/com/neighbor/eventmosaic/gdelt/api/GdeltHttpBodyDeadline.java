package com.neighbor.eventmosaic.gdelt.api;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/**
 * Ограничивает общее время чтения тела HTTP-ответа GDELT. Если срок
 * истек, закрывает поток и тем самым прерывает зависшее чтение.
 */
public final class GdeltHttpBodyDeadline implements AutoCloseable {

	private final InputStream body;
	private final long deadlineNanos;
	private final LongSupplier nanoTime;
	private final AtomicBoolean completed = new AtomicBoolean();
	private final AtomicBoolean expired = new AtomicBoolean();
	private CompletableFuture<Void> expiration;

	private GdeltHttpBodyDeadline(
			InputStream body,
			long deadlineNanos,
			LongSupplier nanoTime
	) {
		this.body = body;
		this.deadlineNanos = deadlineNanos;
		this.nanoTime = nanoTime;
	}

	/**
	 * Вычисляет момент, когда должно завершиться чтение. Значение опирается
	 * на {@link System#nanoTime()}, поэтому не зависит от перевода системных часов.
	 *
	 * @param timeout максимальная длительность чтения
	 * @return момент истечения срока в шкале {@link System#nanoTime()}
	 */
	public static long deadlineAfter(Duration timeout) {
		return deadlineAfter(timeout, System::nanoTime);
	}

	/**
	 * Вычисляет момент завершения по явно переданному счетчику прошедшего
	 * времени. Тот же счетчик нужно передать в
	 * {@link #start(InputStream, long, LongSupplier)}.
	 *
	 * @param timeout максимальная длительность чтения
	 * @param nanoTime счетчик прошедшего времени, который не зависит от системных часов
	 * @return момент истечения срока в шкале переданного счетчика
	 */
	public static long deadlineAfter(Duration timeout, LongSupplier nanoTime) {
		Objects.requireNonNull(timeout, "timeout must not be null");
		Objects.requireNonNull(nanoTime, "nanoTime must not be null");
		if (timeout.isZero() || timeout.isNegative()) {
			throw new IllegalArgumentException("timeout must be positive");
		}
		long now = nanoTime.getAsLong();
		try {
			return Math.addExact(now, timeout.toNanos());
		} catch (ArithmeticException _) {
			return Long.MAX_VALUE;
		}
	}

	/**
	 * Начинает контроль срока чтения для потока тела HTTP-ответа.
	 *
	 * @param body поток с телом HTTP-ответа
	 * @param deadlineNanos ранее вычисленный момент истечения срока
	 * @return объект, который закроет поток при истечении срока
	 */
	public static GdeltHttpBodyDeadline start(InputStream body, long deadlineNanos) {
		return start(body, deadlineNanos, System::nanoTime);
	}

	/**
	 * Начинает контроль срока чтения с явно заданным счетчиком прошедшего времени.
	 * Этот вариант позволяет после чтения проверить тот же момент завершения.
	 *
	 * @param body поток с телом HTTP-ответа
	 * @param deadlineNanos ранее вычисленный момент истечения срока
	 * @param nanoTime тот же счетчик прошедшего времени, по которому вычислен срок
	 * @return объект, который закроет поток при истечении срока
	 */
	public static GdeltHttpBodyDeadline start(
			InputStream body,
			long deadlineNanos,
			LongSupplier nanoTime
	) {
		GdeltHttpBodyDeadline guard = new GdeltHttpBodyDeadline(
				Objects.requireNonNull(body, "body must not be null"),
				deadlineNanos,
				Objects.requireNonNull(nanoTime, "nanoTime must not be null"));
		long remaining = guard.remainingNanos();
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
	 * Сообщает, был ли поток закрыт из-за истечения срока чтения.
	 *
	 * @return {@code true}, если срок чтения истек
	 */
	public boolean expired() {
		if (!expired.get() && remainingNanos() <= 0) {
			expire();
		}
		return expired.get();
	}

	/**
	 * Прекращает контроль срока после завершения чтения или отказа от него.
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
			// Код, который читает поток, сообщит об истечении срока; ошибка закрытия не должна ее подменять.
		}
	}

	private long remainingNanos() {
		try {
			return Math.subtractExact(deadlineNanos, nanoTime.getAsLong());
		} catch (ArithmeticException _) {
			return Long.MAX_VALUE;
		}
	}
}
