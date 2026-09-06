package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Вычисляет последний завершенный интервал на равномерной сетке UTC. Запас после границы
 * публикации не дает включить в окно данные, которые источник еще не успел опубликовать. Расчет
 * принимает уже зафиксированный момент времени, поэтому одинаковые входные данные всегда дают
 * одинаковый результат.
 */
public final class RollingWindowPolicy {

	private static final Duration MAXIMUM_GRACE = Duration.ofHours(1);

	private final Duration cadence;
	private final Duration horizon;
	private final Duration ingestionGrace;

	/**
	 * Создает общую политику окна.
	 *
	 * @param cadence шаг между соседними границами в UTC
	 * @param horizon длительность рассчитываемого интервала
	 * @param ingestionGrace запас после границы публикации источника
	 */
	public RollingWindowPolicy(
			Duration cadence,
			Duration horizon,
			Duration ingestionGrace
	) {
		this.cadence = requirePositiveWholeSeconds(cadence, "cadence");
		this.horizon = requirePositiveWholeSeconds(horizon, "horizon");
		this.ingestionGrace = requireWholeSeconds(ingestionGrace, "ingestionGrace");
		if (this.ingestionGrace.isNegative()) {
			throw new IllegalArgumentException("ingestionGrace must not be negative");
		}
		if (this.ingestionGrace.getSeconds() % this.cadence.getSeconds() != 0) {
			throw new IllegalArgumentException("ingestionGrace must be a multiple of cadence");
		}
		if (this.ingestionGrace.compareTo(MAXIMUM_GRACE) > 0) {
			throw new IllegalArgumentException("ingestionGrace must not exceed one hour");
		}
		if (this.horizon.getSeconds() % this.cadence.getSeconds() != 0) {
			throw new IllegalArgumentException("horizon must be a multiple of cadence");
		}
	}

	/**
	 * Вычисляет окно для одного зафиксированного момента времени.
	 *
	 * @param now момент UTC, относительно которого нужно выбрать окно
	 * @return интервал с границами на заданной шаговой сетке
	 */
	public RollingWindow windowAt(Instant now) {
		Instant to = floorToCadence(Objects.requireNonNull(now, "now must not be null")
				.minus(ingestionGrace));
		return new RollingWindow(to.minus(horizon), to);
	}

	/**
	 * Округляет момент вниз до ближайшей границы заданной сетки UTC.
	 *
	 * @param instant округляемый момент
	 * @return граница сетки не позже исходного момента
	 */
	public Instant floorToCadence(Instant instant) {
		Objects.requireNonNull(instant, "instant must not be null");
		long cadenceSeconds = cadence.getSeconds();
		long floored = Math.floorDiv(instant.getEpochSecond(), cadenceSeconds) * cadenceSeconds;
		return Instant.ofEpochSecond(floored);
	}

	/** Возвращает шаг общей UTC-сетки. */
	public Duration cadence() {
		return cadence;
	}

	/** Возвращает длительность рассчитываемого интервала. */
	public Duration horizon() {
		return horizon;
	}

	/** Возвращает настроенный запас публикации. */
	public Duration ingestionGrace() {
		return ingestionGrace;
	}

	private static Duration requirePositiveWholeSeconds(Duration value, String name) {
		Duration checked = requireWholeSeconds(value, name);
		if (checked.isZero() || checked.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return checked;
	}

	private static Duration requireWholeSeconds(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.getNano() != 0) {
			throw new IllegalArgumentException(name + " must use whole seconds");
		}
		return value;
	}
}
