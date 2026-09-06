package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * Хранит интервал времени с включаемым началом и исключаемым концом. Границы
 * задаются в UTC, чтобы расчет не зависел от местного часового пояса.
 *
 * @param from включаемая нижняя граница
 * @param to исключаемая верхняя граница
 */
public record RollingWindow(Instant from, Instant to) {

	/** Проверяет, что конец интервала расположен после его начала. */
	public RollingWindow {
		Objects.requireNonNull(from, "from must not be null");
		Objects.requireNonNull(to, "to must not be null");
		if (!from.isBefore(to)) {
			throw new IllegalArgumentException("rolling window must have positive duration");
		}
	}

	/** Возвращает время между началом и концом интервала. */
	public Duration duration() {
		return Duration.between(from, to);
	}
}
