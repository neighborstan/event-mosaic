package com.neighbor.eventmosaic.search;

import com.neighbor.eventmosaic.shared.time.RollingWindowPolicy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Выбирает последнее закрытое суточное окно снимка на 15-минутной сетке GDELT.
 * Расчет использует единые серверные часы и не зависит от локального часового
 * пояса процесса.
 */
@Component
final class CountrySnapshotWindowPolicy {

	private static final Duration SNAPSHOT_DURATION = Duration.ofHours(24);

	private final Clock clock;
	private final RollingWindowPolicy rollingWindowPolicy;

	CountrySnapshotWindowPolicy(
			Clock clock,
			RollingWindowPolicy rollingWindowPolicy
	) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.rollingWindowPolicy = Objects.requireNonNull(
				rollingWindowPolicy, "rollingWindowPolicy must not be null");
	}

	Window currentWindow() {
		var window = rollingWindowPolicy.windowAt(clock.instant());
		return new Window(window.from(), window.to());
	}

	/** Хранит суточное окно на сетке UTC: начало входит в интервал, а конец не входит. */
	record Window(Instant from, Instant to) {

		Window {
			Objects.requireNonNull(from, "from must not be null");
			Objects.requireNonNull(to, "to must not be null");
			if (!SNAPSHOT_DURATION.equals(Duration.between(from, to))) {
				throw new IllegalArgumentException("snapshot window must span exactly 24 hours");
			}
			if (!isCadenceBoundary(from) || !isCadenceBoundary(to)) {
				throw new IllegalArgumentException(
						"snapshot window boundaries must use the 15-minute UTC grid");
			}
		}

		private static boolean isCadenceBoundary(Instant instant) {
			return instant.getNano() == 0
					&& Math.floorMod(
							instant.getEpochSecond(),
							Duration.ofMinutes(15).toSeconds()) == 0;
		}
	}
}
