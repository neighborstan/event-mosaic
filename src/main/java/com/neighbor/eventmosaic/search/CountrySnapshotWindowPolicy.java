package com.neighbor.eventmosaic.search;

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

	private static final long SOURCE_CADENCE_SECONDS = Duration.ofMinutes(15).toSeconds();
	private static final Duration SNAPSHOT_DURATION = Duration.ofHours(24);

	private final Clock clock;
	private final CountryMapSnapshotProperties properties;

	CountrySnapshotWindowPolicy(
			Clock clock,
			CountryMapSnapshotProperties properties
	) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
	}

	Window currentWindow() {
		Instant afterGrace = clock.instant().minus(properties.ingestionGrace());
		long closedCadence = Math.floorDiv(
				afterGrace.getEpochSecond(),
				SOURCE_CADENCE_SECONDS) * SOURCE_CADENCE_SECONDS;
		Instant to = Instant.ofEpochSecond(closedCadence);
		return new Window(to.minus(SNAPSHOT_DURATION), to);
	}

	/** Точное полуоткрытое суточное окно на UTC-сетке исходных обновлений. */
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
					&& Math.floorMod(instant.getEpochSecond(), SOURCE_CADENCE_SECONDS) == 0;
		}
	}
}
