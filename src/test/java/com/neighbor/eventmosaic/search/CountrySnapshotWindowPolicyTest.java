package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Выбор закрытого суточного окна снимка карты")
class CountrySnapshotWindowPolicyTest {

	@Test
	@DisplayName("До, в момент и после 15-минутной границы окно переключается точно")
	void switchesWindowExactlyAtCadenceBoundary() {
		assertThat(windowAt("2026-08-11T12:29:59Z").to())
				.isEqualTo(Instant.parse("2026-08-11T12:00:00Z"));
		assertThat(windowAt("2026-08-11T12:30:00Z").to())
				.isEqualTo(Instant.parse("2026-08-11T12:15:00Z"));
		assertThat(windowAt("2026-08-11T12:30:01Z").to())
				.isEqualTo(Instant.parse("2026-08-11T12:15:00Z"));

		CountrySnapshotWindowPolicy.Window boundary = windowAt(
				"2026-08-11T12:30:00Z");
		assertThat(boundary.from())
				.isEqualTo(Instant.parse("2026-08-10T12:15:00Z"));
	}

	@Test
	@DisplayName("Локальный часовой пояс Clock не меняет UTC-границы снимка")
	void ignoresClockTimeZoneWhenFlooringWindow() {
		Instant now = Instant.parse("2026-08-11T12:37:42Z");
		Clock nonUtcClock = Clock.fixed(now, ZoneId.of("Asia/Tokyo"));
		var policy = new CountrySnapshotWindowPolicy(nonUtcClock, properties());

		CountrySnapshotWindowPolicy.Window window = policy.currentWindow();

		assertThat(window.from())
				.isEqualTo(Instant.parse("2026-08-10T12:15:00Z"));
		assertThat(window.to())
				.isEqualTo(Instant.parse("2026-08-11T12:15:00Z"));
	}

	private static CountrySnapshotWindowPolicy.Window windowAt(String instant) {
		Clock clock = Clock.fixed(Instant.parse(instant), ZoneOffset.UTC);
		return new CountrySnapshotWindowPolicy(clock, properties()).currentWindow();
	}

	private static CountryMapSnapshotProperties properties() {
		return new CountryMapSnapshotProperties(
				"country-v1",
				Duration.ofMinutes(15));
	}
}
