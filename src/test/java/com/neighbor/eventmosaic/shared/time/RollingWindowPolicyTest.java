package com.neighbor.eventmosaic.shared.time;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Общая политика скользящего окна")
class RollingWindowPolicyTest {

	@Test
	@DisplayName("Запас публикации 15 и 30 минут выбирает соответствующую закрытую границу")
	void appliesConfiguredIngestionGrace() {
		Instant now = Instant.parse("2026-08-11T12:37:42Z");

		assertThat(policy(Duration.ofMinutes(15)).windowAt(now).to())
				.isEqualTo(Instant.parse("2026-08-11T12:15:00Z"));
		assertThat(policy(Duration.ofMinutes(30)).windowAt(now).to())
				.isEqualTo(Instant.parse("2026-08-11T12:00:00Z"));
	}

	@Test
	@DisplayName("Некратный сетке запас отклоняется до вычисления окна")
	void rejectsGraceOutsideCadenceGrid() {
		assertThatThrownBy(() -> policy(Duration.ofMinutes(20)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("multiple of cadence");
	}

	private static RollingWindowPolicy policy(Duration grace) {
		return new RollingWindowPolicy(
				Duration.ofMinutes(15),
				Duration.ofHours(24),
				grace);
	}
}
