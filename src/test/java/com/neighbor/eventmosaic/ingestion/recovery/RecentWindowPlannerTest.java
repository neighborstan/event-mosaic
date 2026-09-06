package com.neighbor.eventmosaic.ingestion.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.time.RollingWindowPolicy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Планирование ограниченного recent-окна")
class RecentWindowPlannerTest {

	private static final Instant NOW = Instant.parse("2026-08-11T12:37:42Z");

	@Test
	@DisplayName("Latest на верхней границе добавляется к 96 слотам без дубликата")
	void includesFrontierAtWindowEnd() {
		RecentWindowPlan plan = planner(Duration.ofMinutes(15))
				.plan(Instant.parse("2026-08-11T12:15:00Z"));

		assertThat(plan.windowFrom()).isEqualTo(Instant.parse("2026-08-10T12:15:00Z"));
		assertThat(plan.windowTo()).isEqualTo(Instant.parse("2026-08-11T12:15:00Z"));
		assertThat(plan.targetUpdateTimes()).hasSize(97);
		assertThat(plan.targetUpdateTimes()).doesNotHaveDuplicates();
		assertThat(plan.targetUpdateTimes().getLast()).isEqualTo(plan.sourceFrontier());
	}

	@Test
	@DisplayName("Latest на один шаг вперед создает непрерывный двухточечный мост")
	void createsCadenceBridgePastWindow() {
		RecentWindowPlan plan = planner(Duration.ofMinutes(15))
				.plan(Instant.parse("2026-08-11T12:30:00Z"));

		assertThat(plan.targetUpdateTimes()).hasSize(98);
		assertThat(plan.targetUpdateTimes()).endsWith(
				Instant.parse("2026-08-11T12:15:00Z"),
				Instant.parse("2026-08-11T12:30:00Z"));
	}

	@Test
	@DisplayName("Отставший provider добавляет только свой latest вне суточного окна")
	void keepsStaleProviderBounded() {
		Instant stale = Instant.parse("2026-08-09T00:00:00Z");

		RecentWindowPlan plan = planner(Duration.ofMinutes(30)).plan(stale);

		assertThat(plan.windowTo()).isEqualTo(Instant.parse("2026-08-11T12:00:00Z"));
		assertThat(plan.targetUpdateTimes()).hasSize(97).contains(stale);
	}

	@Test
	@DisplayName("Frontier позже уже наступившей UTC-границы отклоняется типизированно")
	void rejectsFrontierAfterCurrentCadenceBoundary() {
		assertThatExceptionOfType(RemoteSourceAccessException.class)
				.isThrownBy(() -> planner(Duration.ofMinutes(15))
						.plan(Instant.parse("2026-08-11T12:45:00Z")))
				.extracting(RemoteSourceAccessException::errorCode)
				.isEqualTo(IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH);
	}

	private static RecentWindowPlanner planner(Duration grace) {
		return new RecentWindowPlanner(
				Clock.fixed(NOW, ZoneOffset.UTC),
				new RollingWindowPolicy(
						Duration.ofMinutes(15),
						Duration.ofHours(24),
						grace));
	}
}
