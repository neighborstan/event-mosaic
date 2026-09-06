package com.neighbor.eventmosaic.ingestion.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Наблюдение за работой автоматического планировщика")
class IngestionCycleActivityTest {

	@Test
	@DisplayName("Завершение отказа или пропуска сохраняет свежую активность при долгой недоступности источника")
	void everyTerminalAttemptRefreshesActivity() {
		Clock clock = mock(Clock.class);
		Instant start = Instant.parse("2026-09-06T12:00:00Z");
		when(clock.instant()).thenReturn(start);
		IngestionCycleActivity activity = new IngestionCycleActivity(
				clock,
				GdeltTestFixtures.properties(Path.of(".local", "activity-test"), 1024),
				GdeltTestFixtures.backendDataProperties());
		activity.automaticEnabled();
		activity.cycleStarted();
		assertThat(activity.observe().running()).isTrue();

		when(clock.instant()).thenReturn(start.plus(Duration.ofMinutes(40)));
		activity.cycleFinished();
		assertThat(activity.observe()).isEqualTo(new IngestionCycleActivity.Observation(true, false, false, 0, 60));

		when(clock.instant()).thenReturn(start.plus(Duration.ofMinutes(54)).minusSeconds(1));
		assertThat(activity.observe().stale()).isFalse();
		when(clock.instant()).thenReturn(start.plus(Duration.ofMinutes(54)));
		assertThat(activity.observe().stale()).isTrue();
	}

	@Test
	@DisplayName("Выключенный автоматический режим не считается остановившимся планировщиком")
	void disabledSchedulerNeverBecomesStale() {
		Clock clock = mock(Clock.class);
		when(clock.instant()).thenReturn(Instant.parse("2026-09-06T12:00:00Z"));
		IngestionCycleActivity activity = new IngestionCycleActivity(
				clock,
				GdeltTestFixtures.properties(Path.of(".local", "activity-test"), 1024),
				GdeltTestFixtures.backendDataProperties());
		assertThat(activity.observe()).isEqualTo(new IngestionCycleActivity.Observation(false, false, false, 0, 0));
	}
}
