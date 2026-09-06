package com.neighbor.eventmosaic.ingestion.observability;

import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Показывает, продолжает ли этот процесс запускать и завершать циклы. Отказы GDELT
 * и пропуски занятого цикла тоже считаются завершенной работой планировщика.
 */
@Component
public class IngestionCycleActivity {

	private final Clock clock;
	private final Duration pollDelay;
	private final Duration staleThreshold;
	private boolean automaticEnabled;
	private Instant activityStartedAt;
	private Instant lastTerminalAt;
	private int runningCycles;

	/** Создает наблюдение с общими часами и настроенными границами длительности цикла. */
	public IngestionCycleActivity(
			Clock clock,
			GdeltIngestionProperties properties,
			BackendDataProperties backendProperties
	) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
		this.pollDelay = properties.automatic().pollDelay();
		this.staleThreshold = properties.automatic().effectiveSchedulerStaleThreshold(
				backendProperties.operationDeadline());
	}

	/** Отмечает создание автоматического планировщика только в разрешенном режиме процесса. */
	public synchronized void automaticEnabled() {
		automaticEnabled = true;
		activityStartedAt = clock.instant();
	}

	/** Отмечает вход в общую границу цикла до обращения в PostgreSQL. */
	public synchronized void cycleStarted() {
		runningCycles++;
	}

	/** Отмечает завершение попытки запуска независимо от ее результата. */
	public synchronized void cycleFinished() {
		runningCycles = Math.max(0, runningCycles - 1);
		lastTerminalAt = clock.instant();
	}

	/** Возвращает возраст и состояние без точных времен, идентификаторов и внешнего текста. */
	public synchronized Observation observe() {
		Instant now = clock.instant();
		Instant reference = lastTerminalAt == null ? activityStartedAt : lastTerminalAt;
		long age = reference == null ? 0 : Math.max(0, Duration.between(reference, now).toSeconds());
		long nextDelay = !automaticEnabled || runningCycles > 0 || lastTerminalAt == null
				? 0 : Math.max(0, pollDelay.toSeconds() - age);
		return new Observation(
				automaticEnabled,
				runningCycles > 0,
				automaticEnabled && age >= staleThreshold.toSeconds(),
				age,
				nextDelay);
	}

	/** Небольшая безопасная проекция активности этого процесса для метрик и health. */
	public record Observation(
			boolean automaticEnabled,
			boolean running,
			boolean stale,
			long terminalAgeSeconds,
			long nextDelaySeconds
	) {
	}
}
