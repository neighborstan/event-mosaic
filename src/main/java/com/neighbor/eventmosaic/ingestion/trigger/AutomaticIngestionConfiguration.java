package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.observability.IngestionCycleActivity;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ConcurrentTaskScheduler;

/**
 * Создает automatic ingestion adapter и его отдельный однопоточный executor
 * только для обычного web-процесса с включенным автоматическим режимом.
 */
@Configuration(proxyBeanMethods = false)
@Conditional(IngestionRuntimeModeCondition.Automatic.class)
public class AutomaticIngestionConfiguration {

	/**
	 * Создает выделенный executor ровно с одним worker и удаляет отмененные
	 * задачи из его очереди.
	 *
	 * @return executor только для automatic ingestion
	 */
	@Bean(destroyMethod = "shutdownNow")
	public ScheduledExecutorService automaticIngestionExecutor() {
		ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(
				1,
				Thread.ofPlatform().name("gdelt-automatic-", 0).factory());
		executor.setRemoveOnCancelPolicy(true);
		executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
		return executor;
	}

	/**
	 * Адаптирует выделенный executor к Spring API планирования по времени.
	 *
	 * @param executor однопоточный executor automatic ingestion
	 * @return scheduler, который не разделяет worker с другими задачами
	 */
	@Bean
	public TaskScheduler automaticIngestionTaskScheduler(
			@Qualifier("automaticIngestionExecutor") ScheduledExecutorService executor
	) {
		return new ConcurrentTaskScheduler(executor);
	}

	/**
	 * Создает lifecycle adapter с настройками фиксированной задержки и
	 * ограниченного shutdown.
	 *
	 * @param coordinator единая граница ingestion cycle
	 * @param taskScheduler выделенный scheduler
	 * @param clock прикладное UTC-время
	 * @param properties настройки GDELT ingestion
	 * @return automatic lifecycle adapter
	 */
	@Bean
	public AutomaticIngestionScheduler automaticIngestionScheduler(
			IngestionCycleCoordinator coordinator,
			@Qualifier("automaticIngestionTaskScheduler") TaskScheduler taskScheduler,
			Clock clock,
			GdeltIngestionProperties properties,
			IngestionCycleActivity activity
	) {
		activity.automaticEnabled();
		return new AutomaticIngestionScheduler(
				coordinator,
				taskScheduler,
				clock,
				properties.automatic().pollDelay(),
				properties.automatic().shutdownGrace());
	}
}
