package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.neighbor.eventmosaic.ingestion.IngestionCycleCoordinator;
import com.neighbor.eventmosaic.ingestion.observability.IngestionCycleActivity;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GenerationCleanupCommandProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.config.IngestionRuntimePropertiesValidator;
import com.neighbor.eventmosaic.ingestion.config.PartitionRebuildCommandProperties;
import java.time.Clock;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.SimpleCommandLinePropertySource;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("Выбор Spring-компонента для режима запуска")
class IngestionTriggerModeSelectionTest {

	private static final String ONE_SHOT_ENABLED =
			"event-mosaic.ingestion.gdelt.one-shot-enabled=true";
	private static final String AUTOMATIC_ENABLED =
			"event-mosaic.ingestion.gdelt.automatic.enabled=true";
	private static final String PARTITION_REBUILD_PROPERTY =
			PartitionRebuildCommandLine.PROPERTY_PREFIX + ".mode";
	private static final String GENERATION_CLEANUP_PROPERTY =
			GenerationCleanupCommandLine.PROPERTY_PREFIX + ".mode";
	private static final String PARTITION_REBUILD_MODE =
			"--" + PARTITION_REBUILD_PROPERTY + "=inspect-rebuild";
	private static final String GENERATION_CLEANUP_MODE =
			"--" + GENERATION_CLEANUP_PROPERTY + "=inspect-cleanup";

	private final WebApplicationContextRunner webContextRunner =
			withDependencies(new WebApplicationContextRunner());
	private final ApplicationContextRunner nonWebContextRunner =
			withDependencies(new ApplicationContextRunner());

	@Test
	@DisplayName("Явный однократный режим создает только один adapter загрузки")
	void oneShotModeCreatesOnlyOneShotAdapter() {
		webContextRunner
				.withPropertyValues(ONE_SHOT_ENABLED, AUTOMATIC_ENABLED)
				.run(context -> {
					assertThat(context).hasSingleBean(IngestionOneShotRunner.class);
					assertThat(context).doesNotHaveBean(AutomaticIngestionScheduler.class);
					assertThat(context).doesNotHaveBean(ScheduledExecutorService.class);
					assertThat(context).doesNotHaveBean(PartitionRebuildCommandLineAdapter.class);
					assertThat(context).doesNotHaveBean(GenerationCleanupCommandLineAdapter.class);
				});
	}

	@Test
	@DisplayName("Явная команда восстановления создает только свой служебный adapter")
	void partitionRebuildCreatesOnlyItsMaintenanceAdapter() {
		withCommandLine(nonWebContextRunner, PARTITION_REBUILD_MODE).run(context -> {
			assertThat(context).hasSingleBean(PartitionRebuildCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(AutomaticIngestionScheduler.class);
			assertThat(context).doesNotHaveBean(GenerationCleanupCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
		});
	}

	@Test
	@DisplayName("Явная команда очистки создает только свой служебный adapter")
	void generationCleanupCreatesOnlyItsMaintenanceAdapter() {
		withCommandLine(nonWebContextRunner, GENERATION_CLEANUP_MODE).run(context -> {
			assertThat(context).hasSingleBean(GenerationCleanupCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(AutomaticIngestionScheduler.class);
			assertThat(context).doesNotHaveBean(PartitionRebuildCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
		});
	}

	@Test
	@DisplayName("Выключенный режим не создает ни один механизм запуска")
	void disabledModeDoesNotCreateTriggerBeans() {
		webContextRunner.withPropertyValues("event-mosaic.ingestion.gdelt.automatic.enabled=false").run(context -> {
			assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
			assertThat(context).doesNotHaveBean(PartitionRebuildCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(GenerationCleanupCommandLineAdapter.class);
			assertThat(context).doesNotHaveBean(AutomaticIngestionScheduler.class);
			assertThat(context).doesNotHaveBean(ScheduledExecutorService.class);
		});
	}

	@Test
	@DisplayName("Настройка восстановления вне command line останавливает startup")
	void hiddenPartitionRebuildPropertyFailsStartup() {
		webContextRunner
				.withPropertyValues(AUTOMATIC_ENABLED, PARTITION_REBUILD_MODE.substring(2))
				.run(context -> {
					assertThat(context).hasFailed();
					assertThat(context.getStartupFailure()).hasRootCauseMessage(
							"Maintenance mode must be provided through commandLineArgs: "
									+ PARTITION_REBUILD_PROPERTY);
				});
	}

	@Test
	@DisplayName("Настройка очистки вне command line останавливает startup")
	void hiddenGenerationCleanupPropertyFailsStartup() {
		webContextRunner
				.withPropertyValues(AUTOMATIC_ENABLED, GENERATION_CLEANUP_MODE.substring(2))
				.run(context -> {
					assertThat(context).hasFailed();
					assertThat(context.getStartupFailure()).hasRootCauseMessage(
							"Maintenance mode must be provided through commandLineArgs: "
									+ GENERATION_CLEANUP_PROPERTY);
				});
	}

	@Test
	@DisplayName("Без дополнительных настроек автоматическая загрузка включается только в обычном веб-процессе")
	void automaticModeRequiresOrdinaryWebRuntime() {
		webContextRunner
				.run(context -> {
					assertThat(context).hasSingleBean(AutomaticIngestionScheduler.class);
					assertThat(context).hasSingleBean(ScheduledExecutorService.class);
					assertThat(context).doesNotHaveBean(IngestionOneShotRunner.class);
					assertThat(context).doesNotHaveBean(PartitionRebuildCommandLineAdapter.class);
					assertThat(context).doesNotHaveBean(GenerationCleanupCommandLineAdapter.class);
				});
		nonWebContextRunner
				.run(context -> {
					assertThat(context).doesNotHaveBean(AutomaticIngestionScheduler.class);
					assertThat(context).doesNotHaveBean(ScheduledExecutorService.class);
				});
	}

	@Test
	@DisplayName("Однократная загрузка со служебной командой останавливает startup")
	void oneShotAndMaintenanceConflictFailsBeforeAdapterCreation() {
		withCommandLine(
				nonWebContextRunner.withPropertyValues(ONE_SHOT_ENABLED),
				PARTITION_REBUILD_MODE)
				.run(context -> {
					assertThat(context).hasFailed();
					assertThat(context.getStartupFailure())
							.hasRootCauseMessage(
									"Ingestion one-shot cannot run with a maintenance command");
				});
	}

	@Test
	@DisplayName("Две служебные команды останавливают startup до создания adapters")
	void twoMaintenanceCommandsFailBeforeAdapterCreation() {
		withCommandLine(
				nonWebContextRunner,
				PARTITION_REBUILD_MODE,
				GENERATION_CLEANUP_MODE)
				.run(context -> {
					assertThat(context).hasFailed();
					assertThat(context.getStartupFailure())
							.hasRootCauseMessage(
									"Only one maintenance command can run in one process");
				});
	}

	private static WebApplicationContextRunner withDependencies(
			WebApplicationContextRunner runner
	) {
		return runner
				.withBean(
						IngestionCycleCoordinator.class,
						() -> mock(IngestionCycleCoordinator.class))
				.withBean(Clock.class, Clock::systemUTC)
				.withBean(PartitionRebuildService.class, () -> mock(PartitionRebuildService.class))
				.withBean(GenerationCleanupService.class, () -> mock(GenerationCleanupService.class))
				.withBean(ObjectMapper.class, IngestionTriggerModeSelectionTest::objectMapper)
				.withUserConfiguration(TriggerConfiguration.class);
	}

	private static ApplicationContextRunner withDependencies(
			ApplicationContextRunner runner
	) {
		return runner
				.withBean(
						IngestionCycleCoordinator.class,
						() -> mock(IngestionCycleCoordinator.class))
				.withBean(Clock.class, Clock::systemUTC)
				.withBean(PartitionRebuildService.class, () -> mock(PartitionRebuildService.class))
				.withBean(GenerationCleanupService.class, () -> mock(GenerationCleanupService.class))
				.withBean(ObjectMapper.class, IngestionTriggerModeSelectionTest::objectMapper)
				.withUserConfiguration(TriggerConfiguration.class);
	}

	private static ApplicationContextRunner withCommandLine(
			ApplicationContextRunner runner,
			String... arguments
	) {
		return runner.withInitializer(context -> context.getEnvironment()
				.getPropertySources()
				.addFirst(new SimpleCommandLinePropertySource(arguments)));
	}

	private static ObjectMapper objectMapper() {
		return JsonMapper.builder().findAndAddModules().build();
	}

	@Configuration(proxyBeanMethods = false)
	@EnableConfigurationProperties({
			PartitionRebuildCommandProperties.class,
			GenerationCleanupCommandProperties.class,
			GdeltIngestionProperties.class,
			BackendDataProperties.class
	})
	@Import({
			IngestionCycleActivity.class,
			IngestionOneShotRunner.class,
			PartitionRebuildCommandLineAdapter.class,
			GenerationCleanupCommandLineAdapter.class,
			AutomaticIngestionConfiguration.class,
			IngestionRuntimePropertiesValidator.class
	})
	static class TriggerConfiguration {
	}
}
