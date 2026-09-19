package com.neighbor.eventmosaic.ingestion.trigger;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import java.util.Objects;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.CommandLinePropertySource;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.web.context.WebApplicationContext;

/**
 * Применяет единый выбор режима к Spring-компонентам, которые запускают
 * загрузку или служебную операцию. Условия выполняются до регистрации bean,
 * поэтому конфликтующие команды не создают ни один adapter. Maintenance mode
 * принимается только из {@code commandLineArgs}, а не из постоянной настройки.
 */
public abstract class IngestionRuntimeModeCondition implements Condition {

	private static final IngestionRuntimeModeResolver RESOLVER =
			new IngestionRuntimeModeResolver();
	private static final String ONE_SHOT_ENABLED =
			"event-mosaic.ingestion.gdelt.one-shot-enabled";
	private static final String AUTOMATIC_ENABLED =
			"event-mosaic.ingestion.gdelt.automatic.enabled";
	private static final String PARTITION_REBUILD_MODE =
			PartitionRebuildCommandLine.PROPERTY_PREFIX + ".mode";
	private static final String GENERATION_CLEANUP_MODE =
			GenerationCleanupCommandLine.PROPERTY_PREFIX + ".mode";

	private final IngestionRuntimeMode expectedMode;
	private final ExplicitCommand expectedCommand;

	IngestionRuntimeModeCondition(
			IngestionRuntimeMode expectedMode,
			ExplicitCommand expectedCommand
	) {
		this.expectedMode = Objects.requireNonNull(expectedMode, "expectedMode must not be null");
		this.expectedCommand = Objects.requireNonNull(
				expectedCommand, "expectedCommand must not be null");
	}

	@Override
	public final boolean matches(
			ConditionContext context,
			AnnotatedTypeMetadata metadata
	) {
		ModeRequest request = ModeRequest.from(context);
		IngestionRuntimeMode mode = RESOLVER.resolve(
				request.ordinaryWebRuntime(),
				request.automaticEnabled(),
				request.oneShotEnabled(),
				request.partitionRebuildRequested(),
				request.generationCleanupRequested());
		return mode == expectedMode && expectedCommand.matches(request);
	}

	/** Выбирает bean однократного запуска ingestion. */
	public static final class OneShot extends IngestionRuntimeModeCondition {

		/** Создает условие для режима одного ingestion cycle. */
		public OneShot() {
			super(IngestionRuntimeMode.ONE_SHOT, ExplicitCommand.NONE);
		}
	}

	/** Выбирает adapter автоматического запуска в обычном web-процессе. */
	public static final class Automatic extends IngestionRuntimeModeCondition {

		/** Создает условие для автоматического режима. */
		public Automatic() {
			super(IngestionRuntimeMode.AUTOMATIC, ExplicitCommand.NONE);
		}
	}

	/** Выбирает bean команды восстановления partition. */
	public static final class PartitionRebuild extends IngestionRuntimeModeCondition {

		/** Создает условие для явно переданной rebuild-команды. */
		public PartitionRebuild() {
			super(IngestionRuntimeMode.MAINTENANCE, ExplicitCommand.PARTITION_REBUILD);
		}
	}

	/** Выбирает bean команды очистки technical generation. */
	public static final class GenerationCleanup extends IngestionRuntimeModeCondition {

		/** Создает условие для явно переданной cleanup-команды. */
		public GenerationCleanup() {
			super(IngestionRuntimeMode.MAINTENANCE, ExplicitCommand.GENERATION_CLEANUP);
		}
	}

	private enum ExplicitCommand {
		NONE {
			@Override
			boolean matches(ModeRequest request) {
				return true;
			}
		},
		PARTITION_REBUILD {
			@Override
			boolean matches(ModeRequest request) {
				return request.partitionRebuildRequested();
			}
		},
		GENERATION_CLEANUP {
			@Override
			boolean matches(ModeRequest request) {
				return request.generationCleanupRequested();
			}
		};

		abstract boolean matches(ModeRequest request);
	}

	private record ModeRequest(
			boolean ordinaryWebRuntime,
			boolean automaticEnabled,
			boolean oneShotEnabled,
			boolean partitionRebuildRequested,
			boolean generationCleanupRequested
	) {

		private static ModeRequest from(ConditionContext context) {
			ConfigurableEnvironment environment = requireConfigurableEnvironment(context);
			PropertySource<?> commandLine = environment.getPropertySources().get(
					CommandLinePropertySource.COMMAND_LINE_PROPERTY_SOURCE_NAME);
			boolean partitionRebuildRequested = contains(commandLine, PARTITION_REBUILD_MODE);
			boolean generationCleanupRequested = contains(commandLine, GENERATION_CLEANUP_MODE);
			rejectHiddenMaintenanceMode(
					environment,
					partitionRebuildRequested,
					PARTITION_REBUILD_MODE);
			rejectHiddenMaintenanceMode(
					environment,
					generationCleanupRequested,
					GENERATION_CLEANUP_MODE);
			return new ModeRequest(
					context.getResourceLoader() instanceof WebApplicationContext,
					environment.getProperty(AUTOMATIC_ENABLED, Boolean.class, true),
					environment.getProperty(ONE_SHOT_ENABLED, Boolean.class, false),
					partitionRebuildRequested,
					generationCleanupRequested);
		}

		private static void rejectHiddenMaintenanceMode(
				ConfigurableEnvironment environment,
				boolean commandLineRequested,
				String propertyName
		) {
			if (!commandLineRequested && environment.containsProperty(propertyName)) {
				throw new IllegalArgumentException(
						"Maintenance mode must be provided through commandLineArgs: "
								+ propertyName);
			}
		}

		private static ConfigurableEnvironment requireConfigurableEnvironment(
				ConditionContext context
		) {
			if (context.getEnvironment() instanceof ConfigurableEnvironment configurable) {
				return configurable;
			}
			throw new IllegalStateException("Spring environment must expose property sources");
		}

		private static boolean contains(PropertySource<?> source, String propertyName) {
			return source != null && source.containsProperty(propertyName);
		}
	}
}
