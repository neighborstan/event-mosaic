package com.neighbor.eventmosaic.ingestion.trigger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Выбор единственного режима загрузки GDELT")
class IngestionRuntimeModeResolverTest {

	private final IngestionRuntimeModeResolver resolver = new IngestionRuntimeModeResolver();

	@Test
	@DisplayName("Обычный web-процесс использует включенный автоматический режим")
	void ordinaryWebRuntimeUsesEnabledAutomaticMode() {
		assertThat(resolve(true, true, false, false, false))
				.isEqualTo(IngestionRuntimeMode.AUTOMATIC);
	}

	@Test
	@DisplayName("Выключенный автоматический режим оставляет веб-процесс без загрузки")
	void disabledAutomaticModeKeepsWebRuntimeWithoutTrigger() {
		assertThat(resolve(true, false, false, false, false))
				.isEqualTo(IngestionRuntimeMode.DISABLED);
	}

	@Test
	@DisplayName("Явный однократный запуск подавляет автоматический режим")
	void explicitOneShotOverridesAutomaticMode() {
		assertThat(resolve(true, true, true, false, false))
				.isEqualTo(IngestionRuntimeMode.ONE_SHOT);
	}

	@Test
	@DisplayName("Служебная команда подавляет автоматический режим web-приложения")
	void maintenanceCommandOverridesAutomaticMode() {
		assertThat(resolve(false, true, false, true, false))
				.isEqualTo(IngestionRuntimeMode.MAINTENANCE);
		assertThat(resolve(false, true, false, false, true))
				.isEqualTo(IngestionRuntimeMode.MAINTENANCE);
	}

	@Test
	@DisplayName("Процесс без веб-сервера и служебной команды не включает загрузку")
	void nonWebRuntimeWithoutCommandRemainsDisabled() {
		assertThat(resolve(false, true, false, false, false))
				.isEqualTo(IngestionRuntimeMode.DISABLED);
	}

	@Test
	@DisplayName("Однократную загрузку нельзя совмещать со служебной командой")
	void oneShotCannotRunWithMaintenanceCommand() {
		assertThatThrownBy(() -> resolve(false, true, true, true, false))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Ingestion one-shot cannot run with a maintenance command");
		assertThatThrownBy(() -> resolve(false, true, true, false, true))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Ingestion one-shot cannot run with a maintenance command");
	}

	@Test
	@DisplayName("Две служебные команды нельзя выполнять в одном процессе")
	void twoMaintenanceCommandsAreRejected() {
		assertThatThrownBy(() -> resolve(false, true, false, true, true))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Only one maintenance command can run in one process");
	}

	private IngestionRuntimeMode resolve(
			boolean ordinaryWebRuntime,
			boolean automaticEnabled,
			boolean oneShotEnabled,
			boolean partitionRebuildRequested,
			boolean generationCleanupRequested
	) {
		return resolver.resolve(
				ordinaryWebRuntime,
				automaticEnabled,
				oneShotEnabled,
				partitionRebuildRequested,
				generationCleanupRequested);
	}
}
