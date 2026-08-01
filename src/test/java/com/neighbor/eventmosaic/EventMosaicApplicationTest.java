package com.neighbor.eventmosaic;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupCommandLine;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildCommandLine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;

@DisplayName("Выбор режима запуска приложения")
class EventMosaicApplicationTest {

	@Test
	@DisplayName("Служебная команда запускается без HTTP-сервера")
	void maintenanceCommandUsesNonWebApplication() {
		assertThat(EventMosaicApplication.createApplication(true).getWebApplicationType())
				.isEqualTo(WebApplicationType.NONE);
	}

	@Test
	@DisplayName("Две служебные команды нельзя запустить в одном процессе")
	void simultaneousMaintenanceCommandsAreRejected() {
		String rebuildMode = "--" + PartitionRebuildCommandLine.PROPERTY_PREFIX
				+ ".mode=inspect-rebuild";
		String cleanupMode = "--" + GenerationCleanupCommandLine.PROPERTY_PREFIX
				+ ".mode=inspect-cleanup";

		assertThatThrownBy(() -> EventMosaicApplication.main(new String[]{
				rebuildMode,
				cleanupMode
		}))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("Only one maintenance command can run in one process");
	}

	@Test
	@DisplayName("Обычный запуск приложения сохраняет HTTP-режим")
	void regularStartupKeepsServletApplication() {
		assertThat(EventMosaicApplication.createApplication(false).getWebApplicationType())
				.isEqualTo(WebApplicationType.SERVLET);
	}
}
