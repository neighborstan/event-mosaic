package com.neighbor.eventmosaic.ingestion.api;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Явный аргумент запуска очистки технического поколения")
class GenerationCleanupCommandLineTest {

	@Test
	@DisplayName("Явный параметр режима в командной строке включает очистку")
	void explicitModeOptionRequestsCleanup() {
		String modeOption = "--" + GenerationCleanupCommandLine.PROPERTY_PREFIX + ".mode";

		assertThat(GenerationCleanupCommandLine.isRequested(new String[]{
				modeOption + "=inspect-cleanup"
		})).isTrue();
		assertThat(GenerationCleanupCommandLine.isRequested(new String[]{
				modeOption,
				"inspect-cleanup"
		})).isTrue();
	}

	@Test
	@DisplayName("Скрытая настройка без аргумента командной строки не включает очистку")
	void hiddenConfigurationDoesNotRequestCleanup() {
		assertThat(GenerationCleanupCommandLine.isRequested(new String[]{
				GenerationCleanupCommandLine.PROPERTY_PREFIX + ".mode=inspect-cleanup",
				"--spring.profiles.active=local"
		})).isFalse();
	}
}
