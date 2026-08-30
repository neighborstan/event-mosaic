package com.neighbor.eventmosaic;

import static org.assertj.core.api.Assertions.assertThat;

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
	@DisplayName("Обычный запуск приложения сохраняет HTTP-режим")
	void regularStartupKeepsServletApplication() {
		assertThat(EventMosaicApplication.createApplication(false).getWebApplicationType())
				.isEqualTo(WebApplicationType.SERVLET);
	}
}
