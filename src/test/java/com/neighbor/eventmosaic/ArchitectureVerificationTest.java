package com.neighbor.eventmosaic;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

@DisplayName("Проверка модульной архитектуры")
class ArchitectureVerificationTest {

	@Test
	@DisplayName("Модульная структура приложения соответствует правилам")
	void verifiesApplicationModules() {
		ApplicationModules.of(EventMosaicApplication.class).verify();
	}
}
