package com.neighbor.eventmosaic;

import org.junit.jupiter.api.Test;
import org.springframework.modulith.core.ApplicationModules;

class ArchitectureVerificationTest {

	@Test
	void verifiesApplicationModules() {
		ApplicationModules.of(EventMosaicApplication.class).verify();
	}
}
