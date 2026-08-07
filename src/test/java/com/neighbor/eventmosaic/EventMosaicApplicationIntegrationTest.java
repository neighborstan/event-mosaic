package com.neighbor.eventmosaic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.health.actuate.endpoint.HealthEndpoint;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("Интеграционный запуск контекста Event Mosaic")
class EventMosaicApplicationIntegrationTest {

	@Autowired
	private HealthEndpoint healthEndpoint;

	@Test
	@DisplayName("Liveness, readiness и pipeline health доступны отдельно")
	void contextLoadsAndInfrastructureIsHealthy() {
		assertThat(healthEndpoint.health().getStatus()).isEqualTo(Status.UP);
		assertThat(healthEndpoint.healthForPath("liveness").getStatus())
				.isEqualTo(Status.UP);
		assertThat(healthEndpoint.healthForPath("readiness").getStatus())
				.isEqualTo(Status.UP);
		assertThat(healthEndpoint.healthForPath("pipeline").getStatus())
				.isEqualTo(Status.UP);
	}

}
