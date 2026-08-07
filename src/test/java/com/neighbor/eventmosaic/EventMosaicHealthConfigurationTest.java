package com.neighbor.eventmosaic;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.endpoint.Show;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.health.autoconfigure.actuate.endpoint.HealthEndpointProperties;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

@DisplayName("Конфигурация operational health groups")
class EventMosaicHealthConfigurationTest {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withInitializer(new ConfigDataApplicationContextInitializer());

	@Test
	@DisplayName("Liveness, readiness и pipeline получают явные изолированные группы")
	void bindsExplicitHealthGroups() {
		contextRunner.run(context -> {
			assertThat(context.getEnvironment().getProperty(
					"management.endpoint.health.probes.enabled",
					Boolean.class)).isTrue();
			HealthEndpointProperties properties = Binder.get(context.getEnvironment())
					.bind("management.endpoint.health", HealthEndpointProperties.class)
					.orElseThrow(() -> new AssertionError("Health properties were not bound"));

			assertThat(properties.getStatus().getOrder()).containsExactlyElementsOf(List.of(
					"down",
					"out-of-service",
					"degraded",
					"unknown",
					"up"));
			assertThat(properties.getGroup()).containsOnlyKeys(
					"liveness",
					"readiness",
					"pipeline");
			assertThat(properties.getGroup().get("liveness").getInclude())
					.containsExactlyInAnyOrderElementsOf(Set.of("livenessState"));
			assertThat(properties.getGroup().get("readiness").getInclude())
					.containsExactlyInAnyOrderElementsOf(Set.of(
							"readinessState",
							"db",
							"elasticsearch",
							"staging"));
			assertThat(properties.getGroup().get("pipeline").getInclude())
					.containsExactlyInAnyOrderElementsOf(Set.of("backendDataPipeline"));
			assertThat(properties.getGroup().get("pipeline").getShowDetails())
					.isEqualTo(Show.ALWAYS);
		});
	}
}
