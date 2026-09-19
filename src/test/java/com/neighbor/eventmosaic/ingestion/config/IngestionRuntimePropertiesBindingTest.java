package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@DisplayName("Привязка настроек автоматической загрузки")
class IngestionRuntimePropertiesBindingTest {

	private static final String PREFIX = "event-mosaic.ingestion.gdelt.automatic.";
	private static final String CONTINUITY_PREFIX = "event-mosaic.ingestion.gdelt.continuity.";

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
			.withUserConfiguration(PropertiesConfiguration.class);

	@Test
	@DisplayName("Spring создает полный набор безопасных значений по умолчанию")
	void bindsSafeDefaults() {
		contextRunner.run(context -> {
			assertThat(context).hasNotFailed();
			GdeltIngestionProperties properties = context
					.getBean(GdeltIngestionProperties.class);
			GdeltIngestionProperties.Automatic automatic = properties.automatic();

			assertThat(properties.continuity().firstRunPolicy())
					.isEqualTo(FirstRunPolicy.RECENT_WINDOW);
			assertThat(automatic.enabled()).isTrue();
			assertThat(automatic.pollDelay()).isEqualTo(Duration.ofMinutes(1));
			assertThat(automatic.cycleLease()).isEqualTo(Duration.ofMinutes(15));
			assertThat(automatic.shutdownGrace()).isEqualTo(Duration.ofSeconds(30));
			assertThat(automatic.schedulerStaleBase()).isEqualTo(Duration.ofMinutes(5));
			assertThat(automatic.sourceOutageThreshold()).isEqualTo(Duration.ofMinutes(30));
			assertThat(automatic.dueWorkLimit()).isEqualTo(256);
			assertThat(automatic.receiptAudit().interval()).isEqualTo(Duration.ofMinutes(15));
			assertThat(automatic.receiptAudit().batchSize()).isEqualTo(2);
			assertThat(automatic.effectiveSchedulerStaleThreshold(Duration.ofMinutes(12)))
					.isEqualTo(Duration.ofMinutes(14));
		});
	}

	@Test
	@DisplayName("Spring привязывает все внешние настройки automatic ingestion")
	void bindsEveryAutomaticProperty() {
		contextRunner
				.withPropertyValues(
						PREFIX + "enabled=true",
						PREFIX + "poll-delay=2m",
						PREFIX + "cycle-lease=16m",
						PREFIX + "shutdown-grace=1m",
						PREFIX + "scheduler-stale-base=20m",
						PREFIX + "source-outage-threshold=40m",
						PREFIX + "due-work-limit=512",
						PREFIX + "receipt-audit.interval=30m",
						PREFIX + "receipt-audit.batch-size=1")
				.run(context -> {
					assertThat(context).hasNotFailed();
					GdeltIngestionProperties.Automatic automatic = context
							.getBean(GdeltIngestionProperties.class)
							.automatic();

					assertThat(automatic.enabled()).isTrue();
					assertThat(automatic.pollDelay()).isEqualTo(Duration.ofMinutes(2));
					assertThat(automatic.cycleLease()).isEqualTo(Duration.ofMinutes(16));
					assertThat(automatic.shutdownGrace()).isEqualTo(Duration.ofMinutes(1));
					assertThat(automatic.schedulerStaleBase()).isEqualTo(Duration.ofMinutes(20));
					assertThat(automatic.sourceOutageThreshold()).isEqualTo(Duration.ofMinutes(40));
					assertThat(automatic.dueWorkLimit()).isEqualTo(512);
					assertThat(automatic.receiptAudit().interval()).isEqualTo(Duration.ofMinutes(30));
					assertThat(automatic.receiptAudit().batchSize()).isEqualTo(1);
					assertThat(automatic.effectiveSchedulerStaleThreshold(Duration.ofMinutes(12)))
							.isEqualTo(Duration.ofMinutes(20));
				});
	}

	@Test
	@DisplayName("Слишком короткий cycle lease останавливает startup")
	void rejectsCycleLeaseWithoutSafetyMargin() {
		assertStartupFailure(
				PREFIX + "cycle-lease=PT14M59S",
				"automatic cycleLease must cover operationDeadline and 3 minute safety margin");
	}

	@Test
	@DisplayName("Ожидание shutdown не может быть длиннее operation deadline")
	void rejectsShutdownGraceLongerThanDeadline() {
		assertStartupFailure(
				PREFIX + "shutdown-grace=13m",
				"automatic shutdownGrace must not exceed operationDeadline");
	}

	@Test
	@DisplayName("Порог недоступности источника покрывает две максимальные retry-задержки")
	void rejectsSourceOutageShorterThanRetryAllowance() {
		assertStartupFailure(
				PREFIX + "source-outage-threshold=PT29M59S",
				"automatic sourceOutageThreshold must cover two maximumRetryDelay intervals");
	}

	@Test
	@DisplayName("Spring отклоняет лимит срочной работы вне жесткого диапазона")
	void rejectsDueWorkLimitOutsideHardRange() {
		assertStartupFailure(
				PREFIX + "due-work-limit=0",
				"dueWorkLimit must be between 1 and 1024");
		assertStartupFailure(
				PREFIX + "due-work-limit=1025",
				"dueWorkLimit must be between 1 and 1024");
	}

	@Test
	@DisplayName("Spring принимает политику текущего окна как first-run default")
	void acceptsRecentWindowPolicyWithRuntimeSupport() {
		contextRunner
				.withPropertyValues(CONTINUITY_PREFIX + "first-run-policy=RECENT_WINDOW")
				.run(context -> {
					assertThat(context).hasNotFailed();
					assertThat(context.getBean(GdeltIngestionProperties.class)
							.continuity()
							.firstRunPolicy())
							.isEqualTo(FirstRunPolicy.RECENT_WINDOW);
				});
	}

	private void assertStartupFailure(String property, String message) {
		contextRunner.withPropertyValues(property).run(context -> {
			assertThat(context).hasFailed();
			assertThat(context.getStartupFailure()).hasRootCauseMessage(message);
		});
	}

	@Configuration(proxyBeanMethods = false)
	@EnableConfigurationProperties({
		GdeltIngestionProperties.class,
		BackendDataProperties.class
	})
	@Import(IngestionRuntimePropertiesValidator.class)
	static class PropertiesConfiguration {
	}
}
