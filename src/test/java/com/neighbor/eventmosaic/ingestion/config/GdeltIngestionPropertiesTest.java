package com.neighbor.eventmosaic.ingestion.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Настройки загрузки GDELT")
class GdeltIngestionPropertiesTest {

	@Test
	@DisplayName("Политика latest принимается с безопасными лимитами")
	void acceptsLatestPolicyWithSafeLimits() {
		assertThatCode(() -> properties(FirstRunPolicy.LATEST, null, officialBaseUri()))
				.doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Политика текущего окна не принимает фиксированную стартовую точку")
	void recentWindowRejectsFixedStartPoint() {
		URI baseUri = officialBaseUri();
		Instant startAt = Instant.parse("2026-07-20T12:00:00Z");

		assertThatCode(() -> properties(FirstRunPolicy.RECENT_WINDOW, null, baseUri))
				.doesNotThrowAnyException();
		assertThatThrownBy(() -> properties(FirstRunPolicy.RECENT_WINDOW, startAt, baseUri))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("firstRunStartAt is allowed only for FIXED firstRunPolicy");
	}

	@Test
	@DisplayName("Политика latest также не принимает фиксированную стартовую точку")
	void latestRejectsFixedStartPoint() {
		assertThatThrownBy(() -> properties(
				FirstRunPolicy.LATEST,
				Instant.parse("2026-07-20T12:00:00Z"),
				officialBaseUri()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("firstRunStartAt is allowed only for FIXED firstRunPolicy");
	}

	@Test
	@DisplayName("Базовый URI должен быть официальным HTTPS-каталогом GDELT")
	void requiresHttpsDirectoryBaseUri() {
		URI untrustedBaseUri = URI.create("https://example.test/gdeltv2/");

		assertThatThrownBy(() -> properties(FirstRunPolicy.LATEST, null, untrustedBaseUri))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("official GDELT HTTPS GCS");
	}

	@Test
	@DisplayName("Фиксированная стартовая точка обязательна и кратна пятнадцати минутам")
	void requiresFixedStartPointOnQuarterHour() {
		URI baseUri = officialBaseUri();
		Instant offBoundary = Instant.parse("2026-07-20T12:01:00Z");

		assertThatThrownBy(() -> properties(FirstRunPolicy.FIXED, null, baseUri))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("required");

		assertThatThrownBy(() -> properties(
				FirstRunPolicy.FIXED,
				offBoundary,
				baseUri))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("15-minute");
	}

	@Test
	@DisplayName("Неположительный лимит отклоняется")
	void rejectsNonPositiveLimit() {
		Duration timeout = Duration.ofSeconds(1);

		assertThatThrownBy(() -> new GdeltIngestionProperties.Http(
				0,
				1024,
				timeout,
				timeout))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("maxManifestBytes");
	}

	@Test
	@DisplayName("Прямое создание настроек проверяет обязательный timeout без Spring")
	void directConstructionRequiresRecoveryLeaseWithoutSpringValidation() {
		assertThatThrownBy(() -> new GdeltIngestionProperties.Continuity(
				null,
				FirstRunPolicy.LATEST,
				null))
				.isInstanceOf(NullPointerException.class)
				.hasMessage("recoveryLease must not be null");
	}

	@Test
	@DisplayName("Автоматический режим получает безопасные промежуточные значения по умолчанию")
	void automaticModeUsesSafeCheckpointDefaults() {
		GdeltIngestionProperties.Automatic automatic = properties(
				FirstRunPolicy.LATEST,
				null,
				officialBaseUri()).automatic();

		assertThat(automatic.enabled()).isFalse();
		assertThat(automatic.pollDelay()).isEqualTo(Duration.ofMinutes(1));
		assertThat(automatic.cycleLease()).isEqualTo(Duration.ofMinutes(15));
		assertThat(automatic.shutdownGrace()).isEqualTo(Duration.ofSeconds(30));
		assertThat(automatic.schedulerStaleBase()).isEqualTo(Duration.ofMinutes(5));
		assertThat(automatic.sourceOutageThreshold()).isEqualTo(Duration.ofMinutes(30));
		assertThat(automatic.dueWorkLimit()).isEqualTo(256);
		assertThat(automatic.receiptAudit().interval()).isEqualTo(Duration.ofMinutes(15));
		assertThat(automatic.receiptAudit().batchSize()).isEqualTo(2);
	}

	@Test
	@DisplayName("Лимит срочной работы принимает только значения от одного до 1024")
	void dueWorkLimitHasHardRange() {
		assertThatThrownBy(() -> automaticWithDueWorkLimit(0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("dueWorkLimit must be between 1 and 1024");
		assertThatThrownBy(() -> automaticWithDueWorkLimit(1025))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("dueWorkLimit must be between 1 and 1024");
		assertThatCode(() -> automaticWithDueWorkLimit(1)).doesNotThrowAnyException();
		assertThatCode(() -> automaticWithDueWorkLimit(1024)).doesNotThrowAnyException();
	}

	@Test
	@DisplayName("Эффективный порог scheduler учитывает два опроса и deadline")
	void effectiveSchedulerStaleThresholdCoversCycleAllowance() {
		GdeltIngestionProperties.Automatic automatic = automaticWithDueWorkLimit(256);

		assertThat(automatic.effectiveSchedulerStaleThreshold(Duration.ofMinutes(12)))
				.isEqualTo(Duration.ofMinutes(14));
	}

	private static GdeltIngestionProperties.Automatic automaticWithDueWorkLimit(int limit) {
		return new GdeltIngestionProperties.Automatic(
				false,
				Duration.ofMinutes(1),
				Duration.ofMinutes(15),
				Duration.ofSeconds(30),
				Duration.ofMinutes(5),
				Duration.ofMinutes(30),
				limit,
				new GdeltIngestionProperties.ReceiptAudit(Duration.ofMinutes(15), 2));
	}

	private static GdeltIngestionProperties properties(
			FirstRunPolicy policy,
			Instant startAt,
			URI baseUri
	) {
		return new GdeltIngestionProperties(
				baseUri,
				Path.of("staging"),
				new GdeltIngestionProperties.Http(
						64 * 1024,
						512 * 1024,
						Duration.ofSeconds(1),
						Duration.ofSeconds(2)),
				new GdeltIngestionProperties.Zip(4, 1024, 2048),
				new GdeltIngestionProperties.Continuity(
						Duration.ofMinutes(15),
						policy,
						startAt),
				automaticWithDueWorkLimit(256),
				false
		);
	}

	private static URI officialBaseUri() {
		return URI.create("https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/");
	}
}
