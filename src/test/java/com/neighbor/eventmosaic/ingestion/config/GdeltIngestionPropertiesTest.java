package com.neighbor.eventmosaic.ingestion.config;

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
				false
		);
	}

	private static URI officialBaseUri() {
		return URI.create("https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/");
	}
}
