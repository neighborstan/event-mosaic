package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.http.HttpHeaders;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Разбор HTTP Retry-After")
class HttpRetryAfterParserTest {

	private static final Instant NOW = Instant.parse("2026-08-01T12:00:00Z");
	private final HttpRetryAfterParser parser = new HttpRetryAfterParser(
			Clock.fixed(NOW, ZoneOffset.UTC));

	@Test
	@DisplayName("Поддерживает delay-seconds и будущую HTTP-date")
	void parsesSupportedValues() {
		assertThat(parser.parse(headers("120"))).isEqualTo(Duration.ofMinutes(2));
		assertThat(parser.parse(headers("Sat, 1 Aug 2026 12:05:00 GMT")))
				.isEqualTo(Duration.ofMinutes(5));
	}

	@Test
	@DisplayName("Некорректное и прошедшее значение не влияет на retry policy")
	void ignoresInvalidAndPastValues() {
		assertThat(parser.parse(headers("invalid"))).isZero();
		assertThat(parser.parse(headers("Sat, 1 Aug 2026 11:59:00 GMT"))).isZero();
		assertThat(parser.parse(HttpHeaders.of(Map.of(), (_, _) -> true))).isZero();
	}

	private static HttpHeaders headers(String value) {
		return HttpHeaders.of(
				Map.of("Retry-After", List.of(value)),
				(_, _) -> true);
	}
}
