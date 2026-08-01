package com.neighbor.eventmosaic.ingestion.source;

import java.net.http.HttpHeaders;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;
import org.springframework.stereotype.Component;

/** Разбирает стандартные delay-seconds и HTTP-date формы Retry-After. */
@Component
public final class HttpRetryAfterParser {

	private final Clock clock;

	/** Создает parser с единым application UTC Clock для HTTP-date. */
	public HttpRetryAfterParser(Clock clock) {
		this.clock = Objects.requireNonNull(clock, "clock must not be null");
	}

	/**
	 * Возвращает положительную задержку или zero для отсутствующего, прошедшего
	 * либо некорректного значения.
	 */
	public Duration parse(HttpHeaders headers) {
		Objects.requireNonNull(headers, "headers must not be null");
		return headers.firstValue("Retry-After")
				.map(String::trim)
				.map(this::parseValue)
				.orElse(Duration.ZERO);
	}

	private Duration parseValue(String value) {
		if (value.isEmpty()) {
			return Duration.ZERO;
		}
		try {
			long seconds = Long.parseLong(value);
			return seconds > 0 ? Duration.ofSeconds(seconds) : Duration.ZERO;
		}
		catch (NumberFormatException _) {
			return parseHttpDate(value);
		}
	}

	private Duration parseHttpDate(String value) {
		try {
			Instant retryAt = ZonedDateTime.parse(
					value,
					DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
			Duration delay = Duration.between(clock.instant(), retryAt);
			return delay.isNegative() || delay.isZero() ? Duration.ZERO : delay;
		}
		catch (DateTimeParseException | ArithmeticException _) {
			return Duration.ZERO;
		}
	}
}
