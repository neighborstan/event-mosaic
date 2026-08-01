package com.neighbor.eventmosaic.ingestion.retry;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.ingestion.GdeltTestFixtures;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Политика delayed retry")
class RetryDelayPolicyTest {

	private static final Instant NOW = Instant.parse("2026-08-01T12:00:00Z");

	@Test
	@DisplayName("Fixed jitter дает последовательность 1, 2, 4 минуты и ограничивает максимум")
	void fixedJitterProducesBoundedExponentialSequence() {
		RetryDelayPolicy policy = policy(0.5);

		assertThat(policy.retryNotBefore(NOW, 0, Duration.ZERO))
				.isEqualTo(NOW.plus(Duration.ofMinutes(1)));
		assertThat(policy.retryNotBefore(NOW, 1, Duration.ZERO))
				.isEqualTo(NOW.plus(Duration.ofMinutes(2)));
		assertThat(policy.retryNotBefore(NOW, 2, Duration.ZERO))
				.isEqualTo(NOW.plus(Duration.ofMinutes(4)));
		assertThat(policy.retryNotBefore(NOW, 20, Duration.ZERO))
				.isEqualTo(NOW.plus(Duration.ofMinutes(15)));
	}

	@Test
	@DisplayName("Симметричный jitter остается в десятипроцентной границе")
	void symmetricJitterStaysWithinConfiguredBoundary() {
		assertThat(policy(0.0).retryNotBefore(NOW, 0, Duration.ZERO))
				.isEqualTo(NOW.plusSeconds(54));
		assertThat(policy(Math.nextDown(1.0)).retryNotBefore(NOW, 0, Duration.ZERO))
				.isEqualTo(NOW.plusSeconds(66));
	}

	@Test
	@DisplayName("Retry-After только увеличивает задержку и ограничивается максимумом")
	void retryAfterCanOnlyIncreaseDelayWithinMaximum() {
		RetryDelayPolicy policy = policy(0.5);

		assertThat(policy.retryNotBefore(NOW, 0, Duration.ofSeconds(30)))
				.isEqualTo(NOW.plus(Duration.ofMinutes(1)));
		assertThat(policy.retryNotBefore(NOW, 0, Duration.ofMinutes(10)))
				.isEqualTo(NOW.plus(Duration.ofMinutes(10)));
		assertThat(policy.retryNotBefore(NOW, 0, Duration.ofHours(1)))
				.isEqualTo(NOW.plus(Duration.ofMinutes(15)));
	}

	private static RetryDelayPolicy policy(double sample) {
		return new RetryDelayPolicy(
				GdeltTestFixtures.backendDataProperties(),
				() -> sample);
	}
}
