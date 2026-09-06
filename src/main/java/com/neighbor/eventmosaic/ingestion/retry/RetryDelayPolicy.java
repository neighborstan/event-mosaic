package com.neighbor.eventmosaic.ingestion.retry;

import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.api.AutomaticRetryState;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Вычисляет bounded exponential backoff с симметричным jitter и ограниченным
 * увеличением по корректному HTTP Retry-After.
 */
@Component
public final class RetryDelayPolicy {

	private final BackendDataProperties.Retry properties;
	private final RetryJitterSource jitterSource;

	/** Создает policy из проверенных runtime defaults и jitter source. */
	public RetryDelayPolicy(
			BackendDataProperties properties,
			RetryJitterSource jitterSource
	) {
		this.properties = Objects.requireNonNull(properties, "properties must not be null").retry();
		this.jitterSource = Objects.requireNonNull(jitterSource, "jitterSource must not be null");
	}

	/**
	 * Возвращает due time после очередного retryable failure.
	 *
	 * @param now durable UTC-время failure
	 * @param consecutiveFailuresBefore число предыдущих подряд retryable failures
	 * @param retryAfter разрешенная сервером дополнительная нижняя граница
	 * @return bounded UTC due time
	 */
	public Instant retryNotBefore(
			Instant now,
			int consecutiveFailuresBefore,
			Duration retryAfter
	) {
		Objects.requireNonNull(now, "now must not be null");
		if (consecutiveFailuresBefore < 0) {
			throw new IllegalArgumentException("consecutiveFailuresBefore must not be negative");
		}
		Objects.requireNonNull(retryAfter, "retryAfter must not be null");
		if (retryAfter.isNegative()) {
			throw new IllegalArgumentException("retryAfter must not be negative");
		}

		Duration exponential = exponentialDelay(consecutiveFailuresBefore);
		double sample = jitterSource.nextSample();
		if (!Double.isFinite(sample) || sample < 0.0 || sample >= 1.0) {
			throw new IllegalStateException("retry jitter sample must be in [0.0, 1.0)");
		}
		double factor = 1.0 + (2.0 * sample - 1.0) * properties.jitterRatio();
		long jitteredNanos = Math.max(1L, Math.round(exponential.toNanos() * factor));
		Duration delay = Duration.ofNanos(jitteredNanos);
		if (retryAfter.compareTo(delay) > 0) {
			delay = retryAfter;
		}
		if (delay.compareTo(properties.maximumDelay()) > 0) {
			delay = properties.maximumDelay();
		}
		return now.plus(delay);
	}

	private Duration exponentialDelay(int consecutiveFailuresBefore) {
		double multiplier = Math.pow(properties.multiplier(), consecutiveFailuresBefore);
		double nanos = properties.initialDelay().toNanos() * multiplier;
		long maximumNanos = properties.maximumDelay().toNanos();
		if (!Double.isFinite(nanos) || nanos >= maximumNanos) {
			return properties.maximumDelay();
		}
		return Duration.ofNanos(Math.max(1L, Math.round(nanos)));
	}

	/** Возвращает конец паузы между сериями: ровно максимальную задержку без случайного отклонения. */
	public Instant cooldownNotBefore(Instant now) {
		return Objects.requireNonNull(now, "now must not be null").plus(properties.maximumDelay());
	}

	/** Выбирает обычную задержку либо полную паузу, если текущая серия уже израсходовала все повторы. */
	public Instant retryNotBefore(Instant now, AutomaticRetryState retry, Duration retryAfter) {
		Objects.requireNonNull(retry, "retry must not be null");
		Objects.requireNonNull(retryAfter, "retryAfter must not be null");
		if (retryAfter.isNegative()) {
			throw new IllegalArgumentException("retryAfter must not be negative");
		}
		return retry.exhausted() ? cooldownNotBefore(now)
				: retryNotBefore(now, retry.consecutiveRetryableFailures(), retryAfter);
	}
}
