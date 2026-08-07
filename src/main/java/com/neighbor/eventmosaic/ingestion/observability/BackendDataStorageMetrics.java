package com.neighbor.eventmosaic.ingestion.observability;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.springframework.stereotype.Component;

/** Публикует free-space, pressure и blocked-operation с bounded resource tag. */
@Component
public class BackendDataStorageMetrics {

	private static final long REFRESH_NANOS = Duration.ofSeconds(5).toNanos();

	private final MeterRegistry meterRegistry;
	private final ResourceGauge staging = new ResourceGauge(StorageResource.STAGING);
	private final ResourceGauge elasticsearch = new ResourceGauge(StorageResource.ELASTICSEARCH);
	private final AtomicReference<Function<StorageResource, StoragePressureObservation>>
			observationSource = new AtomicReference<>();

	/** Регистрирует фиксированные gauges для staging и Elasticsearch. */
	public BackendDataStorageMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(
				meterRegistry,
				"meterRegistry must not be null");
		register(StorageResource.STAGING, staging);
		register(StorageResource.ELASTICSEARCH, elasticsearch);
	}

	/** Подключает bounded pull-source, чтобы scrape не зависел от health polling. */
	void bind(Function<StorageResource, StoragePressureObservation> source) {
		Objects.requireNonNull(source, "source must not be null");
		if (!observationSource.compareAndSet(null, source)) {
			throw new IllegalStateException("storage metrics source is already bound");
		}
	}

	/** Обновляет gauges результатом общей storage проверки. */
	public void observe(StoragePressureObservation observation) {
		Objects.requireNonNull(observation, "observation must not be null");
		resourceGauge(observation.resource()).set(observation, System.nanoTime());
	}

	/** Учитывает отклоненную новую растущую operation. */
	public void blocked(StorageResource resource) {
		meterRegistry.counter(
				"event_mosaic.storage.pressure.blocks",
				"resource",
				tag(resource)).increment();
	}

	private void register(StorageResource resource, ResourceGauge gauge) {
		String resourceTag = tag(resource);
		Gauge.builder("event_mosaic.storage.available", gauge, this::availableBytes)
				.baseUnit("bytes")
				.tag("resource", resourceTag)
				.register(meterRegistry);
		Gauge.builder("event_mosaic.storage.pressure", gauge, this::pressure)
				.tag("resource", resourceTag)
				.register(meterRegistry);
	}

	private ResourceGauge resourceGauge(StorageResource resource) {
		return switch (resource) {
			case STAGING -> staging;
			case ELASTICSEARCH -> elasticsearch;
		};
	}

	private double availableBytes(ResourceGauge gauge) {
		refreshIfNeeded(gauge);
		long value = gauge.availableBytes.get();
		return value < 0 ? Double.NaN : value;
	}

	private double pressure(ResourceGauge gauge) {
		refreshIfNeeded(gauge);
		return switch (gauge.state.get()) {
			case AVAILABLE -> 0.0;
			case PRESSURE -> 1.0;
			case UNAVAILABLE -> Double.NaN;
		};
	}

	private void refreshIfNeeded(ResourceGauge gauge) {
		long now = System.nanoTime();
		if (gauge.isFresh(now)) {
			return;
		}
		Function<StorageResource, StoragePressureObservation> source = observationSource.get();
		if (source == null) {
			return;
		}
		synchronized (gauge) {
			if (gauge.isFresh(now)) {
				return;
			}
			try {
				StoragePressureObservation observation = Objects.requireNonNull(
						source.apply(gauge.resource),
						"storage observation must not be null");
				if (observation.resource() != gauge.resource) {
					throw new IllegalStateException("storage observation resource mismatch");
				}
				gauge.set(observation, System.nanoTime());
			}
			catch (RuntimeException _) {
				gauge.setUnavailable(System.nanoTime());
			}
		}
	}

	private static String tag(StorageResource resource) {
		return resource.name().toLowerCase(Locale.ROOT);
	}

	private static final class ResourceGauge {

		private final StorageResource resource;
		private final AtomicLong availableBytes = new AtomicLong(-1);
		private final AtomicReference<StoragePressureState> state =
				new AtomicReference<>(StoragePressureState.UNAVAILABLE);
		private final AtomicLong observedAtNanos = new AtomicLong();

		private ResourceGauge(StorageResource resource) {
			this.resource = resource;
		}

		private void set(StoragePressureObservation observation, long observedAt) {
			availableBytes.set(observation.availableBytes());
			state.set(observation.state());
			observedAtNanos.set(observedAt);
		}

		private void setUnavailable(long observedAt) {
			availableBytes.set(-1);
			state.set(StoragePressureState.UNAVAILABLE);
			observedAtNanos.set(observedAt);
		}

		private boolean isFresh(long now) {
			long observedAt = observedAtNanos.get();
			return observedAt != 0 && now - observedAt < REFRESH_NANOS;
		}
	}
}
