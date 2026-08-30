package com.neighbor.eventmosaic.ingestion.config;

import java.time.Duration;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Проверяет совместимость настроек automatic ingestion с общей deadline и
 * retry policy. Так приложение останавливается при небезопасной конфигурации
 * еще до первого запуска pipeline.
 */
@Component
public final class IngestionRuntimePropertiesValidator {

	private static final Duration LEASE_SAFETY_MARGIN = Duration.ofMinutes(3);

	/**
	 * Проверяет межгрупповые временные границы после Spring binding.
	 *
	 * @param ingestionProperties настройки GDELT ingestion
	 * @param backendDataProperties общие deadline и retry policy
	 */
	public IngestionRuntimePropertiesValidator(
			GdeltIngestionProperties ingestionProperties,
			BackendDataProperties backendDataProperties
	) {
		Objects.requireNonNull(
				ingestionProperties, "ingestionProperties must not be null");
		Objects.requireNonNull(
				backendDataProperties, "backendDataProperties must not be null");
		validate(ingestionProperties, backendDataProperties);
	}

	private static void validate(
			GdeltIngestionProperties ingestionProperties,
			BackendDataProperties backendData
	) {
		if (ingestionProperties.continuity().firstRunPolicy() == FirstRunPolicy.RECENT_WINDOW) {
			throw new IllegalArgumentException(
					"firstRunPolicy RECENT_WINDOW is not supported yet; use LATEST or FIXED");
		}
		validate(ingestionProperties.automatic(), backendData);
	}

	private static void validate(
			GdeltIngestionProperties.Automatic automatic,
			BackendDataProperties backendData
	) {
		Duration operationDeadline = backendData.operationDeadline();
		Duration minimumCycleLease = add(
				operationDeadline,
				LEASE_SAFETY_MARGIN,
				"operationDeadline and lease safety margin are too large");
		if (automatic.cycleLease().compareTo(minimumCycleLease) < 0) {
			throw new IllegalArgumentException(
					"automatic cycleLease must cover operationDeadline and 3 minute safety margin");
		}
		if (automatic.shutdownGrace().compareTo(operationDeadline) > 0) {
			throw new IllegalArgumentException(
					"automatic shutdownGrace must not exceed operationDeadline");
		}

		Duration minimumSchedulerStale = add(
				automatic.pollDelay().multipliedBy(2),
				operationDeadline,
				"pollDelay and operationDeadline are too large");
		Duration effectiveSchedulerStale = automatic.effectiveSchedulerStaleThreshold(
				operationDeadline);
		if (effectiveSchedulerStale.compareTo(minimumSchedulerStale) < 0) {
			throw new IllegalArgumentException(
					"effective scheduler stale threshold is shorter than two poll delays and operationDeadline");
		}

		Duration minimumSourceOutage;
		try {
			minimumSourceOutage = backendData.retry().maximumDelay().multipliedBy(2);
		}
		catch (ArithmeticException exception) {
			throw new IllegalArgumentException("maximumRetryDelay is too large", exception);
		}
		if (automatic.sourceOutageThreshold().compareTo(minimumSourceOutage) < 0) {
			throw new IllegalArgumentException(
					"automatic sourceOutageThreshold must cover two maximumRetryDelay intervals");
		}
	}

	private static Duration add(Duration left, Duration right, String overflowMessage) {
		try {
			return left.plus(right);
		}
		catch (ArithmeticException exception) {
			throw new IllegalArgumentException(overflowMessage, exception);
		}
	}
}
