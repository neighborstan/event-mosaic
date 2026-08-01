package com.neighbor.eventmosaic.ingestion.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import java.time.Duration;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Проверенные operational defaults Backend Data MVP для partitioning, retry,
 * deadline, receipt verification и безопасного maintenance.
 *
 * @param partitionInterval длительность logical partition
 * @param retry bounded automatic retry policy
 * @param operationDeadline общая monotonic deadline одного one-shot cycle
 * @param receiptPageSize максимальный размер страницы receipt verification
 * @param diskPressure минимальные резервы свободного места
 * @param rebuild сроки подтвержденного плана и maintenance ownership
 * @param cleanup окна формирования maintenance candidates
 */
@Validated
@ConfigurationProperties("event-mosaic.backend-data")
public record BackendDataProperties(
		@DefaultValue("P7D") @NotNull Duration partitionInterval,
		@DefaultValue @Valid @NotNull Retry retry,
		@DefaultValue("12m") @NotNull Duration operationDeadline,
		@DefaultValue("500") @Positive @Max(10_000) int receiptPageSize,
		@DefaultValue @Valid @NotNull DiskPressure diskPressure,
		@DefaultValue @Valid @NotNull Rebuild rebuild,
		@DefaultValue @Valid @NotNull Cleanup cleanup
) {

	/** Проверяет обязательные группы и положительные временные границы. */
	public BackendDataProperties {
		requirePositive(partitionInterval, "partitionInterval");
		if (!Duration.ofDays(7).equals(partitionInterval)) {
			throw new IllegalArgumentException("partitionInterval must remain P7D");
		}
		Objects.requireNonNull(retry, "retry must not be null");
		requirePositive(operationDeadline, "operationDeadline");
		if (receiptPageSize <= 0 || receiptPageSize > 10_000) {
			throw new IllegalArgumentException("receiptPageSize must be between 1 and 10000");
		}
		Objects.requireNonNull(diskPressure, "diskPressure must not be null");
		Objects.requireNonNull(rebuild, "rebuild must not be null");
		if (rebuild.ownershipLease().compareTo(operationDeadline) <= 0) {
			throw new IllegalArgumentException(
					"rebuild ownershipLease must exceed operationDeadline");
		}
		Objects.requireNonNull(cleanup, "cleanup must not be null");
	}

	/**
	 * Bounded retry policy, сохраняемая вместе с current operation state.
	 *
	 * @param initialDelay начальная задержка
	 * @param multiplier множитель экспоненциальной задержки
	 * @param maximumDelay максимальная задержка, включая Retry-After
	 * @param jitterRatio симметричная доля bounded jitter
	 * @param automaticRetryLimit число automatic retries после initial claim
	 */
	public record Retry(
			@DefaultValue("1m") @NotNull Duration initialDelay,
			@DefaultValue("2.0") double multiplier,
			@DefaultValue("15m") @NotNull Duration maximumDelay,
			@DefaultValue("0.1") double jitterRatio,
			@DefaultValue("3") @PositiveOrZero @Max(100) int automaticRetryLimit
	) {

		/** Проверяет bounded policy и согласованность начальной и максимальной задержки. */
		public Retry {
			requirePositive(initialDelay, "initialDelay");
			requirePositive(maximumDelay, "maximumDelay");
			if (!Double.isFinite(multiplier) || multiplier < 1.0) {
				throw new IllegalArgumentException("multiplier must be finite and at least 1.0");
			}
			if (!Double.isFinite(jitterRatio) || jitterRatio < 0.0 || jitterRatio >= 1.0) {
				throw new IllegalArgumentException("jitterRatio must be between 0.0 inclusive and 1.0 exclusive");
			}
			if (automaticRetryLimit < 0 || automaticRetryLimit > 100) {
				throw new IllegalArgumentException("automaticRetryLimit must be between 0 and 100");
			}
			if (maximumDelay.compareTo(initialDelay) < 0) {
				throw new IllegalArgumentException("maximumDelay must not be less than initialDelay");
			}
		}
	}

	/**
	 * Provisional минимальные резервы, при которых новая растущая operation не
	 * должна начинаться.
	 *
	 * @param stagingMinFreeBytes минимальный свободный резерв staging
	 * @param elasticsearchMinFreeBytes минимальный свободный резерв Elasticsearch
	 */
	public record DiskPressure(
			@DefaultValue("1073741824") @Positive long stagingMinFreeBytes,
			@DefaultValue("2147483648") @Positive long elasticsearchMinFreeBytes
	) {

		/** Проверяет положительные резервы независимо от Spring binding. */
		public DiskPressure {
			if (stagingMinFreeBytes <= 0) {
				throw new IllegalArgumentException("stagingMinFreeBytes must be positive");
			}
			if (elasticsearchMinFreeBytes <= 0) {
				throw new IllegalArgumentException("elasticsearchMinFreeBytes must be positive");
			}
		}
	}

	/**
	 * Bounded сроки ручного rebuild protocol.
	 *
	 * @param planTtl срок действия read-only inspect plan
	 * @param ownershipLease срок lease, продлеваемый между архивами
	 */
	public record Rebuild(
			@DefaultValue("15m") @NotNull Duration planTtl,
			@DefaultValue("15m") @NotNull Duration ownershipLease
	) {

		/** Проверяет положительные сроки независимо от Spring binding. */
		public Rebuild {
			requirePositive(planTtl, "planTtl");
			requirePositive(ownershipLease, "ownershipLease");
		}
	}

	/**
	 * Окна, которые только формируют inspect candidates и не разрешают delete.
	 *
	 * @param orphanBuildingAge возраст BUILDING без heartbeat для inspect
	 * @param supersededAge возраст SUPERSEDED после cutover для inspect
	 * @param automaticDeletionEnabled запрещенный automatic delete switch
	 */
	public record Cleanup(
			@DefaultValue("24h") @NotNull Duration orphanBuildingAge,
			@DefaultValue("7d") @NotNull Duration supersededAge,
			@DefaultValue("false") boolean automaticDeletionEnabled
	) {

		/** Проверяет положительные окна и fail-fast запрет automatic deletion. */
		public Cleanup {
			requirePositive(orphanBuildingAge, "orphanBuildingAge");
			requirePositive(supersededAge, "supersededAge");
			if (automaticDeletionEnabled) {
				throw new IllegalArgumentException("automaticDeletionEnabled must remain false");
			}
		}
	}

	private static void requirePositive(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
	}
}
