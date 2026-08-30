package com.neighbor.eventmosaic.ingestion.config;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Типизированные настройки acquisition pipeline, сгруппированные по связным
 * HTTP, ZIP и continuity обязанностям.
 *
 * @param baseUri официальный каталог объектов GDELT
 * @param stagingRoot локальный корень atomic staging
 * @param http ограничения и timeout внешнего HTTP
 * @param zip ограничения безопасной распаковки
 * @param continuity lease и first-run policy
 * @param automatic настройки автоматического запуска и ограниченной фоновой работы
 * @param oneShotEnabled явное включение однократного trigger
 */
@Validated
@ConfigurationProperties("event-mosaic.ingestion.gdelt")
public record GdeltIngestionProperties(
		@DefaultValue("https://storage.googleapis.com/data.gdeltproject.org/gdeltv2/") @NotNull URI baseUri,
		@DefaultValue(".local/gdelt") @NotNull Path stagingRoot,
		@DefaultValue @Valid @NotNull Http http,
		@DefaultValue @Valid @NotNull Zip zip,
		@DefaultValue @Valid @NotNull Continuity continuity,
		@DefaultValue @Valid @NotNull Automatic automatic,
		@DefaultValue("false") boolean oneShotEnabled
) {

	/** Проверяет source allowlist и наличие обязательных nested groups. */
	public GdeltIngestionProperties {
		GdeltSourceContract.requireOfficialDownloadBaseUri(baseUri);
		Objects.requireNonNull(stagingRoot, "stagingRoot must not be null");
		Objects.requireNonNull(http, "http must not be null");
		Objects.requireNonNull(zip, "zip must not be null");
		Objects.requireNonNull(continuity, "continuity must not be null");
		Objects.requireNonNull(automatic, "automatic must not be null");
	}

	/**
	 * HTTP limits и timeout manifest/archive transport.
	 *
	 * @param maxManifestBytes максимальный размер manifest
	 * @param maxArchiveBytes максимальный размер одного ZIP
	 * @param connectTimeout timeout установки соединения
	 * @param requestTimeout timeout одного запроса
	 */
	public record Http(
			@DefaultValue("65536") @Positive long maxManifestBytes,
			@DefaultValue("536870912") @Positive long maxArchiveBytes,
			@DefaultValue("10s") @NotNull Duration connectTimeout,
			@DefaultValue("2m") @NotNull Duration requestTimeout
	) {

		/** Проверяет положительные HTTP limits и timeout независимо от Spring binding. */
		public Http {
			requirePositive(maxManifestBytes, "maxManifestBytes");
			requirePositive(maxArchiveBytes, "maxArchiveBytes");
			requirePositive(connectTimeout, "connectTimeout");
			requirePositive(requestTimeout, "requestTimeout");
		}
	}

	/**
	 * Ограничения потоковой распаковки недоверенного ZIP.
	 *
	 * @param maxEntries максимальное количество entries
	 * @param maxEntryBytes максимальный размер одной entry
	 * @param maxTotalBytes максимальный суммарный распакованный размер
	 */
	public record Zip(
			@DefaultValue("4") @Positive int maxEntries,
			@DefaultValue("1073741824") @Positive long maxEntryBytes,
			@DefaultValue("1073741824") @Positive long maxTotalBytes
	) {

		/** Проверяет положительность и согласованность ZIP limits. */
		public Zip {
			requirePositive(maxEntries, "maxEntries");
			requirePositive(maxEntryBytes, "maxEntryBytes");
			requirePositive(maxTotalBytes, "maxTotalBytes");
			if (maxEntryBytes > maxTotalBytes) {
				throw new IllegalArgumentException("maxEntryBytes must not exceed maxTotalBytes");
			}
		}
	}

	/**
	 * Настройки lease, recovery и continuity первого запуска.
	 *
	 * @param recoveryLease длительность attempt ownership
	 * @param firstRunPolicy политика continuity baseline
	 * @param firstRunStartAt явная UTC граница для policy FIXED
	 */
	public record Continuity(
			@DefaultValue("15m") @NotNull Duration recoveryLease,
			@DefaultValue("LATEST") @NotNull FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt
	) {

		/** Проверяет lease и cross-field first-run contract. */
		public Continuity {
			Objects.requireNonNull(firstRunPolicy, "firstRunPolicy must not be null");
			requirePositive(recoveryLease, "recoveryLease");
			if (firstRunPolicy == FirstRunPolicy.FIXED && firstRunStartAt == null) {
				throw new IllegalArgumentException("firstRunStartAt is required for FIXED firstRunPolicy");
			}
			if (firstRunPolicy != FirstRunPolicy.FIXED && firstRunStartAt != null) {
				throw new IllegalArgumentException(
						"firstRunStartAt is allowed only for FIXED firstRunPolicy");
			}
			if (firstRunStartAt != null && !GdeltSourceContract.isUpdateBoundary(firstRunStartAt)) {
				throw new IllegalArgumentException("firstRunStartAt must align to a 15-minute UTC boundary");
			}
		}
	}

	/**
	 * Настройки одного последовательного автоматического worker и ограниченной
	 * фоновой работы ingestion.
	 *
	 * @param enabled разрешен ли автоматический запуск в обычном web-процессе
	 * @param pollDelay задержка от terminal завершения до следующего запуска
	 * @param cycleLease срок глобального владения одним ingestion cycle
	 * @param shutdownGrace максимальное ожидание cooperative остановки worker
	 * @param schedulerStaleBase базовый порог устаревшего scheduler
	 * @param sourceOutageThreshold порог недоступности источника
	 * @param dueWorkLimit максимум локальной due work за один cycle
	 * @param receiptAudit настройки ограниченной проверки terminal receipts
	 */
	public record Automatic(
			@DefaultValue("false") boolean enabled,
			@DefaultValue("1m") @NotNull Duration pollDelay,
			@DefaultValue("15m") @NotNull Duration cycleLease,
			@DefaultValue("30s") @NotNull Duration shutdownGrace,
			@DefaultValue("5m") @NotNull Duration schedulerStaleBase,
			@DefaultValue("30m") @NotNull Duration sourceOutageThreshold,
			@DefaultValue("256") @Min(1) @Max(1024) int dueWorkLimit,
			@DefaultValue @Valid @NotNull ReceiptAudit receiptAudit
	) {

		/** Проверяет локальные положительные границы без зависимости от Spring. */
		public Automatic {
			requirePositive(pollDelay, "pollDelay");
			requirePositive(cycleLease, "cycleLease");
			requirePositive(shutdownGrace, "shutdownGrace");
			requirePositive(schedulerStaleBase, "schedulerStaleBase");
			requirePositive(sourceOutageThreshold, "sourceOutageThreshold");
			if (dueWorkLimit < 1 || dueWorkLimit > 1024) {
				throw new IllegalArgumentException("dueWorkLimit must be between 1 and 1024");
			}
			Objects.requireNonNull(receiptAudit, "receiptAudit must not be null");
		}

		/**
		 * Вычисляет порог stale так, чтобы долгий штатный cycle не считался
		 * остановившимся scheduler.
		 *
		 * @param operationDeadline общая deadline одного cycle
		 * @return большее из базового порога и двух poll delay плюс deadline
		 */
		public Duration effectiveSchedulerStaleThreshold(Duration operationDeadline) {
			requirePositive(operationDeadline, "operationDeadline");
			Duration cycleAllowance = pollDelay.multipliedBy(2).plus(operationDeadline);
			return schedulerStaleBase.compareTo(cycleAllowance) >= 0
					? schedulerStaleBase
					: cycleAllowance;
		}

	}

	/**
	 * Настройки небольшой периодической проверки уже сохраненных receipts.
	 *
	 * @param interval минимальный интервал между проверками
	 * @param batchSize максимум receipts в одной проверке
	 */
	public record ReceiptAudit(
			@DefaultValue("15m") @NotNull Duration interval,
			@DefaultValue("2") @Positive int batchSize
	) {

		/** Проверяет положительные границы независимо от Spring binding. */
		public ReceiptAudit {
			requirePositive(interval, "interval");
			if (batchSize <= 0) {
				throw new IllegalArgumentException("batchSize must be positive");
			}
		}
	}

	private static void requirePositive(long value, String name) {
		if (value <= 0) {
			throw new IllegalArgumentException(name + " must be positive");
		}
	}

	private static void requirePositive(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
	}
}
