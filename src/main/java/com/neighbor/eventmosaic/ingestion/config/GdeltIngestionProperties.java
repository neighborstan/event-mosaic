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
 * Хранит все настройки, от которых зависят загрузка и безопасная обработка
 * данных GDELT. Настройки разделены на сетевые ограничения, распаковку архивов,
 * восстановление после сбоев и автоматический запуск.
 *
 * @param baseUri официальный каталог файлов GDELT
 * @param stagingRoot локальный рабочий каталог для скачанных и распакованных файлов
 * @param http сетевые ограничения и максимальное время HTTP-запросов
 * @param zip ограничения безопасной распаковки ZIP
 * @param continuity правила первого запуска и повтора незавершенной работы
 * @param automatic настройки последовательного фонового запуска
 * @param oneShotEnabled разрешен ли явно запрошенный однократный цикл
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

	/** Проверяет адрес источника и наличие всех обязательных групп настроек. */
	public GdeltIngestionProperties {
		GdeltSourceContract.requireOfficialDownloadBaseUri(baseUri);
		Objects.requireNonNull(stagingRoot, "stagingRoot must not be null");
		Objects.requireNonNull(http, "http must not be null");
		Objects.requireNonNull(zip, "zip must not be null");
		Objects.requireNonNull(continuity, "continuity must not be null");
		Objects.requireNonNull(automatic, "automatic must not be null");
	}

	/**
	 * Задает максимальные размеры ответов GDELT и время ожидания HTTP-запросов.
	 *
	 * @param maxManifestBytes максимальный размер файла со списком архивов
	 * @param maxArchiveBytes максимальный размер одного ZIP
	 * @param connectTimeout максимальное время установки соединения
	 * @param requestTimeout максимальное время одного запроса вместе с чтением ответа
	 */
	public record Http(
			@DefaultValue("65536") @Positive long maxManifestBytes,
			@DefaultValue("536870912") @Positive long maxArchiveBytes,
			@DefaultValue("10s") @NotNull Duration connectTimeout,
			@DefaultValue("2m") @NotNull Duration requestTimeout
	) {

		/** Проверяет, что размеры и временные ограничения положительны. */
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
	 * @param maxEntries максимальное количество файлов внутри архива
	 * @param maxEntryBytes максимальный размер одного распакованного файла
	 * @param maxTotalBytes максимальный суммарный распакованный размер
	 */
	public record Zip(
			@DefaultValue("4") @Positive int maxEntries,
			@DefaultValue("1073741824") @Positive long maxEntryBytes,
			@DefaultValue("1073741824") @Positive long maxTotalBytes
	) {

		/** Проверяет, что все ограничения положительны и размер одного файла не превышает общий предел. */
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
	 * Задает правила первого запуска и повтора незавершенной после сбоя работы.
	 *
	 * @param recoveryLease срок, в течение которого начатая попытка считается принадлежащей одному процессу
	 * @param firstRunPolicy способ выбора начальной границы загрузки
	 * @param firstRunStartAt явная граница UTC для режима {@link FirstRunPolicy#FIXED}
	 */
	public record Continuity(
			@DefaultValue("15m") @NotNull Duration recoveryLease,
			@DefaultValue("RECENT_WINDOW") @NotNull FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt
	) {

		/** Проверяет срок владения попыткой и согласованность настроек первого запуска. */
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
	 * Задает режим одного последовательного фонового исполнителя загрузки GDELT.
	 *
	 * @param enabled разрешен ли автоматический запуск в обычном серверном процессе
	 * @param pollDelay задержка между завершением одного цикла и началом следующего
	 * @param cycleLease срок, на который один процесс получает исключительное право выполнять цикл
	 * @param shutdownGrace максимальное время ожидания штатной остановки фонового исполнителя
	 * @param schedulerStaleBase базовый срок, после которого отсутствие работы планировщика считается подозрительным
	 * @param sourceOutageThreshold порог недоступности источника
	 * @param dueWorkLimit максимальное число отложенных локальных задач за один цикл
	 * @param receiptAudit настройки периодической проверки сохраненных подтверждений индексации
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
		 * Вычисляет, через какое время без новой работы планировщик можно считать
		 * остановившимся. Порог учитывает, что штатный цикл может занять все разрешенное время.
		 *
		 * @param operationDeadline общее ограничение времени одного цикла
		 * @return большее из базового порога и суммы двух задержек с ограничением времени цикла
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
	 * Задает периодическую проверку ранее сохраненных подтверждений индексации.
	 *
	 * @param interval минимальный интервал между проверками
	 * @param batchSize максимальное число подтверждений в одной проверке
	 */
	public record ReceiptAudit(
			@DefaultValue("15m") @NotNull Duration interval,
			@DefaultValue("2") @Positive int batchSize
	) {

		/** Проверяет, что интервал и количество проверяемых подтверждений положительны. */
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
