package com.neighbor.eventmosaic.indexing.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.Objects;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.ConstructorBinding;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * Проверенные настройки ограниченной bulk-обработки.
 *
 * <p>Безопасные defaults - {@code 500} документов и {@code 5 MiB}. Верхняя
 * граница байтов оставляет запас относительно стандартного Elasticsearch
 * {@code http.max_content_length=100mb}.</p>
 *
 * @param bulkSize максимальное число документов в одной порции
 * @param maxBulkBytes максимальная оценка NDJSON body одной порции в байтах
 * @param requestTimeout максимальное ожидание одного запроса Elasticsearch
 */
@Validated
@ConfigurationProperties("event-mosaic.indexing")
public record IndexingProperties(
		@DefaultValue("500") @Positive @Max(10_000) int bulkSize,
		@DefaultValue("5242880") @Positive @Max(99_614_720) long maxBulkBytes,
		@DefaultValue("2m") @NotNull Duration requestTimeout
) {

	/**
	 * Значение по умолчанию official Elasticsearch BulkIngester.
	 */
	public static final long DEFAULT_MAX_BULK_BYTES = 5L * 1024 * 1024;

	/**
	 * Защитный верхний предел ниже стандартных 100 MiB Elasticsearch.
	 */
	public static final long MAX_BULK_BYTES = 95L * 1024 * 1024;

	/** Начальный безопасный предел ожидания одного запроса Elasticsearch. */
	public static final Duration DEFAULT_REQUEST_TIMEOUT = Duration.ofMinutes(2);

	/**
	 * Сохраняет прежнюю форму прямого создания настроек в unit tests и
	 * небольших локальных adapters.
	 *
	 * @param bulkSize максимальное число документов в порции
	 * @param maxBulkBytes максимальная оценка размера порции в байтах
	 */
	public IndexingProperties(int bulkSize, long maxBulkBytes) {
		this(bulkSize, maxBulkBytes, DEFAULT_REQUEST_TIMEOUT);
	}

	/**
	 * Проверяет ограничения порции при прямом создании вне Spring binding.
	 */
	@ConstructorBinding
	public IndexingProperties {
		Objects.requireNonNull(requestTimeout, "requestTimeout must not be null");
		if (bulkSize <= 0 || bulkSize > 10_000) {
			throw new IllegalArgumentException("bulkSize must be between 1 and 10000");
		}
		if (maxBulkBytes <= 0 || maxBulkBytes > MAX_BULK_BYTES) {
			throw new IllegalArgumentException(
					"maxBulkBytes must be between 1 and " + MAX_BULK_BYTES);
		}
		if (requestTimeout.isZero() || requestTimeout.isNegative()) {
			throw new IllegalArgumentException("requestTimeout must be positive");
		}
	}

}
