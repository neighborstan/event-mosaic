package com.neighbor.eventmosaic.indexing.api;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
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
 */
@Validated
@ConfigurationProperties("event-mosaic.indexing")
public record IndexingProperties(
		@DefaultValue("500") @Positive @Max(10_000) int bulkSize,
		@DefaultValue("5242880") @Positive @Max(99_614_720) long maxBulkBytes
) {

	/**
	 * Значение по умолчанию official Elasticsearch BulkIngester.
	 */
	public static final long DEFAULT_MAX_BULK_BYTES = 5L * 1024 * 1024;

	/**
	 * Защитный верхний предел ниже стандартных 100 MiB Elasticsearch.
	 */
	public static final long MAX_BULK_BYTES = 95L * 1024 * 1024;

	/**
	 * Проверяет ограничения порции при прямом создании вне Spring binding.
	 */
	public IndexingProperties {
		if (bulkSize <= 0 || bulkSize > 10_000) {
			throw new IllegalArgumentException("bulkSize must be between 1 and 10000");
		}
		if (maxBulkBytes <= 0 || maxBulkBytes > MAX_BULK_BYTES) {
			throw new IllegalArgumentException(
					"maxBulkBytes must be between 1 and " + MAX_BULK_BYTES);
		}
	}

}
