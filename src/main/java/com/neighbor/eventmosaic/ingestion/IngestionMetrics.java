package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunStatus;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.Locale;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Публикует ingestion counters только с типизированными тегами из небольших и
 * заранее ограниченных наборов значений.
 */
@Component
public class IngestionMetrics {

	private static final String OUTCOME_TAG = "outcome";

	private final MeterRegistry meterRegistry;

	/**
	 * Создает publisher поверх общего application registry.
	 *
	 * @param meterRegistry реестр Micrometer
	 */
	public IngestionMetrics(MeterRegistry meterRegistry) {
		this.meterRegistry = Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
	}

	/** Отмечает начало одного ingestion cycle. */
	public void runStarted() {
		meterRegistry.counter("event_mosaic.ingestion.runs", OUTCOME_TAG, "started").increment();
	}

	/**
	 * Публикует производный итог ingestion cycle.
	 *
	 * @param status итоговый статус run
	 */
	public void runCompleted(IngestionRunStatus status) {
		meterRegistry.counter("event_mosaic.ingestion.runs", OUTCOME_TAG, tag(status)).increment();
	}

	/**
	 * Публикует результат обработки archive.
	 *
	 * @param archiveType роль архива
	 * @param outcome типизированный результат обработки
	 */
	public void archiveOutcome(ArchiveType archiveType, ArchiveOutcome outcome) {
		meterRegistry.counter(
				"event_mosaic.ingestion.archives",
				"type", tag(archiveType),
				OUTCOME_TAG, tag(outcome)
		).increment();
	}

	/**
	 * Публикует результат получения archive.
	 *
	 * @param archiveType роль архива
	 * @param outcome результат download или reuse
	 */
	public void downloadOutcome(ArchiveType archiveType, DownloadOutcome outcome) {
		meterRegistry.counter(
				"event_mosaic.ingestion.downloads",
				"type", tag(archiveType),
				OUTCOME_TAG, tag(outcome)
		).increment();
	}

	/**
	 * Публикует отказ cleanup без подмены исходной ошибки.
	 */
	public void cleanupFailed() {
		meterRegistry.counter(
				"event_mosaic.ingestion.cleanup",
				OUTCOME_TAG, "failed"
		).increment();
	}

	/**
	 * Увеличивает счетчик впервые обнаруженных continuity gaps.
	 *
	 * @param count число новых gaps; нулевое значение не публикуется
	 */
	public void gapsCreated(int count) {
		if (count > 0) {
			meterRegistry.counter("event_mosaic.ingestion.gaps", OUTCOME_TAG, "detected").increment(count);
		}
	}

	/**
	 * Отмечает повторную попытку обработки архива.
	 *
	 * @param archiveType роль архива
	 * @param recovered {@code true}, если claim восстановил истекший lease
	 */
	public void retry(ArchiveType archiveType, boolean recovered) {
		meterRegistry.counter(
				"event_mosaic.ingestion.retries",
				"type", tag(archiveType),
				"recovered", Boolean.toString(recovered)
		).increment();
	}

	/**
	 * Публикует стабильный error code.
	 *
	 * @param code каталогизированный код ошибки
	 */
	public void error(IngestionErrorCode code) {
		meterRegistry.counter(
				"event_mosaic.ingestion.errors",
				"code", tag(code)
		).increment();
	}

	/**
	 * Публикует informational source diagnostic.
	 *
	 * @param code каталогизированный код события
	 */
	public void event(IngestionEventCode code) {
		meterRegistry.counter(
				"event_mosaic.ingestion.events",
				"code", code.name().toLowerCase(Locale.ROOT)
		).increment();
	}

	private static String tag(Enum<?> value) {
		return value.name().toLowerCase(Locale.ROOT);
	}
}
