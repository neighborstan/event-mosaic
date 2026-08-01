package com.neighbor.eventmosaic.ingestion.error;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import java.time.Duration;

/**
 * Предоставляет безопасные module-specific детали для проекции ожидаемого
 * ingestion failure в ledger и structured logs.
 *
 * <p>Готовая projection создается concrete смысловым exception одной
 * retry-ветки. Точный code и при необходимости безопасный context передаются
 * непосредственно в месте создания.</p>
 */
public interface IngestionFailureContract {

	/**
	 * Возвращает готовую durable projection без runtime-классификации типа.
	 *
	 * @return code и retryable текущего failure
	 */
	IngestionFailure failure();

	/**
	 * Возвращает разрешенный безопасный диагностический контекст.
	 *
	 * @return типизированный context без unsafe data
	 */
	default IngestionErrorContext context() {
		return IngestionErrorContext.empty();
	}

	/**
	 * Возвращает корректную серверную нижнюю границу следующего retry.
	 * Окончательное значение всегда ограничивает application retry policy.
	 *
	 * @return Retry-After либо zero при отсутствии подсказки
	 */
	default Duration retryAfter() {
		return Duration.ZERO;
	}

	/**
	 * Определяет, должен ли записанный failure остановить весь ingestion cycle.
	 *
	 * @return {@code true} только для управляющих отказов вроде interruption
	 */
	default boolean abortsCycle() {
		return false;
	}
}
