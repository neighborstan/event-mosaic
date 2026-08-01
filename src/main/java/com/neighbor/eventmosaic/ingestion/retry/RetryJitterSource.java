package com.neighbor.eventmosaic.ingestion.retry;

/** Поставляет нормализованную случайную величину для bounded retry jitter. */
@FunctionalInterface
public interface RetryJitterSource {

	/**
	 * Возвращает значение от {@code 0.0} включительно до {@code 1.0} исключительно.
	 *
	 * @return нормализованный jitter sample
	 */
	double nextSample();
}
