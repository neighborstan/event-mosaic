package com.neighbor.eventmosaic.processing.api;

/**
 * Синхронная граница checkpoint и продления processing ownership.
 */
@FunctionalInterface
public interface ArchiveProcessingProgressListener {

	/**
	 * Принимает монотонный progress.
	 *
	 * @param progress текущие absolute counters
	 * @return {@code true} для продолжения или {@code false} при потере ownership
	 */
	boolean onProgress(ArchiveProcessingProgress progress);

	/**
	 * Возвращает listener без durable side effects.
	 *
	 * @return listener, всегда разрешающий продолжение
	 */
	static ArchiveProcessingProgressListener continuing() {
		return progress -> true;
	}
}
