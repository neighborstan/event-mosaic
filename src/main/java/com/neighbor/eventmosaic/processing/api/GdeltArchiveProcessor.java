package com.neighbor.eventmosaic.processing.api;

/**
 * Потоково преобразует и индексирует один подготовленный GDELT CSV.
 */
public interface GdeltArchiveProcessor {

	/**
	 * Обрабатывает archive с естественным backpressure и bounded checkpoints.
	 *
	 * @param request source context и CSV
	 * @param progressListener владелец durable checkpoint
	 * @param diagnosticListener владелец безопасного attempt logging
	 * @return terminal attempt result
	 */
	ArchiveProcessingResult process(
			ArchiveProcessingRequest request,
			ArchiveProcessingProgressListener progressListener,
			ArchiveProcessingDiagnosticListener diagnosticListener);

	/**
	 * Обрабатывает archive без сохранения attempt-local diagnostics.
	 *
	 * @param request source context и CSV
	 * @param progressListener владелец durable checkpoint
	 * @return terminal attempt result
	 */
	default ArchiveProcessingResult process(
			ArchiveProcessingRequest request,
			ArchiveProcessingProgressListener progressListener
	) {
		return process(
				request,
				progressListener,
				ArchiveProcessingDiagnosticListener.ignoring());
	}
}
