package com.neighbor.eventmosaic.ingestion.error;

/**
 * Предоставляет cooperative checkpoints, основанные только на interrupt flag потока.
 */
public final class IngestionInterruption {

	private IngestionInterruption() {
	}

	/**
	 * Завершает ingestion operation, если текущий поток запросил interruption.
	 */
	public static void throwIfRequested() {
		if (Thread.currentThread().isInterrupted()) {
			throw new IngestionInterruptedException();
		}
	}

	/**
	 * Завершает ingestion operation и сохраняет связанную техническую причину,
	 * если текущий поток запросил interruption.
	 *
	 * @param cause техническая причина, полученная на I/O boundary
	 */
	public static void throwIfRequested(Throwable cause) {
		if (Thread.currentThread().isInterrupted()) {
			throw new IngestionInterruptedException(cause);
		}
	}
}
