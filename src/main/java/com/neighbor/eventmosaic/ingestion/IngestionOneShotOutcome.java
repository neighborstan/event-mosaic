package com.neighbor.eventmosaic.ingestion;

/** Различимый terminal outcome одного явно запущенного ingestion cycle. */
public enum IngestionOneShotOutcome {

	/** Все доступные операции текущего cycle завершены. */
	COMPLETED,

	/** Poll или archive пока не достиг durable retryNotBefore либо исчерпан. */
	RETRY_DEFERRED,

	/** Общий monotonic cycle budget исчерпан. */
	OPERATION_DEADLINE_EXCEEDED,

	/** Новая растущая operation отложена из-за storage pressure. */
	STORAGE_PRESSURE
}
