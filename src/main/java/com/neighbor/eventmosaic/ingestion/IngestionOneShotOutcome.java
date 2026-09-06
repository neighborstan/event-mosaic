package com.neighbor.eventmosaic.ingestion;

/** Различимый terminal outcome одного явно запущенного ingestion cycle. */
public enum IngestionOneShotOutcome {

	/** Все доступные операции текущего cycle завершены. */
	COMPLETED,

	/** Новых данных и готовой локальной работы не обнаружено. */
	UNCHANGED,

	/** Одна из частей цикла завершилась ожидаемым отказом, остальные могли сохранить прогресс. */
	EXPECTED_FAILURE,

	/** Poll или archive пока не достиг durable retryNotBefore либо исчерпан. */
	RETRY_DEFERRED,

	/** Source poll потерял более глубокий attempt token до downstream I/O. */
	OWNERSHIP_LOST,

	/** Общий monotonic cycle budget исчерпан. */
	OPERATION_DEADLINE_EXCEEDED,

	/** Новая растущая operation отложена из-за storage pressure. */
	STORAGE_PRESSURE
}
