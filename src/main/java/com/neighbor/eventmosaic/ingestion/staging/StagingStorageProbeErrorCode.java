package com.neighbor.eventmosaic.ingestion.staging;

/**
 * Стабильные причины результата проверки staging storage.
 */
public enum StagingStorageProbeErrorCode {

	/** Проверка завершилась успешно. */
	NONE,

	/** Staging root содержит небезопасную filesystem indirection. */
	PATH_REJECTED,

	/** Staging root нельзя безопасно подготовить или открыть. */
	ROOT_ACCESS_FAILED,

	/** Проверочный файл нельзя создать или записать. */
	WRITE_CHECK_FAILED,

	/** Свободное место filesystem нельзя определить. */
	USABLE_SPACE_CHECK_FAILED,

	/** Проверочный файл нельзя удалить. */
	CLEANUP_CHECK_FAILED
}
