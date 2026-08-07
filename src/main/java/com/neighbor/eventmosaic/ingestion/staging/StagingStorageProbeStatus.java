package com.neighbor.eventmosaic.ingestion.staging;

/**
 * Описывает доступность staging без раскрытия filesystem paths и технических
 * исключений.
 */
public enum StagingStorageProbeStatus {

	/** Staging доступен для безопасной записи. */
	WRITABLE,

	/** Настроенный staging path нарушает правила безопасного размещения. */
	UNSAFE,

	/** Доступность staging не удалось подтвердить. */
	UNAVAILABLE
}
