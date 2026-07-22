package com.neighbor.eventmosaic.ingestion.api;

/**
 * Производный итог обработки обязательной пары архивов одного update.
 */
public enum IngestionRunStatus {
	DISCOVERED,
	IN_PROGRESS,
	PARTIAL,
	FAILED,
	STAGED
}
