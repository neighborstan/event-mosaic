package com.neighbor.eventmosaic.ingestion.api;

/**
 * Задает коды информационных событий загрузки GDELT. Эти события помогают понять особенности данных источника, но сами по себе не означают сбой.
 */
public enum IngestionEventCode {
	MANIFEST_UNSUPPORTED_ARCHIVE,
	MASTER_AHEAD_OF_LATEST
}
