package com.neighbor.eventmosaic.ingestion.config;

/**
 * Определяет исходную точку continuity при первом обнаруженном update.
 */
public enum FirstRunPolicy {
	/** Начинает ограниченное восстановление с текущего скользящего окна карты. */
	RECENT_WINDOW,

	/** Начинает наблюдение с первого фактически обнаруженного update. */
	LATEST,

	/** Начинает наблюдение с явно настроенного UTC timestamp. */
	FIXED
}
