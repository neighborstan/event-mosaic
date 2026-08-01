package com.neighbor.eventmosaic.indexing.api;

/**
 * Описывает, успела ли незавершенная сборка записать документы в свои индексы.
 * Это значение сохраняется в плане очистки как часть evidence и не используется
 * для автоматического решения об удалении.
 */
public enum CleanupBuildWriteOutcome {
	NONE,
	PARTIAL,
	COMPLETED,
	UNKNOWN
}
