package com.neighbor.eventmosaic.ingestion.observability;

/** Ограниченное диагностическое состояние stable read aliases. */
public enum AliasConsistencyState {

	/** PostgreSQL ACTIVE generations и фактические aliases совпадают. */
	CONSISTENT,

	/** Расхождение наблюдается во время допустимого lifecycle cutover. */
	MAINTENANCE,

	/** Расхождение наблюдается вне допустимого maintenance перехода. */
	INCIDENT,

	/** Проверку нельзя безопасно завершить. */
	UNAVAILABLE
}
