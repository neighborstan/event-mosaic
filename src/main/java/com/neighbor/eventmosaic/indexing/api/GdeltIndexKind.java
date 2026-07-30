package com.neighbor.eventmosaic.indexing.api;

/**
 * Ограниченный набор видов документов read model GDELT.
 */
public enum GdeltIndexKind {

	/**
	 * Событие GDELT.
	 */
	EVENT("gdelt-events-v1"),

	/**
	 * Упоминание события GDELT.
	 */
	MENTION("gdelt-mentions-v1");

	/** Фиксированное имя физического индекса. */
	private final String indexName;

	GdeltIndexKind(String indexName) {
		this.indexName = indexName;
	}

	/**
	 * Возвращает фиксированное имя версионированного индекса.
	 *
	 * @return имя индекса Elasticsearch
	 */
	public String indexName() {
		return indexName;
	}

}
