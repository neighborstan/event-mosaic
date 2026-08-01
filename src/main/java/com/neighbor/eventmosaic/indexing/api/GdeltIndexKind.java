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

	/**
	 * Проверяет, что exact target относится к этому виду schema v1.
	 *
	 * @param target exact physical target
	 * @return {@code true}, если physical имя имеет ожидаемый kind prefix
	 */
	public boolean accepts(ExactIndexTarget target) {
		return target != null && target.indexName().startsWith(indexName + "-");
	}

}
