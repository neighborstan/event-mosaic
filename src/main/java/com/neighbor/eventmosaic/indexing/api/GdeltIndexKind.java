package com.neighbor.eventmosaic.indexing.api;

/**
 * Ограниченный набор видов документов read model GDELT.
 */
public enum GdeltIndexKind {

	/**
	 * Событие GDELT.
	 */
	EVENT("gdelt-events-v1", "globalEventId"),

	/**
	 * Упоминание события GDELT.
	 */
	MENTION("gdelt-mentions-v1", "rawMentionId");

	/** Фиксированное имя физического индекса. */
	private final String indexName;
	private final String identityField;

	GdeltIndexKind(String indexName, String identityField) {
		this.indexName = indexName;
		this.identityField = identityField;
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
	 * Возвращает ordinary doc-values field стабильной document identity.
	 *
	 * @return поле вторичной сортировки receipt
	 */
	public String identityField() {
		return identityField;
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
