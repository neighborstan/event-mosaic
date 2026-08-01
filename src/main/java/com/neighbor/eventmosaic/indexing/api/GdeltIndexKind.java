package com.neighbor.eventmosaic.indexing.api;

/**
 * Ограниченный набор видов документов read model GDELT.
 */
public enum GdeltIndexKind {

	/**
	 * Событие GDELT.
	 */
	EVENT("gdelt-events-v1", "gdelt-events-read", "globalEventId"),

	/**
	 * Упоминание события GDELT.
	 */
	MENTION("gdelt-mentions-v1", "gdelt-mentions-read", "rawMentionId");

	/** Базовое schema-v1 имя для physical generations и legacy fixed index. */
	private final String indexName;
	private final String readAlias;
	private final String identityField;

	GdeltIndexKind(String indexName, String readAlias, String identityField) {
		this.indexName = indexName;
		this.readAlias = readAlias;
		this.identityField = identityField;
	}

	/**
	 * Возвращает базовое schema-v1 имя для проверки physical generation.
	 *
	 * @return префикс physical generation Elasticsearch
	 */
	public String indexName() {
		return indexName;
	}

	/**
	 * Возвращает стабильное имя, через которое поиск читает текущие поколения
	 * всех временных разделов этого вида документов.
	 *
	 * @return имя read alias Elasticsearch
	 */
	public String readAlias() {
		return readAlias;
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
