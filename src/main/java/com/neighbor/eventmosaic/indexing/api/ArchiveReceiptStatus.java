package com.neighbor.eventmosaic.indexing.api;

/**
 * Результат проверки числа документов точного исходного архива.
 */
public enum ArchiveReceiptStatus {

	/**
	 * Фактическое число совпадает с ожидаемым.
	 */
	MATCHED,

	/**
	 * Индекс существует, но число документов отличается.
	 */
	MISMATCHED,

	/**
	 * Целевой индекс отсутствует.
	 */
	INDEX_ABSENT

}
