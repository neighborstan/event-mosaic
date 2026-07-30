package com.neighbor.eventmosaic.indexing.api;

/**
 * Итог подтвержденного Elasticsearch bulk-ответа.
 */
public enum BulkIndexOutcome {

	/**
	 * Все элементы подтверждены.
	 */
	SUCCEEDED,

	/**
	 * Есть только временные отказы элементов.
	 */
	RETRYABLE_PARTIAL_FAILURE,

	/**
	 * Есть хотя бы один постоянный отказ элемента.
	 */
	NON_RETRYABLE_PARTIAL_FAILURE

}
