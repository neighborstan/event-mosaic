package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.error.ApplicationErrorCode;

/**
 * Стабильный каталог безопасных ошибок indexing boundary.
 */
public enum IndexingErrorCode implements ApplicationErrorCode {

	/**
	 * Elasticsearch временно недоступен или завершил транспортную операцию
	 * неопределенным результатом.
	 */
	INDEXING_UNAVAILABLE(
			"INDEXING_UNAVAILABLE",
			"Elasticsearch временно недоступен"),

	/**
	 * Транспортная операция завершилась при установленном interrupt flag.
	 */
	INDEXING_INTERRUPTED(
			"INDEXING_INTERRUPTED",
			"Операция Elasticsearch была прервана"),

	/**
	 * Elasticsearch окончательно отклонил корректно сформированную операцию.
	 */
	INDEXING_REQUEST_REJECTED(
			"INDEXING_REQUEST_REJECTED",
			"Elasticsearch отклонил операцию индексирования"),

	/**
	 * Ответ Elasticsearch нарушил ожидаемый контракт клиента.
	 */
	INDEXING_RESPONSE_INVALID(
			"INDEXING_RESPONSE_INVALID",
			"Elasticsearch вернул некорректный ответ"),

	/**
	 * Exact physical target больше не существует.
	 */
	INDEX_TARGET_MISSING(
			"INDEX_TARGET_MISSING",
			"Exact Elasticsearch target отсутствует"),

	/**
	 * Exact physical имя теперь принадлежит другому index UUID.
	 */
	INDEX_TARGET_REPLACED(
			"INDEX_TARGET_REPLACED",
			"Exact Elasticsearch target был заменен"),

	/**
	 * Exact physical target закрыт для записи maintenance block.
	 */
	INDEX_TARGET_WRITE_BLOCKED(
			"INDEX_TARGET_WRITE_BLOCKED",
			"Exact Elasticsearch target временно закрыт для записи");

	/** Стабильное значение для durable diagnostics. */
	private final String code;

	/** Безопасное сообщение без внешнего содержимого. */
	private final String safeMessage;

	IndexingErrorCode(String code, String safeMessage) {
		this.code = code;
		this.safeMessage = safeMessage;
	}

	@Override
	public String code() {
		return code;
	}

	@Override
	public String safeMessage() {
		return safeMessage;
	}

}
