package com.neighbor.eventmosaic.ingestion.api;

/**
 * Безопасный типизированный контекст ожидаемой ingestion-ошибки.
 *
 * <p>Контекст содержит только значения, которые разрешено помещать в
 * structured logs. Исходные URI, ответы удаленного сервера и локальные пути
 * намеренно не входят в этот контракт.</p>
 *
 * @param lineNumber номер строки manifest или {@code null}
 * @param httpStatus HTTP status удаленного источника или {@code null}
 */
public record IngestionErrorContext(Integer lineNumber, Integer httpStatus) {

	private static final IngestionErrorContext EMPTY = new IngestionErrorContext(null, null);

	/**
	 * Проверяет допустимые диапазоны безопасных числовых атрибутов.
	 */
	public IngestionErrorContext {
		if (lineNumber != null && lineNumber < 1) {
			throw new IllegalArgumentException("lineNumber must be positive");
		}
		if (httpStatus != null && (httpStatus < 100 || httpStatus > 599)) {
			throw new IllegalArgumentException("httpStatus must be a valid HTTP status code");
		}
	}

	/**
	 * Возвращает пустой контекст.
	 *
	 * @return общий пустой экземпляр
	 */
	public static IngestionErrorContext empty() {
		return EMPTY;
	}

	/**
	 * Создает контекст строки manifest.
	 *
	 * @param lineNumber номер строки, начиная с единицы
	 * @return безопасный контекст
	 */
	public static IngestionErrorContext atLine(int lineNumber) {
		return new IngestionErrorContext(lineNumber, null);
	}

	/**
	 * Создает контекст HTTP-ответа.
	 *
	 * @param httpStatus HTTP status
	 * @return безопасный контекст
	 */
	public static IngestionErrorContext forHttpStatus(int httpStatus) {
		return new IngestionErrorContext(null, httpStatus);
	}
}
