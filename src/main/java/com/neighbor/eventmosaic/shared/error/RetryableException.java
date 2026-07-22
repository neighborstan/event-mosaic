package com.neighbor.eventmosaic.shared.error;

/**
 * Базовое исключение для ожидаемых временных отказов, допускающих повтор
 * owning operation.
 */
public abstract class RetryableException extends ApplicationException {

	/**
	 * Создает повторяемую ошибку с безопасным catalog message.
	 *
	 * @param errorCode стабильный код ошибки
	 */
	protected RetryableException(ApplicationErrorCode errorCode) {
		super(errorCode);
	}

	/**
	 * Создает повторяемую ошибку с исходной технической причиной.
	 *
	 * @param errorCode стабильный код ошибки
	 * @param cause исходная техническая причина
	 */
	protected RetryableException(ApplicationErrorCode errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}
