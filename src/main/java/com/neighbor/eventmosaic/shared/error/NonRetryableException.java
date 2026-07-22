package com.neighbor.eventmosaic.shared.error;

/**
 * Базовое исключение для ожидаемых отказов, которые нельзя повторять без
 * изменения входных данных, конфигурации или кода.
 */
public abstract class NonRetryableException extends ApplicationException {

	/**
	 * Создает неповторяемую ошибку с безопасным catalog message.
	 *
	 * @param errorCode стабильный код ошибки
	 */
	protected NonRetryableException(ApplicationErrorCode errorCode) {
		super(errorCode);
	}

	/**
	 * Создает неповторяемую ошибку с исходной технической причиной.
	 *
	 * @param errorCode стабильный код ошибки
	 * @param cause исходная техническая причина
	 */
	protected NonRetryableException(ApplicationErrorCode errorCode, Throwable cause) {
		super(errorCode, cause);
	}
}
