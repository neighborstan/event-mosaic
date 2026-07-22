package com.neighbor.eventmosaic.shared.error;

import java.util.Objects;

/**
 * Базовое unchecked-исключение для ожидаемых прикладных отказов Event Mosaic.
 *
 * <p>Неожиданные programming errors не следует оборачивать этим типом: они
 * должны сохранять исходный stack trace и завершать owning operation.</p>
 */
public abstract class ApplicationException extends RuntimeException {

	private final ApplicationErrorCode errorCode;

	/**
	 * Создает ошибку с безопасным catalog message.
	 *
	 * @param errorCode стабильный код ошибки
	 */
	protected ApplicationException(ApplicationErrorCode errorCode) {
		this(errorCode, null);
	}

	/**
	 * Создает ошибку с безопасным catalog message и исходной причиной.
	 *
	 * @param errorCode стабильный код ошибки
	 * @param cause исходная техническая причина
	 */
	protected ApplicationException(ApplicationErrorCode errorCode, Throwable cause) {
		super(Objects.requireNonNull(errorCode, "errorCode must not be null").safeMessage(), cause);
		this.errorCode = errorCode;
	}

	/**
	 * Возвращает каталогизированный код ошибки.
	 *
	 * @return код ошибки
	 */
	public final ApplicationErrorCode errorCode() {
		return errorCode;
	}
}
