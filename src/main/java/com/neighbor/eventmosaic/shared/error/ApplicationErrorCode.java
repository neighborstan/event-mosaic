package com.neighbor.eventmosaic.shared.error;

/**
 * Общий контракт стабильного кода прикладной ошибки.
 *
 * <p>Каталог владеет безопасным сообщением. Транспортные adapters могут
 * отображать код и сообщение в HTTP, CLI или сообщения, не анализируя текст
 * исключения.</p>
 */
public interface ApplicationErrorCode {

	/**
	 * Возвращает стабильный машинно-читаемый код.
	 *
	 * @return код, сохраняемый в state и observability events
	 */
	String code();

	/**
	 * Возвращает безопасное сообщение без внешнего недоверенного содержимого.
	 *
	 * @return сообщение каталога
	 */
	String safeMessage();

}
