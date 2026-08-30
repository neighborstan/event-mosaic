package com.neighbor.eventmosaic.shared.time;

/**
 * Подтверждает внешнее право продолжать общую операцию. Реализация может
 * обратиться к долговечному хранилищу и обязана завершиться ошибкой, если
 * подтвердить владельца невозможно.
 */
@FunctionalInterface
public interface OperationLeaseGuard {

	/** Возвращает актуальный снимок ownership перед новым побочным эффектом. */
	OperationLeaseSnapshot check();
}
