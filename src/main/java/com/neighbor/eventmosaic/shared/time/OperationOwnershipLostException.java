package com.neighbor.eventmosaic.shared.time;

/**
 * Останавливает общую операцию, когда внешний lease больше не подтверждает
 * текущего владельца. Исключение не означает исчерпанную monotonic deadline.
 */
public final class OperationOwnershipLostException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	/** Создает короткий control-flow сигнал без внешних идентификаторов. */
	public OperationOwnershipLostException() {
		super("Operation ownership was lost", null, true, false);
	}
}
