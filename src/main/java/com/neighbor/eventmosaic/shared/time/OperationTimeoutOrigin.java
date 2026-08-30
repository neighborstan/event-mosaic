package com.neighbor.eventmosaic.shared.time;

/**
 * Объясняет, почему внешний вызов получил именно такой effective timeout.
 */
public enum OperationTimeoutOrigin {

	/** Сработала настройка timeout самого внешнего вызова. */
	CONFIGURED_TIMEOUT,

	/** Вызов ограничен остатком общей монотонной deadline. */
	OPERATION_DEADLINE,

	/** Вызов ограничен остатком lease до его safety margin. */
	LEASE_SAFETY
}
