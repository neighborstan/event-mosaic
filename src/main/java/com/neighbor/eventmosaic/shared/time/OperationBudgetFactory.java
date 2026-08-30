package com.neighbor.eventmosaic.shared.time;

import java.time.Duration;

/** Создает новый monotonic budget для одной общей операции. */
@FunctionalInterface
public interface OperationBudgetFactory {

	/**
	 * Начинает новый независимый budget.
	 *
	 * @param limit положительная общая длительность
	 * @return запущенный budget
	 */
	OperationBudget start(Duration limit);
}
