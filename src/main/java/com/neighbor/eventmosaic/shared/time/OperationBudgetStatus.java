package com.neighbor.eventmosaic.shared.time;

/**
 * Объясняет, можно ли начинать следующий ограниченный шаг общей операции.
 */
public enum OperationBudgetStatus {

	/** До общей deadline и безопасной границы lease еще остается время. */
	AVAILABLE,

	/** Общая monotonic deadline уже исчерпана. */
	DEADLINE_EXCEEDED,

	/** Текущий владелец больше не подтвержден либо lease подошел к safety margin. */
	OWNERSHIP_LOST
}
