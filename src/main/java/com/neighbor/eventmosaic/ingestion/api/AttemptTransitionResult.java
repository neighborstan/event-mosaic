package com.neighbor.eventmosaic.ingestion.api;

/**
 * Результат условного transition, защищенного attempt token.
 */
public enum AttemptTransitionResult {
	APPLIED,
	OWNERSHIP_LOST;

	/**
	 * Преобразует число измененных строк conditional update в результат.
	 *
	 * @param updatedRows число измененных строк
	 * @return типизированный результат
	 */
	public static AttemptTransitionResult fromUpdatedRows(int updatedRows) {
		return switch (updatedRows) {
			case 0 -> OWNERSHIP_LOST;
			case 1 -> APPLIED;
			default -> throw new IllegalStateException(
					"Conditional transition changed an unexpected number of rows: " + updatedRows);
		};
	}
}
