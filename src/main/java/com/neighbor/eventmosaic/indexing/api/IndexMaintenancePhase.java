package com.neighbor.eventmosaic.indexing.api;

/** Durable фаза resumable lifecycle operation. */
public enum IndexMaintenancePhase {
	PLANNED,
	FREEZE_REQUESTED,
	FROZEN,
	BUILDING,
	VERIFIED,
	CUTOVER_REQUESTED,
	CUTOVER_OBSERVED,
	UNFREEZE_REQUESTED,
	CLEANUP_PENDING,
	DELETE_REQUESTED,
	COMPLETED,
	FAILED;

	/**
	 * Проверяет допустимость прямого durable перехода между фазами.
	 *
	 * @param next следующая фаза
	 * @return {@code true}, если переход является частью lifecycle protocol
	 */
	public boolean canAdvanceTo(IndexMaintenancePhase next) {
		return switch (this) {
			case PLANNED -> next == FREEZE_REQUESTED || next == BUILDING;
			case FREEZE_REQUESTED -> next == FROZEN;
			case FROZEN -> next == BUILDING || next == UNFREEZE_REQUESTED;
			case BUILDING -> next == VERIFIED || next == UNFREEZE_REQUESTED;
			case VERIFIED -> next == CUTOVER_REQUESTED || next == UNFREEZE_REQUESTED;
			case CUTOVER_REQUESTED -> next == CUTOVER_OBSERVED;
			case CLEANUP_PENDING -> next == DELETE_REQUESTED;
			case CUTOVER_OBSERVED, UNFREEZE_REQUESTED, DELETE_REQUESTED, COMPLETED, FAILED -> false;
		};
	}
}
