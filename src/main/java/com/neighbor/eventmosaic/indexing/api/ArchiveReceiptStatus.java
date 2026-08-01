package com.neighbor.eventmosaic.indexing.api;

/**
 * Результат проверки count и identity digest точного исходного архива.
 */
public enum ArchiveReceiptStatus {

	/**
	 * Count и digest совпадают.
	 */
	MATCHED,

	/**
	 * Фактических документов меньше ожидаемого.
	 */
	SHORTAGE,

	/**
	 * Фактических документов больше ожидаемого.
	 */
	SURPLUS,

	/**
	 * Count совпадает, но состав identities отличается.
	 */
	IDENTITY_MISMATCH

}
