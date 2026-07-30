package com.neighbor.eventmosaic.gdelt.api;

/**
 * Синхронно сообщает абсолютное число локально отклоненных source records.
 *
 * <p>Production reader вызывает listener сразу после каждого нового
 * отклонения. Это позволяет owning processing boundary продлевать lease и на
 * invalid-only input, не накапливая records в памяти.</p>
 */
@FunctionalInterface
public interface GdeltCsvProgressListener {

	/**
	 * Принимает монотонный абсолютный counter отклоненных source records.
	 *
	 * @param invalidRecords число отклоненных records, включая текущее
	 */
	void onInvalidRecords(long invalidRecords);

	/**
	 * Возвращает listener без side effects.
	 *
	 * @return listener, намеренно игнорирующий progress
	 */
	static GdeltCsvProgressListener ignoring() {
		return _ -> {
			// Вызов намеренно не имеет side effects.
		};
	}
}
