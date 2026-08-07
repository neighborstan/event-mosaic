package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;

/**
 * Bounded-наблюдение одного storage ресурса.
 *
 * @param resource проверенный ресурс
 * @param state результат проверки резерва
 * @param availableBytes доступное место или {@code -1} для unavailable
 * @param minimumFreeBytes настроенный минимальный резерв
 */
public record StoragePressureObservation(
		StorageResource resource,
		StoragePressureState state,
		long availableBytes,
		long minimumFreeBytes
) {

	/** Проверяет согласованность результата и положительный резерв. */
	public StoragePressureObservation {
		Objects.requireNonNull(resource, "resource must not be null");
		Objects.requireNonNull(state, "state must not be null");
		if (minimumFreeBytes <= 0) {
			throw new IllegalArgumentException("minimumFreeBytes must be positive");
		}
		if (state == StoragePressureState.UNAVAILABLE && availableBytes != -1) {
			throw new IllegalArgumentException("unavailable observation must use -1 bytes");
		}
		if (state != StoragePressureState.UNAVAILABLE && availableBytes < 0) {
			throw new IllegalArgumentException("available bytes must not be negative");
		}
	}

	/** Возвращает {@code true}, если новую растущую operation начинать нельзя. */
	public boolean blocksGrowth() {
		return state != StoragePressureState.AVAILABLE;
	}
}
