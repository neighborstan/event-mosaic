package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.UUID;

/**
 * Сохраненное состояние попыток и текущего lease архива.
 *
 * @param count количество начатых попыток
 * @param token token текущего owner или {@code null}
 * @param lastAttemptAt время последнего claim или {@code null}
 * @param leaseExpiresAt окончание lease или {@code null}
 */
public record ArchiveAttemptState(
		int count,
		UUID token,
		Instant lastAttemptAt,
		Instant leaseExpiresAt
) {

	/**
	 * Проверяет, что сохраненный счетчик попыток неотрицателен.
	 */
	public ArchiveAttemptState {
		if (count < 0) {
			throw new IllegalArgumentException("count must not be negative");
		}
	}
}
