package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;

/**
 * Наблюдаемый непрерывный диапазон пропущенных GDELT updates.
 *
 * @param id идентификатор записи ledger
 * @param firstMissingUpdateTime первый отсутствующий UTC timestamp
 * @param lastMissingUpdateTime последний отсутствующий UTC timestamp
 * @param detectedAt момент обнаружения
 */
public record IngestionGap(
		long id,
		Instant firstMissingUpdateTime,
		Instant lastMissingUpdateTime,
		Instant detectedAt
) {
}
