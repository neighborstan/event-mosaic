package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;

/** Читает доказательство полноты Event ingestion для точного суточного окна. */
public interface IngestionCoverageQuery {

	/**
	 * Проверяет каждый 15-минутный Event-слот внутри полуоткрытого окна.
	 *
	 * @param from включенная граница окна на 15-минутной сетке UTC
	 * @param to исключенная граница ровно через 24 часа
	 * @return полное, частичное или неизвестное состояние coverage
	 * @throws IngestionCoverageUnavailableException если диагностический запрос временно недоступен
	 */
	IngestionCoverageEvidence read(Instant from, Instant to);
}
