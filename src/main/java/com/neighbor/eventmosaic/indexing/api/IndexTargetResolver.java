package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.time.Instant;
import java.util.Objects;

/** Разрешает или recoverably создает exact ACTIVE generation для archive time. */
public interface IndexTargetResolver {

	/** Возвращает READY только после наблюдаемой полной пары stable aliases и SQL ACTIVE. */
	IndexTargetResolution resolve(Instant sourceUpdateTime);

	/**
	 * Разрешает target, ограничивая каждый Elasticsearch lifecycle request общим
	 * budget и подтвержденным ownership ingestion cycle.
	 *
	 * @param sourceUpdateTime время исходного GDELT update
	 * @param budget общий deadline и ownership guard ingestion cycle
	 * @return подтвержденный target либо различимый outcome
	 */
	default IndexTargetResolution resolve(
			Instant sourceUpdateTime,
			OperationBudget budget
	) {
		Objects.requireNonNull(budget, "budget must not be null");
		return resolve(sourceUpdateTime);
	}
}
