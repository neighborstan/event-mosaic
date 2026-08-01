package com.neighbor.eventmosaic.indexing.api;

import java.time.Instant;

/** Разрешает или recoverably создает exact ACTIVE generation для archive time. */
public interface IndexTargetResolver {

	/** Возвращает READY только после наблюдаемой полной пары stable aliases и SQL ACTIVE. */
	IndexTargetResolution resolve(Instant sourceUpdateTime);
}
