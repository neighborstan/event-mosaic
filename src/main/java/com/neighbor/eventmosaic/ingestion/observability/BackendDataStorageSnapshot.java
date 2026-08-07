package com.neighbor.eventmosaic.ingestion.observability;

import java.util.Objects;

/** Согласованная пара bounded storage observations для operational health. */
public record BackendDataStorageSnapshot(
		StoragePressureObservation staging,
		StoragePressureObservation elasticsearch
) {

	/** Проверяет наличие обеих обязательных storage observations. */
	public BackendDataStorageSnapshot {
		Objects.requireNonNull(staging, "staging must not be null");
		Objects.requireNonNull(elasticsearch, "elasticsearch must not be null");
		if (staging.resource() != StorageResource.STAGING
				|| elasticsearch.resource() != StorageResource.ELASTICSEARCH) {
			throw new IllegalArgumentException("storage observations use unexpected resources");
		}
	}
}
