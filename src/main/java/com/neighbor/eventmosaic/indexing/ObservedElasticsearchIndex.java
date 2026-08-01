package com.neighbor.eventmosaic.indexing;

import java.util.Objects;

/** Наблюдаемая Elasticsearch identity и write state exact index. */
record ObservedElasticsearchIndex(String indexName, String indexUuid, boolean writeBlocked) {

	ObservedElasticsearchIndex {
		Objects.requireNonNull(indexName, "indexName must not be null");
		Objects.requireNonNull(indexUuid, "indexUuid must not be null");
		if (indexName.isBlank() || indexUuid.isBlank()) {
			throw new IllegalArgumentException("Observed index identity must not be blank");
		}
	}
}
