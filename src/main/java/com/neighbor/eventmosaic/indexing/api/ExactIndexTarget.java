package com.neighbor.eventmosaic.indexing.api;

import java.util.Objects;
import java.util.regex.Pattern;

/** Exact physical Elasticsearch index вместе с наблюдаемой identity. */
public record ExactIndexTarget(String indexName, String indexUuid) {
	private static final Pattern INDEX_NAME = Pattern.compile(
			"^gdelt-(events|mentions)-v1-p[0-9]{8}-g[0-9]{4,}$");
	private static final Pattern INDEX_UUID = Pattern.compile("^[^*?,\\s]{1,128}$");

	/** Проверяет, что target нельзя интерпретировать как alias или wildcard. */
	public ExactIndexTarget {
		Objects.requireNonNull(indexName, "indexName must not be null");
		Objects.requireNonNull(indexUuid, "indexUuid must not be null");
		if (indexName.length() > 255 || !INDEX_NAME.matcher(indexName).matches()) {
			throw new IllegalArgumentException("indexName must be an exact schema v1 physical index");
		}
		if (!INDEX_UUID.matcher(indexUuid).matches()) {
			throw new IllegalArgumentException("indexUuid must be a bounded exact Elasticsearch UUID");
		}
	}
}
