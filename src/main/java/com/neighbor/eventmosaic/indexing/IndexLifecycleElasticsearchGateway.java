package com.neighbor.eventmosaic.indexing;

import java.io.IOException;
import java.util.Optional;

/** Narrow exact-name Elasticsearch boundary lifecycle coordinator. */
interface IndexLifecycleElasticsearchGateway {

	void installTemplates() throws IOException;

	Optional<ObservedElasticsearchIndex> findExactIndex(String indexName) throws IOException;

	void createExactIndex(String indexName) throws IOException;

	IndexAliasMembership readStableAliases() throws IOException;

	void addStableAliases(
			String eventIndexName,
			boolean addEvent,
			String mentionIndexName,
			boolean addMention
	) throws IOException;
}
