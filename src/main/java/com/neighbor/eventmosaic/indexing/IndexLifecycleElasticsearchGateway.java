package com.neighbor.eventmosaic.indexing;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.util.Optional;

/** Narrow exact-name Elasticsearch boundary lifecycle coordinator. */
interface IndexLifecycleElasticsearchGateway {

	void installTemplates() throws IOException;

	void installTemplates(OperationBudget budget) throws IOException;

	Optional<ObservedElasticsearchIndex> findExactIndex(String indexName) throws IOException;

	Optional<ObservedElasticsearchIndex> findExactIndex(
			String indexName,
			OperationBudget budget
	) throws IOException;

	void createExactIndex(String indexName) throws IOException;

	void createExactIndex(String indexName, OperationBudget budget) throws IOException;

	IndexAliasMembership readStableAliases() throws IOException;

	IndexAliasMembership readStableAliases(OperationBudget budget) throws IOException;

	void addStableAliases(
			String eventIndexName,
			boolean addEvent,
			String mentionIndexName,
			boolean addMention
	) throws IOException;

	void addStableAliases(
			String eventIndexName,
			boolean addEvent,
			String mentionIndexName,
			boolean addMention,
			OperationBudget budget
	) throws IOException;
}
