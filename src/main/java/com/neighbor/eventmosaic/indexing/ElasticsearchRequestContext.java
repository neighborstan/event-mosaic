package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Выбирает standalone либо guarded-выполнение для цепочки связанных
 * Elasticsearch requests, не скрывая общий {@link OperationBudget} в
 * thread-local состоянии.
 */
final class ElasticsearchRequestContext {

	private static final ElasticsearchRequestContext STANDALONE =
			new ElasticsearchRequestContext(null);

	private final OperationBudget budget;

	private ElasticsearchRequestContext(OperationBudget budget) {
		this.budget = budget;
	}

	/** Возвращает context без cycle owner для standalone maintenance. */
	static ElasticsearchRequestContext standalone() {
		return STANDALONE;
	}

	/** Возвращает context текущего ingestion cycle. */
	static ElasticsearchRequestContext guarded(OperationBudget budget) {
		return new ElasticsearchRequestContext(Objects.requireNonNull(
				budget, "budget must not be null"));
	}

	/** Выполняет один request по правилам этого context. */
	<T> T execute(
			ElasticsearchRequestExecutor executor,
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		Objects.requireNonNull(executor, "executor must not be null");
		return budget == null
				? executor.execute(request)
				: executor.execute(budget, request);
	}
}
