package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** Выполняет async client lambda напрямую для focused adapter unit tests. */
final class DirectElasticsearchRequestExecutor implements ElasticsearchRequestExecutor {

	private final ElasticsearchAsyncClient client;

	DirectElasticsearchRequestExecutor(ElasticsearchAsyncClient client) {
		this.client = Objects.requireNonNull(client, "client must not be null");
	}

	@Override
	public <T> T execute(
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		try {
			return request.apply(client).join();
		}
		catch (CompletionException exception) {
			Throwable cause = exception.getCause();
			if (cause instanceof IOException ioException) {
				throw ioException;
			}
			if (cause instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			throw exception;
		}
	}

	@Override
	public <T> T execute(
			OperationBudget budget,
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		Objects.requireNonNull(budget, "budget must not be null");
		return execute(request);
	}
}
