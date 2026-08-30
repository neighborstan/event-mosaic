package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProperties;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationEffectiveTimeout;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import com.neighbor.eventmosaic.shared.time.OperationTimeoutOrigin;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Ограничивает запросы pinned Elasticsearch client через async cancellation.
 * Async facade разделяет transport с обычным Spring client и отдельно не
 * закрывается, поэтому владельцем соединений остается исходный client.
 */
@Component
final class AsyncElasticsearchRequestExecutor implements ElasticsearchRequestExecutor {

	private final ElasticsearchAsyncClient asyncClient;
	private final Duration requestTimeout;

	@Autowired
	AsyncElasticsearchRequestExecutor(
			ElasticsearchClient client,
			IndexingProperties properties
	) {
		this(
				createAsyncClient(client),
				Objects.requireNonNull(properties, "properties must not be null")
						.requestTimeout());
	}

	static ElasticsearchAsyncClient createAsyncClient(ElasticsearchClient client) {
		ElasticsearchClient source = Objects.requireNonNull(
				client, "client must not be null");
		return new ElasticsearchAsyncClient(
				source._transport(),
				source._transportOptions());
	}

	AsyncElasticsearchRequestExecutor(
			ElasticsearchAsyncClient asyncClient,
			Duration requestTimeout
	) {
		this.asyncClient = Objects.requireNonNull(asyncClient, "asyncClient must not be null");
		this.requestTimeout = requirePositive(requestTimeout);
	}

	@Override
	public <T> T execute(
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		return executeInternal(null, request);
	}

	@Override
	public <T> T execute(
			OperationBudget budget,
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		return executeInternal(
				Objects.requireNonNull(budget, "budget must not be null"),
				request);
	}

	private <T> T executeInternal(
			OperationBudget budget,
			Function<ElasticsearchAsyncClient, CompletableFuture<T>> request
	) throws IOException {
		Objects.requireNonNull(request, "request must not be null");
		checkInterrupted();
		OperationEffectiveTimeout attempted = budget == null
				? new OperationEffectiveTimeout(
						requestTimeout,
						OperationTimeoutOrigin.CONFIGURED_TIMEOUT)
				: budget.effectiveTimeout(requestTimeout);
		CompletableFuture<T> future = Objects.requireNonNull(
				request.apply(asyncClient), "request returned null future");
		try {
			T result = future.get(timeoutNanos(attempted.timeout()), TimeUnit.NANOSECONDS);
			if (budget != null) {
				budget.requireAvailableAfterExternalResult();
			}
			return result;
		}
		catch (TimeoutException exception) {
			future.cancel(true);
			throw timeoutFailure(budget, attempted, exception);
		}
		catch (InterruptedException exception) {
			future.cancel(true);
			Thread.currentThread().interrupt();
			throw new IndexingInterruptedException(exception);
		}
		catch (CancellationException exception) {
			throw new IndexingAccessException(
					IndexingErrorCode.INDEXING_UNAVAILABLE,
					exception);
		}
		catch (ExecutionException exception) {
			throwExecutionFailure(exception.getCause());
			throw new IllegalStateException("unreachable");
		}
	}

	private static RuntimeException timeoutFailure(
			OperationBudget budget,
			OperationEffectiveTimeout attempted,
			TimeoutException cause
	) {
		OperationTimeoutOrigin origin = budget == null
				? OperationTimeoutOrigin.CONFIGURED_TIMEOUT
				: budget.resolveTimeoutOrigin(attempted);
		return switch (origin) {
			case CONFIGURED_TIMEOUT -> new IndexingAccessException(
					IndexingErrorCode.INDEXING_UNAVAILABLE,
					cause);
			case OPERATION_DEADLINE -> new OperationDeadlineReachedException();
			case LEASE_SAFETY -> new OperationOwnershipLostException();
		};
	}

	private static void throwExecutionFailure(Throwable failure) throws IOException {
		Throwable cause = unwrap(failure);
		if (cause instanceof IOException ioException) {
			throw ioException;
		}
		if (cause instanceof RuntimeException runtimeException) {
			throw runtimeException;
		}
		if (cause instanceof Error error) {
			throw error;
		}
		throw new IOException("Elasticsearch async request failed", cause);
	}

	private static Throwable unwrap(Throwable failure) {
		Throwable result = Objects.requireNonNull(failure, "failure must not be null");
		while (result instanceof CompletionException && result.getCause() != null) {
			result = result.getCause();
		}
		return result;
	}

	private static Duration requirePositive(Duration value) {
		Objects.requireNonNull(value, "requestTimeout must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException("requestTimeout must be positive");
		}
		return value;
	}

	private static long timeoutNanos(Duration timeout) {
		try {
			return timeout.toNanos();
		}
		catch (ArithmeticException exception) {
			return Long.MAX_VALUE;
		}
	}

	private static void checkInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throw new IndexingInterruptedException();
		}
	}
}
