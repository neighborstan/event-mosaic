package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.TransportOptions;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Ограниченное ожидание запросов Elasticsearch")
class AsyncElasticsearchRequestExecutorTest {

	@Test
	@DisplayName("Async facade сохраняет transport и настройки исходного Spring client")
	void asyncFacadeSharesTransportAndOptionsWithoutOwningClose() {
		ElasticsearchClient source = mock(ElasticsearchClient.class);
		ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
		TransportOptions options = mock(TransportOptions.class);
		when(source._transport()).thenReturn(transport);
		when(source._transportOptions()).thenReturn(options);

		ElasticsearchAsyncClient asyncClient =
				AsyncElasticsearchRequestExecutor.createAsyncClient(source);
		AsyncElasticsearchRequestExecutor executor =
				new AsyncElasticsearchRequestExecutor(asyncClient, Duration.ofMinutes(2));

		assertThat(asyncClient._transport()).isSameAs(transport);
		assertThat(asyncClient._transportOptions()).isSameAs(options);
		assertThat(executor).isNotInstanceOf(AutoCloseable.class);
	}

	@Test
	@DisplayName("Configured timeout отменяет future и остается повторяемым отказом доступа")
	void configuredTimeoutCancelsFutureAsRetryableAccessFailure() {
		CompletableFuture<Object> pending = new CompletableFuture<>();
		AsyncElasticsearchRequestExecutor executor = executor(Duration.ofMillis(10));

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> executor.execute(client -> pending))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE));

		assertThat(pending).isCancelled();
	}

	@Test
	@DisplayName("Cycle deadline отменяет future и возвращает управляющий deadline outcome")
	void deadlineLimitedTimeoutUsesDeadlineControlFlow() {
		CompletableFuture<Object> pending = new CompletableFuture<>();
		OperationBudget budget = OperationBudget.start(
				Duration.ofMillis(10),
				() -> 0L);

		assertThatExceptionOfType(OperationDeadlineReachedException.class)
				.isThrownBy(() -> executor(Duration.ofMinutes(2))
						.execute(budget, client -> pending));

		assertThat(pending).isCancelled();
	}

	@Test
	@DisplayName("Lease-limited timeout после fresh проверки становится потерей ownership")
	void leaseLimitedTimeoutUsesOwnershipControlFlow() {
		CompletableFuture<Object> pending = new CompletableFuture<>();
		AtomicInteger leaseChecks = new AtomicInteger();
		OperationBudget budget = OperationBudget
				.start(Duration.ofSeconds(1), () -> 0L)
				.withLeaseGuard(
						() -> leaseChecks.incrementAndGet() == 1
								? OperationLeaseSnapshot.current(Duration.ofMillis(10))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> executor(Duration.ofMinutes(2))
						.execute(budget, client -> pending));

		assertThat(pending).isCancelled();
		assertThat(leaseChecks).hasValue(2);
	}

	@Test
	@DisplayName("Interruption отменяет future и сохраняет interrupt flag worker")
	void interruptionCancelsFutureAndRestoresFlag() throws Exception {
		CompletableFuture<Object> pending = new CompletableFuture<>();
		CountDownLatch requestStarted = new CountDownLatch(1);
		AtomicReference<Thread> worker = new AtomicReference<>();
		AsyncElasticsearchRequestExecutor executor = executor(Duration.ofMinutes(2));
		try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
			var outcomeFuture = workers.submit(() -> {
				worker.set(Thread.currentThread());
				Throwable failure = catchThrowable(() -> executor.execute(client -> {
					requestStarted.countDown();
					return pending;
				}));
				return new InterruptedOutcome(
						failure,
						Thread.currentThread().isInterrupted());
			});

			assertThat(requestStarted.await(1, TimeUnit.SECONDS)).isTrue();
			worker.get().interrupt();
			InterruptedOutcome outcome = outcomeFuture.get(1, TimeUnit.SECONDS);

			assertThat(outcome.failure()).isInstanceOf(IndexingInterruptedException.class);
			assertThat(outcome.interrupted()).isTrue();
			assertThat(pending).isCancelled();
		}
	}

	@Test
	@DisplayName("Потеря ownership до и после ответа запрещает новый Elasticsearch call")
	void ownershipChecksStopBeforeRequestAndBeforeNextRequest() {
		AsyncElasticsearchRequestExecutor executor = executor(Duration.ofSeconds(1));
		AtomicInteger calls = new AtomicInteger();
		OperationBudget lostBefore = OperationBudget
				.start(Duration.ofSeconds(10), () -> 0L)
				.withLeaseGuard(OperationLeaseSnapshot::lost, Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> executor.execute(lostBefore, client -> {
					calls.incrementAndGet();
					return CompletableFuture.completedFuture("unexpected");
				}));
		assertThat(calls).hasValue(0);

		AtomicInteger leaseChecks = new AtomicInteger();
		OperationBudget lostAfter = OperationBudget
				.start(Duration.ofSeconds(10), () -> 0L)
				.withLeaseGuard(
						() -> leaseChecks.incrementAndGet() == 1
								? OperationLeaseSnapshot.current(Duration.ofSeconds(10))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> {
					executor.execute(lostAfter, client -> {
						calls.incrementAndGet();
						return CompletableFuture.completedFuture("late response");
					});
					executor.execute(lostAfter, client -> {
						calls.incrementAndGet();
						return CompletableFuture.completedFuture("next response");
					});
				});

		assertThat(calls).hasValue(1);
		assertThat(leaseChecks).hasValue(2);
	}

	private static AsyncElasticsearchRequestExecutor executor(Duration timeout) {
		return new AsyncElasticsearchRequestExecutor(
				mock(ElasticsearchAsyncClient.class),
				timeout);
	}

	private record InterruptedOutcome(Throwable failure, boolean interrupted) {
	}
}
