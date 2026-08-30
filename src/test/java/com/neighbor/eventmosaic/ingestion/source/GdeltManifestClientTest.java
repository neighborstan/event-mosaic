package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Получение GDELT manifest")
class GdeltManifestClientTest {

	private HttpServer server;
	private HttpClient httpClient;

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
		server.start();
		httpClient = HttpClient.newBuilder()
				.followRedirects(HttpClient.Redirect.NEVER)
				.build();
	}

	@AfterEach
	void stopServer() {
		Thread.interrupted();
		try {
			if (server != null) {
				server.stop(0);
			}
		} finally {
			if (httpClient != null) {
				httpClient.close();
			}
		}
	}

	@Test
	@DisplayName("Установленный interrupt flag отменяет получение до HTTP request")
	void interruptFlagStopsBeforeHttpRequest() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/manifest", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, "unexpected");
		});
		Thread.currentThread().interrupt();
		GdeltManifestClient client = client(1024);

		assertThatExceptionOfType(IngestionInterruptedException.class)
				.isThrownBy(client::fetchLatestManifest)
				.satisfies(exception -> {
					assertThat(exception).hasNoCause();
					assertThat(Thread.currentThread().isInterrupted()).isTrue();
				});
		assertThat(requests).hasValue(0);
	}

	@Test
	@DisplayName("Manifest в пределах лимита успешно загружается")
	void fetchesManifestWithinLimit() {
		String body = "100 md5 http://example.test/archive.zip\n";
		server.createContext("/manifest", exchange -> respond(exchange, 200, body));

		assertThat(client(1024).fetchLatestManifest()).isEqualTo(body);
	}

	@Test
	@DisplayName("Неуспешный ответ и перенаправление отклоняются без перехода")
	void rejectsNonSuccessAndRedirectWithoutFollowingIt() {
		server.createContext("/manifest", exchange -> {
			exchange.getResponseHeaders().add("Location", "/other");
			exchange.sendResponseHeaders(302, -1);
			exchange.close();
		});
		server.createContext("/other", exchange -> respond(exchange, 200, "unexpected"));
		GdeltManifestClient client = client(1024);

		assertThatExceptionOfType(RemoteResponseRejectedException.class)
				.isThrownBy(client::fetchLatestManifest)
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IngestionErrorCode.MANIFEST_HTTP_STATUS_REJECTED);
					assertThat(exception.context().httpStatus()).isEqualTo(302);
				});
	}

	@Test
	@DisplayName("Transient status передает корректный Retry-After в retry policy")
	void transientStatusCarriesRetryAfter() {
		server.createContext("/manifest", exchange -> {
			exchange.getResponseHeaders().add("Retry-After", "120");
			exchange.sendResponseHeaders(429, -1);
			exchange.close();
		});

		assertThatExceptionOfType(RemoteSourceAccessException.class)
				.isThrownBy(() -> client(1024).fetchLatestManifest())
				.satisfies(exception -> {
					assertThat(exception.context().httpStatus()).isEqualTo(429);
					assertThat(exception.retryAfter()).isEqualTo(Duration.ofMinutes(2));
				});
	}

	@Test
	@DisplayName("Исчерпанный cycle budget запрещает новый HTTP request")
	void expiredCycleBudgetPreventsRequest() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/manifest", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, "unexpected");
		});
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		nanoTime.set(Duration.ofSeconds(1).toNanos());

		assertThatExceptionOfType(OperationDeadlineExceededException.class)
				.isThrownBy(() -> client(1024).fetchLatestManifest(budget));
		assertThat(requests).hasValue(0);
	}

	@Test
	@DisplayName("Потерянное владение запрещает новый HTTP-запрос")
	void lostOwnershipPreventsRequest() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/manifest", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, "unexpected");
		});
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(5))
				.withLeaseGuard(OperationLeaseSnapshot::lost, Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> client(1024).fetchLatestManifest(budget));
		assertThat(requests).hasValue(0);
	}

	@Test
	@DisplayName("Поздний HTTP-ответ после потери владения не принимается")
	void rejectsResponseReturnedAfterOwnershipLoss() {
		AtomicBoolean ownershipCurrent = new AtomicBoolean(true);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/manifest", exchange -> {
			requests.incrementAndGet();
			ownershipCurrent.set(false);
			respond(exchange, 200, "unexpected");
		});
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(5))
				.withLeaseGuard(
						() -> ownershipCurrent.get()
								? OperationLeaseSnapshot.current(Duration.ofSeconds(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> client(1024).fetchLatestManifest(budget));
		assertThat(requests).hasValue(1);
	}

	@Test
	@DisplayName("Одновременная deadline и потеря владения позднего ответа классифицируются как ownership loss")
	void ownershipLossWinsWhenDeadlineAlsoExpiresBeforeResponse() {
		AtomicBoolean ownershipCurrent = new AtomicBoolean(true);
		AtomicLong nanoTime = new AtomicLong();
		server.createContext("/manifest", exchange -> {
			ownershipCurrent.set(false);
			nanoTime.set(Duration.ofSeconds(5).toNanos());
			respond(exchange, 200, "unexpected");
		});
		OperationBudget budget = OperationBudget
				.start(Duration.ofSeconds(5), nanoTime::get)
				.withLeaseGuard(
						() -> ownershipCurrent.get()
								? OperationLeaseSnapshot.current(Duration.ofSeconds(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> client(1024).fetchLatestManifest(budget));
	}

	@Test
	@DisplayName("Отклоненный после takeover HTTP-ответ закрывает свое тело")
	@SuppressWarnings("unchecked")
	void rejectedLateResponseClosesBody() throws Exception {
		HttpClient delayedClient = mock(HttpClient.class);
		HttpResponse<InputStream> response = mock(HttpResponse.class);
		AtomicBoolean closed = new AtomicBoolean();
		InputStream body = new ByteArrayInputStream("late".getBytes(StandardCharsets.UTF_8)) {
			@Override
			public void close() throws IOException {
				closed.set(true);
				super.close();
			}
		};
		AtomicBoolean ownershipCurrent = new AtomicBoolean(true);
		when(response.body()).thenReturn(body);
		when(delayedClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
				.thenAnswer(_ -> {
					ownershipCurrent.set(false);
					return response;
				});
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(5))
				.withLeaseGuard(
						() -> ownershipCurrent.get()
								? OperationLeaseSnapshot.current(Duration.ofSeconds(5))
								: OperationLeaseSnapshot.lost(),
						Duration.ZERO);
		GdeltManifestClient client = new GdeltManifestClient(
				delayedClient,
				URI.create("http://127.0.0.1/manifest"),
				Duration.ofSeconds(5),
				1024);

		assertThatExceptionOfType(OperationOwnershipLostException.class)
				.isThrownBy(() -> client.fetchLatestManifest(budget));

		assertThat(closed).isTrue();
	}

	@Test
	@DisplayName("Тело сверх лимита отклоняется даже без Content-Length")
	void rejectsBodyThatExceedsLimitEvenWithoutContentLengthHint() {
		server.createContext("/manifest", exchange -> {
			byte[] bytes = "0123456789".getBytes(StandardCharsets.UTF_8);
			exchange.sendResponseHeaders(200, 0);
			exchange.getResponseBody().write(bytes);
			exchange.close();
		});

		assertFailure(
				client(4),
				IngestionErrorCode.MANIFEST_SIZE_LIMIT_EXCEEDED,
				RemoteResponseRejectedException.class);
	}

	@Test
	@DisplayName("Deadline прерывает чтение незавершенного streaming body")
	void deadlineStopsIncompleteStreamingBody() {
		CountDownLatch releaseBody = new CountDownLatch(1);
		server.createContext("/manifest", exchange -> {
			exchange.sendResponseHeaders(200, 0);
			exchange.getResponseBody().write('x');
			exchange.getResponseBody().flush();
			try {
				releaseBody.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			} finally {
				exchange.close();
			}
		});

		try {
			assertFailure(
					client(1024, Duration.ofMillis(100)),
					IngestionErrorCode.MANIFEST_TIMEOUT,
					RemoteSourceAccessException.class);
		} finally {
			releaseBody.countDown();
		}
	}

	@Test
	@DisplayName("Request timeout до получения headers имеет отдельный код")
	void classifiesRequestTimeoutBeforeResponseHeaders() {
		CountDownLatch releaseResponse = new CountDownLatch(1);
		server.createContext("/manifest", exchange -> {
			try {
				releaseResponse.await(5, TimeUnit.SECONDS);
			} catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			} finally {
				exchange.close();
			}
		});

		try {
			assertFailure(
					client(1024, Duration.ofMillis(100)),
					IngestionErrorCode.MANIFEST_TIMEOUT,
					RemoteSourceAccessException.class);
		} finally {
			releaseResponse.countDown();
		}
	}

	@Test
	@DisplayName("Cycle deadline во время request получает отдельный typed outcome")
	void classifiesCycleDeadlineDuringRequest() {
		CountDownLatch releaseResponse = new CountDownLatch(1);
		server.createContext("/manifest", exchange -> {
			try {
				releaseResponse.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			}
			finally {
				exchange.close();
			}
		});

		try {
			assertThatExceptionOfType(OperationDeadlineExceededException.class)
					.isThrownBy(() -> client(1024, Duration.ofSeconds(5))
							.fetchLatestManifest(OperationBudget.start(Duration.ofMillis(100))));
		}
		finally {
			releaseResponse.countDown();
		}
	}

	@Test
	@DisplayName("Timeout у безопасной границы lease возвращает потерю владения")
	void classifiesLeaseLimitedTimeoutAsOwnershipLoss() {
		CountDownLatch releaseResponse = new CountDownLatch(1);
		server.createContext("/manifest", exchange -> {
			try {
				releaseResponse.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			}
			finally {
				exchange.close();
			}
		});
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(5))
				.withLeaseGuard(
						() -> OperationLeaseSnapshot.current(Duration.ofMillis(100)),
						Duration.ZERO);

		try {
			assertThatExceptionOfType(OperationOwnershipLostException.class)
					.isThrownBy(() -> client(1024, Duration.ofSeconds(5))
							.fetchLatestManifest(budget));
		}
		finally {
			releaseResponse.countDown();
		}
	}

	private GdeltManifestClient client(long maxBytes) {
		return client(maxBytes, Duration.ofSeconds(5));
	}

	private GdeltManifestClient client(long maxBytes, Duration timeout) {
		URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/manifest");
		return new GdeltManifestClient(
				httpClient,
				uri,
				timeout,
				maxBytes
		);
	}

	private static void assertFailure(
			GdeltManifestClient client,
			IngestionErrorCode code,
			Class<? extends ApplicationException> exceptionType
	) {
		assertThatExceptionOfType(exceptionType)
				.isThrownBy(client::fetchLatestManifest)
				.satisfies(exception -> assertThat(exception.errorCode()).isEqualTo(code));
	}

	private static void respond(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
