package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Получение ограниченного хвоста GDELT master catalog")
class GdeltTranslationMasterCatalogClientTest {

	private static final Instant OLDER = Instant.parse("2026-08-30T12:00:00Z");
	private static final Instant FRONTIER = Instant.parse("2026-08-30T12:15:00Z");
	private static final Instant RETRY_CLOCK = Instant.parse("2026-08-30T12:00:00Z");
	private static final String BOUNDARY_GKG = "300 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "
			+ "http://data.gdeltproject.org/gdeltv2/"
			+ "20260830114500.translation.gkg.csv.zip\n";
	private static final String OLDER_GKG = "300 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "
			+ "http://data.gdeltproject.org/gdeltv2/"
			+ "20260830120000.translation.gkg.csv.zip\n";
	private static final String FRONTIER_PAIR = "100 0123456789abcdef0123456789abcdef "
			+ "http://data.gdeltproject.org/gdeltv2/"
			+ "20260830121500.translation.export.CSV.zip\n"
			+ "200 fedcba9876543210fedcba9876543210 "
			+ "http://data.gdeltproject.org/gdeltv2/"
			+ "20260830121500.translation.mentions.CSV.zip\n";

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
		try {
			if (server != null) {
				server.stop(0);
			}
		}
		finally {
			if (httpClient != null) {
				httpClient.close();
			}
		}
	}

	@Test
	@DisplayName("Маленький object принимается только как точный 206 range с обязательной версией")
	void fetchesSmallCatalogFromExactPartialResponse() {
		byte[] object = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		List<String> ranges = new ArrayList<>();
		server.createContext("/catalog", exchange -> {
			ranges.add(exchange.getRequestHeaders().getFirst("Range"));
			respondRange(exchange, object, object.length, "101", "\"etag-a\"");
		});

		GdeltTranslationMasterCatalog catalog = client(Duration.ofSeconds(5))
				.fetchCatalog(Set.of(FRONTIER), latest(), budget());

		assertThat(catalog.status())
				.isEqualTo(GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE);
		assertThat(catalog.updates()).singleElement()
				.satisfies(update -> assertThat(update.sourceUpdateTime()).isEqualTo(FRONTIER));
		assertThat(catalog.generation()).isEqualTo("101");
		assertThat(catalog.etag()).isEqualTo("\"etag-a\"");
		assertThat(ranges).containsExactly("bytes=-131072");
	}

	@Test
	@DisplayName("Расширение заменяет первый хвост, отбрасывает его fragment и закрепляет generation")
	void expandsWithPinnedGenerationAndReplacementBuffer() {
		byte[] object = expandingObject(384 * 1024);
		List<String> ranges = new ArrayList<>();
		List<String> preconditions = new ArrayList<>();
		server.createContext("/catalog", exchange -> {
			String range = exchange.getRequestHeaders().getFirst("Range");
			ranges.add(range);
			preconditions.add(exchange.getRequestHeaders()
					.getFirst("x-goog-if-generation-match"));
			int suffix = requestedSuffix(range);
			byte[] body = Arrays.copyOfRange(object, object.length - suffix, object.length);
			respondRange(exchange, body, object.length, "101", "\"etag-a\"");
		});

		GdeltTranslationMasterCatalog catalog = client(Duration.ofSeconds(5))
				.fetchCatalog(Set.of(FRONTIER), latest(), budget());

		assertThat(catalog.status())
				.isEqualTo(GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE);
		assertThat(ranges).containsExactly("bytes=-131072", "bytes=-262144");
		assertThat(preconditions).containsExactly(null, "101");
	}

	@Test
	@DisplayName("Расширенный range отбрасывает похожий на архив fragment без доказанного начала строки")
	void discardsDangerousPartialFirstLineDuringExpansion() {
		byte[] object = objectWithDangerousPartialLine(384 * 1024);
		List<String> ranges = new ArrayList<>();
		server.createContext("/catalog", exchange -> {
			String range = exchange.getRequestHeaders().getFirst("Range");
			ranges.add(range);
			int suffix = requestedSuffix(range);
			byte[] body = Arrays.copyOfRange(object, object.length - suffix, object.length);
			respondRange(exchange, body, object.length, "101", "\"etag-a\"");
		});

		GdeltTranslationMasterCatalog catalog = client(Duration.ofSeconds(5))
				.fetchCatalog(Set.of(OLDER, FRONTIER), latest(), budget());

		assertThat(catalog.status())
				.isEqualTo(GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE);
		assertThat(catalog.updates())
				.extracting(DiscoveredUpdate::sourceUpdateTime)
				.containsExactly(FRONTIER);
		assertThat(catalog.genuineMissingTargets()).containsExactly(OLDER);
		assertThat(ranges).containsExactly("bytes=-131072", "bytes=-262144");
	}

	@Test
	@DisplayName("Ответ 200 означает проигнорированный Range и отклоняется без full-body fallback")
	void rejectsIgnoredRangeWithoutFallback() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			requests.incrementAndGet();
			respond(exchange, 200, "ignored range");
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION,
				exception -> assertThat(exception.context().httpStatus()).isEqualTo(200));
		assertThat(requests).hasValue(1);
	}

	@Test
	@DisplayName("Постоянные ответы 403 и 404 отклоняются без автоматического повтора")
	void rejectsPermanentHttpStatuses() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			int status = requests.getAndIncrement() == 0 ? 403 : 404;
			exchange.sendResponseHeaders(status, -1);
			exchange.close();
		});

		for (int status : List.of(403, 404)) {
			assertThatExceptionOfType(RemoteResponseRejectedException.class)
					.isThrownBy(() -> client(Duration.ofSeconds(5)).fetchCatalog(
							Set.of(FRONTIER), latest(), budget()))
					.satisfies(exception -> {
						assertThat(exception.errorCode())
								.isEqualTo(IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR);
						assertThat(exception.context().httpStatus()).isEqualTo(status);
						assertThat(exception.failure().retryable()).isFalse();
					});
		}
	}

	@Test
	@DisplayName("Redirect не выполняется и остается повторяемым нарушением master protocol")
	void rejectsRedirectWithoutFollowingIt() {
		AtomicInteger redirectedRequests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			exchange.getResponseHeaders().add("Location", "/redirected");
			exchange.sendResponseHeaders(302, -1);
			exchange.close();
		});
		server.createContext("/redirected", exchange -> {
			redirectedRequests.incrementAndGet();
			exchange.sendResponseHeaders(500, -1);
			exchange.close();
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION,
				exception -> {
					assertThat(exception.context().httpStatus()).isEqualTo(302);
					assertThat(exception.failure().retryable()).isTrue();
				});
		assertThat(redirectedRequests).hasValue(0);
	}

	@Test
	@DisplayName("Временные ответы 429 и 503 сохраняют тип и точную подсказку Retry-After")
	void keepsTransientStatusesRetryableWithDeterministicRetryAfter() {
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			int status = requests.getAndIncrement() == 0 ? 429 : 503;
			exchange.getResponseHeaders().add(
					"Retry-After",
					status == 429 ? "Sun, 30 Aug 2026 12:02:00 GMT" : "45");
			exchange.sendResponseHeaders(status, -1);
			exchange.close();
		});

		for (int status : List.of(429, 503)) {
			Duration expectedRetryAfter = status == 429
					? Duration.ofMinutes(2)
					: Duration.ofSeconds(45);
			assertThatExceptionOfType(RemoteSourceAccessException.class)
					.isThrownBy(() -> client(Duration.ofSeconds(5)).fetchCatalog(
							Set.of(FRONTIER), latest(), budget()))
					.satisfies(exception -> {
						assertThat(exception.errorCode())
								.isEqualTo(IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR);
						assertThat(exception.context().httpStatus()).isEqualTo(status);
						assertThat(exception.retryAfter()).isEqualTo(expectedRetryAfter);
						assertThat(exception.failure().retryable()).isTrue();
					});
		}
	}

	@Test
	@DisplayName("Отсутствующие generation и ETag отклоняются до разбора catalog")
	void rejectsMissingVersionHeaders() {
		byte[] body = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			boolean omitGeneration = requests.getAndIncrement() == 0;
			respondRange(
					exchange,
					body,
					body.length,
					omitGeneration ? List.of() : List.of("101"),
					omitGeneration ? List.of("\"etag-a\"") : List.of());
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
	}

	@Test
	@DisplayName("Повторяющиеся generation и ETag отклоняются до разбора catalog")
	void rejectsDuplicateVersionHeaders() {
		byte[] body = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			boolean duplicateGeneration = requests.getAndIncrement() == 0;
			respondRange(
					exchange,
					body,
					body.length,
					duplicateGeneration ? List.of("101", "101") : List.of("101"),
					duplicateGeneration
							? List.of("\"etag-a\"")
							: List.of("\"etag-a\"", "\"etag-a\""));
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
	}

	@Test
	@DisplayName("Content-Range, который не заканчивается на последнем байте object, отклоняется")
	void rejectsWrongContentRangeEnd() {
		server.createContext("/catalog", exchange -> {
			byte[] body = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
			exchange.getResponseHeaders().add("Content-Range", "bytes 0-1/3");
			exchange.getResponseHeaders().add("x-goog-generation", "101");
			exchange.getResponseHeaders().add("ETag", "\"etag-a\"");
			exchange.sendResponseHeaders(206, body.length);
			exchange.getResponseBody().write(body);
			exchange.close();
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
	}

	@Test
	@DisplayName("Фактическое тело короче заявленного диапазона отклоняется до parser")
	void rejectsBodyShorterThanDeclaredRange() {
		server.createContext("/catalog", exchange -> {
			exchange.getResponseHeaders().add("Content-Range", "bytes 0-9/10");
			exchange.getResponseHeaders().add("x-goog-generation", "101");
			exchange.getResponseHeaders().add("ETag", "\"etag-a\"");
			exchange.sendResponseHeaders(206, 0);
			exchange.getResponseBody().write("short".getBytes(StandardCharsets.UTF_8));
			exchange.close();
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
	}

	@Test
	@DisplayName("Нарушенная generation precondition возвращает отдельный consistency outcome")
	void classifiesPreconditionFailure() {
		server.createContext("/catalog", exchange -> {
			exchange.sendResponseHeaders(412, -1);
			exchange.close();
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_SNAPSHOT_CHANGED);
	}

	@Test
	@DisplayName("Смена generation между заменяющими ranges отклоняет смешанный snapshot")
	void rejectsGenerationChangeDuringExpansion() {
		byte[] object = expandingObject(384 * 1024);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			int request = requests.incrementAndGet();
			int suffix = requestedSuffix(exchange.getRequestHeaders().getFirst("Range"));
			byte[] body = Arrays.copyOfRange(object, object.length - suffix, object.length);
			respondRange(
					exchange,
					body,
					object.length,
					request == 1 ? "101" : "102",
					"\"etag-a\"");
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_SNAPSHOT_CHANGED);
		assertThat(requests).hasValue(2);
	}

	@Test
	@DisplayName("Смена дополнительного ETag при той же generation также отклоняет snapshot")
	void rejectsEtagChangeDuringExpansion() {
		byte[] object = expandingObject(384 * 1024);
		AtomicInteger requests = new AtomicInteger();
		server.createContext("/catalog", exchange -> {
			int request = requests.incrementAndGet();
			int suffix = requestedSuffix(exchange.getRequestHeaders().getFirst("Range"));
			byte[] body = Arrays.copyOfRange(object, object.length - suffix, object.length);
			respondRange(
					exchange,
					body,
					object.length,
					"101",
					request == 1 ? "\"etag-a\"" : "\"etag-b\"");
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_SNAPSHOT_CHANGED);
	}

	@Test
	@DisplayName("Хвост, не достигший нижней границы за один мегабайт, завершается typed limit failure")
	void stopsAtOneMebibyteCap() {
		byte[] object = cappedObject(2 * 1024 * 1024);
		List<String> ranges = new ArrayList<>();
		server.createContext("/catalog", exchange -> {
			String range = exchange.getRequestHeaders().getFirst("Range");
			ranges.add(range);
			int suffix = requestedSuffix(range);
			byte[] body = Arrays.copyOfRange(object, object.length - suffix, object.length);
			respondRange(exchange, body, object.length, "101", "\"etag-a\"");
		});

		assertFailure(
				client(Duration.ofSeconds(5)),
				IngestionErrorCode.MASTER_CATALOG_RANGE_LIMIT_EXCEEDED);
		assertThat(ranges).containsExactly(
				"bytes=-131072",
				"bytes=-262144",
				"bytes=-524288",
				"bytes=-1048576");
	}

	@Test
	@DisplayName("Общий body deadline прерывает незавершенный range stream")
	void deadlineStopsIncompleteRangeBody() {
		CountDownLatch releaseBody = new CountDownLatch(1);
		server.createContext("/catalog", exchange -> {
			exchange.getResponseHeaders().add("Content-Range", "bytes 0-9/10");
			exchange.getResponseHeaders().add("x-goog-generation", "101");
			exchange.getResponseHeaders().add("ETag", "\"etag-a\"");
			exchange.sendResponseHeaders(206, 0);
			exchange.getResponseBody().write('x');
			exchange.getResponseBody().flush();
			try {
				releaseBody.await(5, TimeUnit.SECONDS);
			}
			catch (InterruptedException _) {
				Thread.currentThread().interrupt();
			}
			finally {
				exchange.close();
			}
		});

		try {
			assertFailure(
					client(Duration.ofMillis(100)),
					IngestionErrorCode.MASTER_CATALOG_TIMEOUT);
		}
		finally {
			releaseBody.countDown();
		}
	}

	@Test
	@DisplayName("Завершившееся после абсолютной границы тело отклоняется до запуска фонового таймера")
	void rejectsBodyCompletedAfterDeadlineBeforeDelayedExpiration() throws Exception {
		AtomicLong nanoTime = new AtomicLong(1_000);
		byte[] payload = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		InputStream body = new ByteArrayInputStream(payload) {
			@Override
			public synchronized int read(byte[] target, int offset, int length) {
				int read = super.read(target, offset, length);
				if (read == -1) {
					nanoTime.set(Duration.ofMinutes(1).toNanos());
				}
				return read;
			}
		};
		HttpResponse<InputStream> response = partialResponse(payload, body);
		HttpClient transport = mock(HttpClient.class);
		when(transport.send(
				any(HttpRequest.class),
				org.mockito.ArgumentMatchers.<HttpResponse.BodyHandler<InputStream>>any()))
				.thenReturn(response);
		GdeltTranslationMasterCatalogClient client = new GdeltTranslationMasterCatalogClient(
				transport,
				URI.create("http://source.test/catalog"),
				Duration.ofSeconds(30),
				new HttpRetryAfterParser(fixedClock()),
				new GdeltTranslationMasterCatalogParser(new GdeltManifestLineParser()),
				nanoTime::get);

		assertThatExceptionOfType(RemoteSourceAccessException.class)
				.isThrownBy(() -> client.fetchCatalog(
						Set.of(FRONTIER),
						latest(),
						OperationBudget.start(Duration.ofMinutes(2))))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IngestionErrorCode.MASTER_CATALOG_TIMEOUT));
	}

	private GdeltTranslationMasterCatalogClient client(Duration timeout) {
		URI uri = URI.create("http://127.0.0.1:"
				+ server.getAddress().getPort()
				+ "/catalog");
		return new GdeltTranslationMasterCatalogClient(
				httpClient,
				uri,
				timeout,
				new HttpRetryAfterParser(fixedClock()),
				new GdeltTranslationMasterCatalogParser(new GdeltManifestLineParser()));
	}

	private static Clock fixedClock() {
		return Clock.fixed(RETRY_CLOCK, ZoneOffset.UTC);
	}

	private static OperationBudget budget() {
		return OperationBudget.start(Duration.ofSeconds(10));
	}

	private static DiscoveredUpdate latest() {
		return new GdeltManifestParser(new GdeltManifestLineParser())
				.parse(FRONTIER_PAIR);
	}

	private static byte[] expandingObject(int totalBytes) {
		byte[] object = new byte[totalBytes];
		Arrays.fill(object, (byte) '\n');
		byte[] older = OLDER_GKG.getBytes(StandardCharsets.UTF_8);
		byte[] frontier = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		int olderOffset = totalBytes - 200 * 1024;
		System.arraycopy(older, 0, object, olderOffset, older.length);
		System.arraycopy(frontier, 0, object, totalBytes - frontier.length, frontier.length);
		return object;
	}

	private static byte[] cappedObject(int totalBytes) {
		byte[] object = new byte[totalBytes];
		Arrays.fill(object, (byte) '\n');
		byte[] frontier = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		System.arraycopy(frontier, 0, object, totalBytes - frontier.length, frontier.length);
		return object;
	}

	private static byte[] objectWithDangerousPartialLine(int totalBytes) {
		byte[] object = new byte[totalBytes];
		Arrays.fill(object, (byte) '\n');
		int expandedRangeStart = totalBytes - 2 * GdeltTranslationMasterCatalogClient.INITIAL_SUFFIX_BYTES;
		object[expandedRangeStart - 1] = 'X';
		byte[] dangerousFragment = OLDER_GKG.replace(
				"translation.gkg.csv.zip",
				"translation.export.CSV.zip").getBytes(StandardCharsets.UTF_8);
		byte[] boundary = BOUNDARY_GKG.getBytes(StandardCharsets.UTF_8);
		byte[] frontier = FRONTIER_PAIR.getBytes(StandardCharsets.UTF_8);
		System.arraycopy(
				dangerousFragment,
				0,
				object,
				expandedRangeStart,
				dangerousFragment.length);
		System.arraycopy(
				boundary,
				0,
				object,
				expandedRangeStart + dangerousFragment.length,
				boundary.length);
		System.arraycopy(frontier, 0, object, totalBytes - frontier.length, frontier.length);
		return object;
	}

	private static int requestedSuffix(String range) {
		return Integer.parseInt(range.substring("bytes=-".length()));
	}

	private static void assertFailure(
			GdeltTranslationMasterCatalogClient client,
			IngestionErrorCode expectedCode
	) {
		assertFailure(client, expectedCode, _ -> {
		});
	}

	private static void assertFailure(
			GdeltTranslationMasterCatalogClient client,
			IngestionErrorCode expectedCode,
			java.util.function.Consumer<RemoteSourceAccessException> extraAssertion
	) {
		assertThatExceptionOfType(RemoteSourceAccessException.class)
				.isThrownBy(() -> client.fetchCatalog(Set.of(FRONTIER), latest(), budget()))
				.satisfies(exception -> {
					assertThat(exception.errorCode()).isEqualTo(expectedCode);
					extraAssertion.accept(exception);
				});
	}

	private static void respondRange(
			HttpExchange exchange,
			byte[] body,
			long totalBytes,
			String generation,
			String etag
	) throws IOException {
		respondRange(
				exchange,
				body,
				totalBytes,
				List.of(generation),
				List.of(etag));
	}

	private static void respondRange(
			HttpExchange exchange,
			byte[] body,
			long totalBytes,
			List<String> generations,
			List<String> etags
	) throws IOException {
		long start = totalBytes - body.length;
		long end = totalBytes - 1;
		exchange.getResponseHeaders().add(
				"Content-Range",
				"bytes " + start + "-" + end + "/" + totalBytes);
		generations.forEach(value -> exchange.getResponseHeaders().add(
				"x-goog-generation",
				value));
		etags.forEach(value -> exchange.getResponseHeaders().add("ETag", value));
		exchange.sendResponseHeaders(206, body.length);
		exchange.getResponseBody().write(body);
		exchange.close();
	}

	@SuppressWarnings("unchecked")
	private static HttpResponse<InputStream> partialResponse(
			byte[] payload,
			InputStream body
	) {
		HttpResponse<InputStream> response = mock(HttpResponse.class);
		HttpHeaders headers = HttpHeaders.of(
				Map.of(
						"Content-Range",
						List.of("bytes 0-" + (payload.length - 1) + "/" + payload.length),
						"Content-Length",
						List.of(Integer.toString(payload.length)),
						"x-goog-generation",
						List.of("101"),
						"ETag",
						List.of("\"etag-a\"")),
				(_, _) -> true);
		when(response.statusCode()).thenReturn(206);
		when(response.headers()).thenReturn(headers);
		when(response.body()).thenReturn(body);
		return response;
	}

	private static void respond(HttpExchange exchange, int status, String body) throws IOException {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		exchange.sendResponseHeaders(status, bytes.length);
		exchange.getResponseBody().write(bytes);
		exchange.close();
	}
}
