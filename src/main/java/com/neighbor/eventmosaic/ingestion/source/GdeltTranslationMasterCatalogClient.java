package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.api.GdeltHttpBodyDeadline;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpStatusPolicy;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationEffectiveTimeout;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import com.neighbor.eventmosaic.shared.time.OperationTimeoutOrigin;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Загружает только хвостовой фрагмент официального полного каталога GDELT, чтобы не передавать весь большой файл. Если
 * фрагмент нужно расширить, клиент проверяет, что сервер все еще отдает ту же версию файла.
 */
@Component
public class GdeltTranslationMasterCatalogClient {

	static final int INITIAL_SUFFIX_BYTES = 128 * 1024;
	static final int MAX_SUFFIX_BYTES = 1024 * 1024;

	private static final int BUFFER_SIZE = 8192;
	private static final int MAX_GENERATION_LENGTH = 128;
	private static final int MAX_ETAG_LENGTH = 256;
	private static final Pattern CONTENT_RANGE = Pattern.compile(
			"^bytes (\\d+)-(\\d+)/(\\d+)$");
	private static final Pattern GENERATION = Pattern.compile("^[0-9]+$");

	private final HttpClient httpClient;
	private final URI catalogUri;
	private final Duration requestTimeout;
	private final HttpRetryAfterParser retryAfterParser;
	private final GdeltTranslationMasterCatalogParser catalogParser;
	private final LongSupplier nanoTime;

	/**
	 * Создает клиент полного каталога на общих сетевых настройках GDELT.
	 *
	 * @param httpClient HTTP-клиент JDK, который не переходит по перенаправлениям
	 * @param properties проверенные настройки официального источника
	 * @param retryAfterParser разборщик безопасной серверной подсказки о времени повтора
	 * @param catalogParser разборщик и проверка хвостового фрагмента каталога
	 */
	@Autowired
	public GdeltTranslationMasterCatalogClient(
			HttpClient httpClient,
			GdeltIngestionProperties properties,
			HttpRetryAfterParser retryAfterParser,
			GdeltTranslationMasterCatalogParser catalogParser
	) {
		this(
				httpClient,
				properties.baseUri().resolve(GdeltSourceContract.TRANSLATION_MASTER_CATALOG),
				properties.http().requestTimeout(),
				retryAfterParser,
				catalogParser,
				System::nanoTime);
	}

	GdeltTranslationMasterCatalogClient(
			HttpClient httpClient,
			URI catalogUri,
			Duration requestTimeout,
			HttpRetryAfterParser retryAfterParser,
			GdeltTranslationMasterCatalogParser catalogParser
	) {
		this(
				httpClient,
				catalogUri,
				requestTimeout,
				retryAfterParser,
				catalogParser,
				System::nanoTime);
	}

	GdeltTranslationMasterCatalogClient(
			HttpClient httpClient,
			URI catalogUri,
			Duration requestTimeout,
			HttpRetryAfterParser retryAfterParser,
			GdeltTranslationMasterCatalogParser catalogParser,
			LongSupplier nanoTime
	) {
		this.httpClient = Objects.requireNonNull(httpClient, "httpClient must not be null");
		this.catalogUri = Objects.requireNonNull(catalogUri, "catalogUri must not be null");
		this.requestTimeout = requirePositive(requestTimeout, "requestTimeout");
		this.retryAfterParser = Objects.requireNonNull(
				retryAfterParser, "retryAfterParser must not be null");
		this.catalogParser = Objects.requireNonNull(catalogParser, "catalogParser must not be null");
		this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime must not be null");
	}

	/**
	 * Загружает минимальный хвостовой фрагмент, в котором можно проверить все нужные обновления. Фрагмент расширяется только
	 * до жесткого предела, а данные из разных версий файла не смешиваются.
	 *
	 * @param targetTimestamps моменты публикации, которые нужны текущему плану
	 * @param capturedLatest последнее обновление, зафиксированное в начале текущего цикла
	 * @param budget общее ограничение времени цикла и срок исключительного права на его выполнение
	 * @return проверенные данные каталога для нужных моментов публикации
	 */
	public GdeltTranslationMasterCatalog fetchCatalog(
			Set<Instant> targetTimestamps,
			DiscoveredUpdate capturedLatest,
			OperationBudget budget
	) {
		Objects.requireNonNull(targetTimestamps, "targetTimestamps must not be null");
		Objects.requireNonNull(capturedLatest, "capturedLatest must not be null");
		Objects.requireNonNull(budget, "budget must not be null");
		int suffixBytes = INITIAL_SUFFIX_BYTES;
		String pinnedGeneration = null;
		String pinnedEtag = null;
		while (true) {
			GdeltMasterTailSnapshot snapshot = fetchSuffix(
					suffixBytes,
					pinnedGeneration,
					pinnedEtag,
					budget);
			GdeltMasterCatalogParseResult parsed = catalogParser.parse(
					snapshot,
					targetTimestamps,
					capturedLatest);
			if (!parsed.requiresExpansion()) {
				return parsed.requireCatalog();
			}
			if (suffixBytes == MAX_SUFFIX_BYTES) {
				throw new RemoteSourceAccessException(
						IngestionErrorCode.MASTER_CATALOG_RANGE_LIMIT_EXCEEDED);
			}
			pinnedGeneration = snapshot.generation();
			pinnedEtag = snapshot.etag();
			suffixBytes = Math.min(Math.multiplyExact(suffixBytes, 2), MAX_SUFFIX_BYTES);
		}
	}

	private GdeltMasterTailSnapshot fetchSuffix(
			int suffixBytes,
			String pinnedGeneration,
			String pinnedEtag,
			OperationBudget budget
	) {
		IngestionInterruption.throwIfRequested();
		OperationEffectiveTimeout effectiveTimeout = effectiveTimeout(budget);
		long deadlineNanos = GdeltHttpBodyDeadline.deadlineAfter(
				effectiveTimeout.timeout(),
				nanoTime);
		HttpRequest.Builder request = HttpRequest.newBuilder(catalogUri)
				.timeout(effectiveTimeout.timeout())
				.header("Range", "bytes=-" + suffixBytes)
				.GET();
		if (pinnedGeneration != null) {
			request.header("x-goog-if-generation-match", pinnedGeneration);
		}
		try {
			HttpResponse<InputStream> response = httpClient.send(
					request.build(),
					HttpResponse.BodyHandlers.ofInputStream());
			try (InputStream body = response.body()) {
				ensureAvailableAfterExternalResult(budget);
				try (GdeltHttpBodyDeadline deadline = GdeltHttpBodyDeadline.start(
						body,
						deadlineNanos,
						nanoTime)) {
					validateStatus(response, budget);
					String generation = requireGeneration(response.headers());
					String etag = requireEtag(response.headers());
					if (pinnedGeneration != null && !pinnedGeneration.equals(generation)) {
						throw snapshotChanged();
					}
					if (pinnedEtag != null && !pinnedEtag.equals(etag)) {
						throw snapshotChanged();
					}
					ContentRange range = requireContentRange(response.headers(), suffixBytes);
					validateContentLength(response.headers(), range.length());
					byte[] rawBody = readWithinDeadline(
							body,
							range.length(),
							deadline,
							budget,
							effectiveTimeout);
					if (deadline.expired()) {
						throwIfGuardLimitedTimeout(budget, effectiveTimeout);
						throw timeout();
					}
					return new GdeltMasterTailSnapshot(
							removeFirstPartialLine(rawBody, range.start()),
							range.start(),
							range.end(),
							range.total(),
							generation,
							etag);
				}
			}
		}
		catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IngestionInterruptedException(exception);
		}
		catch (HttpTimeoutException exception) {
			throwIfGuardLimitedTimeout(budget, effectiveTimeout);
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_TIMEOUT,
					exception);
		}
		catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			ensureAvailable(budget);
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR,
					exception);
		}
	}

	private void validateStatus(HttpResponse<InputStream> response, OperationBudget budget) {
		int status = response.statusCode();
		if (status == 206) {
			return;
		}
		if (status == 412) {
			throw snapshotChanged();
		}
		IngestionErrorContext context = IngestionErrorContext.forHttpStatus(status);
		if (status >= 300 && status <= 399) {
			ensureAvailable(budget);
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION,
					context);
		}
		if (GdeltHttpStatusPolicy.isTransient(status)) {
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR,
					context,
					retryAfterParser.parse(response.headers()));
		}
		ensureAvailable(budget);
		if (status >= 200 && status <= 299) {
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION,
					context);
		}
		throw new RemoteResponseRejectedException(
				IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR,
				context);
	}

	private byte[] readWithinDeadline(
			InputStream body,
			long expectedLength,
			GdeltHttpBodyDeadline deadline,
			OperationBudget budget,
			OperationEffectiveTimeout effectiveTimeout
	) {
		try {
			return readExact(body, expectedLength, budget);
		}
		catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			if (deadline.expired()) {
				throwIfGuardLimitedTimeout(budget, effectiveTimeout);
				throw timeout();
			}
			ensureAvailable(budget);
			throw new RemoteSourceAccessException(
					IngestionErrorCode.MASTER_CATALOG_HTTP_ERROR,
					exception);
		}
	}

	private static byte[] readExact(
			InputStream input,
			long expectedLength,
			OperationBudget budget
	) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream(Math.toIntExact(expectedLength));
		byte[] buffer = new byte[BUFFER_SIZE];
		long total = 0;
		while (true) {
			IngestionInterruption.throwIfRequested();
			ensureLoopAvailable(budget);
			int read = input.read(buffer);
			ensureLoopAvailable(budget);
			if (read == -1) {
				break;
			}
			total = Math.addExact(total, read);
			if (total > expectedLength) {
				throw protocolViolation();
			}
			output.write(buffer, 0, read);
		}
		if (total != expectedLength) {
			throw protocolViolation();
		}
		return output.toByteArray();
	}

	private static ContentRange requireContentRange(HttpHeaders headers, int suffixBytes) {
		String value = requireSingleHeader(headers, "Content-Range");
		Matcher matcher = CONTENT_RANGE.matcher(value);
		if (!matcher.matches()) {
			throw protocolViolation();
		}
		try {
			long start = Long.parseLong(matcher.group(1));
			long end = Long.parseLong(matcher.group(2));
			long total = Long.parseLong(matcher.group(3));
			long expectedStart = Math.max(0, Math.subtractExact(total, suffixBytes));
			long length = Math.addExact(Math.subtractExact(end, start), 1);
			if (total <= 0
					|| start < 0
					|| end < start
					|| end != total - 1
					|| start != expectedStart
					|| length > suffixBytes) {
				throw protocolViolation();
			}
			return new ContentRange(start, end, total, length);
		}
		catch (ArithmeticException | NumberFormatException _) {
			throw protocolViolation();
		}
	}

	private static void validateContentLength(HttpHeaders headers, long expectedLength) {
		List<String> values = headers.allValues("Content-Length");
		if (values.size() > 1) {
			throw protocolViolation();
		}
		if (values.isEmpty()) {
			return;
		}
		try {
			if (Long.parseLong(values.getFirst()) != expectedLength) {
				throw protocolViolation();
			}
		}
		catch (NumberFormatException _) {
			throw protocolViolation();
		}
	}

	private static String requireGeneration(HttpHeaders headers) {
		String generation = requireSingleHeader(headers, "x-goog-generation");
		if (generation.length() > MAX_GENERATION_LENGTH
				|| !GENERATION.matcher(generation).matches()) {
			throw protocolViolation();
		}
		return generation;
	}

	private static String requireEtag(HttpHeaders headers) {
		String etag = requireSingleHeader(headers, "ETag");
		if (etag.isBlank() || etag.length() > MAX_ETAG_LENGTH) {
			throw protocolViolation();
		}
		return etag;
	}

	private static String requireSingleHeader(HttpHeaders headers, String name) {
		List<String> values = headers.allValues(name);
		if (values.size() != 1) {
			throw protocolViolation();
		}
		return values.getFirst();
	}

	private static byte[] removeFirstPartialLine(byte[] body, long rangeStart) {
		if (rangeStart == 0) {
			return body;
		}
		for (int index = 0; index < body.length; index++) {
			if (body[index] == '\n') {
				return Arrays.copyOfRange(body, index + 1, body.length);
			}
		}
		return new byte[0];
	}

	private OperationEffectiveTimeout effectiveTimeout(OperationBudget budget) {
		try {
			return budget.effectiveTimeout(requestTimeout);
		}
		catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void ensureAvailable(OperationBudget budget) {
		try {
			budget.requireAvailable();
		}
		catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void ensureAvailableAfterExternalResult(OperationBudget budget) {
		try {
			budget.requireAvailableAfterExternalResult();
		}
		catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void ensureLoopAvailable(OperationBudget budget) {
		try {
			budget.requireLoopAvailable();
		}
		catch (OperationDeadlineReachedException _) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void throwIfGuardLimitedTimeout(
			OperationBudget budget,
			OperationEffectiveTimeout effectiveTimeout
	) {
		OperationTimeoutOrigin origin = budget.resolveTimeoutOrigin(effectiveTimeout);
		if (origin == OperationTimeoutOrigin.OPERATION_DEADLINE) {
			throw new OperationDeadlineExceededException();
		}
		if (origin == OperationTimeoutOrigin.LEASE_SAFETY) {
			throw new OperationOwnershipLostException();
		}
	}

	private static RemoteSourceAccessException timeout() {
		return new RemoteSourceAccessException(IngestionErrorCode.MASTER_CATALOG_TIMEOUT);
	}

	private static RemoteSourceAccessException protocolViolation() {
		return new RemoteSourceAccessException(
				IngestionErrorCode.MASTER_CATALOG_PROTOCOL_VIOLATION);
	}

	private static RemoteSourceAccessException snapshotChanged() {
		return new RemoteSourceAccessException(
				IngestionErrorCode.MASTER_CATALOG_SNAPSHOT_CHANGED);
	}

	private static Duration requirePositive(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}

	private record ContentRange(long start, long end, long total, long length) {
	}
}
