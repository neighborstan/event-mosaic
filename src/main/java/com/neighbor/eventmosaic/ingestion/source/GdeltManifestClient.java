package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.api.GdeltHttpBodyDeadline;
import com.neighbor.eventmosaic.gdelt.api.GdeltHttpStatusPolicy;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruption;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.error.RemoteResponseRejectedException;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Ограниченно по размеру и без redirect загружает latest Translation manifest GDELT.
 */
@Component
public class GdeltManifestClient {

	private static final int BUFFER_SIZE = 8192;

	private final HttpClient httpClient;
	private final URI manifestUri;
	private final Duration requestTimeout;
	private final long maxManifestBytes;
	private final HttpRetryAfterParser retryAfterParser;

	/**
	 * Создает production client с общим HTTP transport и runtime limits.
	 *
	 * @param httpClient общий HTTP client GDELT
	 * @param properties настройки URI, timeout и предельного размера
	 * @param retryAfterParser parser разрешенной серверной retry-подсказки
	 */
	@Autowired
	public GdeltManifestClient(
			HttpClient httpClient,
			GdeltIngestionProperties properties,
			HttpRetryAfterParser retryAfterParser
	) {
		this(
				httpClient,
				properties.baseUri().resolve(GdeltSourceContract.LATEST_TRANSLATION_MANIFEST),
				properties.http().requestTimeout(),
				properties.http().maxManifestBytes(),
				retryAfterParser
		);
	}

	GdeltManifestClient(
			HttpClient httpClient,
			URI manifestUri,
			Duration requestTimeout,
			long maxManifestBytes
	) {
		this(
				httpClient,
				manifestUri,
				requestTimeout,
				maxManifestBytes,
				new HttpRetryAfterParser(Clock.systemUTC()));
	}

	GdeltManifestClient(
			HttpClient httpClient,
			URI manifestUri,
			Duration requestTimeout,
			long maxManifestBytes,
			HttpRetryAfterParser retryAfterParser
	) {
		this.httpClient = httpClient;
		this.manifestUri = manifestUri;
		this.requestTimeout = requestTimeout;
		this.maxManifestBytes = maxManifestBytes;
		this.retryAfterParser = retryAfterParser;
	}

	/**
	 * Загружает manifest, принимая только HTTP 200 и тело в настроенном лимите.
	 *
	 * @return UTF-8 содержимое manifest
	 */
	public String fetchLatestManifest() {
		return fetchLatestManifest(OperationBudget.start(Duration.ofDays(1)));
	}

	/** Загружает manifest в пределах общего monotonic cycle budget. */
	public String fetchLatestManifest(OperationBudget budget) {
		IngestionInterruption.throwIfRequested();
		Duration effectiveTimeout = requireRemaining(budget);
		boolean cycleLimited = effectiveTimeout.compareTo(requestTimeout) < 0;
		long deadlineNanos = GdeltHttpBodyDeadline.deadlineAfter(effectiveTimeout);
		HttpRequest request = HttpRequest.newBuilder(manifestUri)
				.timeout(effectiveTimeout)
				.GET()
				.build();
		try {
			HttpResponse<InputStream> response = httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
			try (InputStream body = response.body();
					GdeltHttpBodyDeadline deadline = GdeltHttpBodyDeadline.start(body, deadlineNanos)) {
				if (response.statusCode() != 200) {
					int status = response.statusCode();
					if (GdeltHttpStatusPolicy.isTransient(status)) {
						throw new RemoteSourceAccessException(
								IngestionErrorCode.MANIFEST_HTTP_ERROR,
								IngestionErrorContext.forHttpStatus(status),
								retryAfterParser.parse(response.headers()));
					}
					throw new RemoteResponseRejectedException(
							IngestionErrorCode.MANIFEST_HTTP_STATUS_REJECTED,
							IngestionErrorContext.forHttpStatus(status));
				}
				long contentLength = response.headers().firstValueAsLong("Content-Length").orElse(-1);
				if (contentLength > maxManifestBytes) {
					throw new RemoteResponseRejectedException(
							IngestionErrorCode.MANIFEST_SIZE_LIMIT_EXCEEDED);
				}
				byte[] manifest = readWithinDeadline(body, deadline, budget, cycleLimited);
				if (deadline.expired()) {
					throwIfCycleLimited(cycleLimited);
					throwIfExpired(budget);
					throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_TIMEOUT);
				}
				IngestionInterruption.throwIfRequested();
				return new String(manifest, StandardCharsets.UTF_8);
			}
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			throw new IngestionInterruptedException(exception);
		} catch (HttpTimeoutException exception) {
			throwIfCycleLimited(cycleLimited);
			throwIfExpired(budget);
			throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_TIMEOUT, exception);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throwIfExpired(budget);
			throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_HTTP_ERROR, exception);
		}
	}

	private byte[] readWithinDeadline(
			InputStream body,
			GdeltHttpBodyDeadline deadline,
			OperationBudget budget,
			boolean cycleLimited
	) throws IOException {
		try {
			return readBounded(body, budget);
		} catch (IOException exception) {
			IngestionInterruption.throwIfRequested(exception);
			throwIfExpired(budget);
			if (deadline.expired()) {
				throwIfCycleLimited(cycleLimited);
				throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_TIMEOUT);
			}
			throw exception;
		}
	}

	private byte[] readBounded(InputStream inputStream, OperationBudget budget) throws IOException {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		byte[] buffer = new byte[BUFFER_SIZE];
		long total = 0;
		while (true) {
			IngestionInterruption.throwIfRequested();
			throwIfExpired(budget);
			int read = inputStream.read(buffer);
			throwIfExpired(budget);
			if (read == -1) {
				break;
			}
			total += read;
			if (total > maxManifestBytes) {
				throw new RemoteResponseRejectedException(
						IngestionErrorCode.MANIFEST_SIZE_LIMIT_EXCEEDED);
			}
			output.write(buffer, 0, read);
		}
		return output.toByteArray();
	}

	private Duration requireRemaining(OperationBudget budget) {
		Duration timeout = budget.cap(requestTimeout);
		if (timeout.isZero()) {
			throw new OperationDeadlineExceededException();
		}
		return timeout;
	}

	private static void throwIfExpired(OperationBudget budget) {
		if (!budget.hasRemaining()) {
			throw new OperationDeadlineExceededException();
		}
	}

	private static void throwIfCycleLimited(boolean cycleLimited) {
		if (cycleLimited) {
			throw new OperationDeadlineExceededException();
		}
	}

}
