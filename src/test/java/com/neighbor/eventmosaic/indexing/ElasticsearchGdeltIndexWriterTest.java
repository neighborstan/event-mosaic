package com.neighbor.eventmosaic.indexing;

import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.json.jackson.Jackson3JsonpMapper;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.BinaryData;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProperties;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Граница official Elasticsearch client")
class ElasticsearchGdeltIndexWriterTest {

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);

	@Test
	@DisplayName("Не подтвержденное создание индекса остаётся retryable unknown outcome")
	void treatsUnacknowledgedIndexCreationAsRetryable() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.putIndexTemplate(any(PutIndexTemplateRequest.class)))
				.thenReturn(PutIndexTemplateResponse.of(response -> response.acknowledged(true)));
		when(indices.create(any(CreateIndexRequest.class)))
				.thenReturn(CreateIndexResponse.of(response -> response
						.index("gdelt-events-v1")
						.acknowledged(false)
						.shardsAcknowledged(false)));

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(writer()::prepareReadModel)
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("Не подтвержденная установка template остаётся retryable unknown outcome")
	void treatsUnacknowledgedTemplateInstallationAsRetryable() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.putIndexTemplate(any(PutIndexTemplateRequest.class)))
				.thenReturn(PutIndexTemplateResponse.of(response -> response.acknowledged(false)));

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(writer()::prepareReadModel)
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("I/O failure при установленном flag становится interruption")
	void classifiesIoFailureWithInterruptFlagAsInterruption() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.putIndexTemplate(any(PutIndexTemplateRequest.class)))
				.thenAnswer(invocation -> {
					Thread.currentThread().interrupt();
					throw new IOException("remote details must stay local");
				});

		try {
			assertThatExceptionOfType(IndexingInterruptedException.class)
					.isThrownBy(writer()::prepareReadModel)
					.satisfies(exception -> {
						assertThat(exception.errorCode())
								.isEqualTo(IndexingErrorCode.INDEXING_INTERRUPTED);
						assertThat(exception.getMessage())
								.doesNotContain("remote details");
					});
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		}
		finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Failed shard не может подтвердить receipt даже при совпавшем count")
	void rejectsPartialReceiptCount() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.exists(any(ExistsRequest.class)))
				.thenReturn(new BooleanResponse(true));
		when(client.count(any(CountRequest.class)))
				.thenReturn(CountResponse.of(response -> response
						.count(0)
						.shards(shards -> shards
								.total(2)
								.successful(1)
								.failed(1))));
		ElasticsearchGdeltIndexWriter indexWriter = writer();
		ArchiveReceiptQuery query = new ArchiveReceiptQuery(
				EVENT,
				"event-archive",
				0);

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> indexWriter.verifyReceipt(query))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("Failed shard при refresh остаётся retryable unknown outcome")
	void rejectsPartialRefresh() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.refresh(any(RefreshRequest.class)))
				.thenReturn(RefreshResponse.of(response -> response
						.shards(shards -> shards
								.total(2)
								.successful(1)
								.failed(1))));
		ElasticsearchGdeltIndexWriter indexWriter = writer();

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> indexWriter.refresh(EVENT))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("Byte estimate включает serialized document и metadata")
	void estimatesSerializedDocumentAndMetadataBytes() {
		Jackson3JsonpMapper mapper = jsonpMapper();
		when(client._jsonpMapper()).thenReturn(mapper);
		IndexedEventDocument document = event();
		ElasticsearchGdeltIndexWriter indexWriter = writer();

		long sourceBytes = BinaryData.of(document, mapper).size();
		long operationBytes = indexWriter.estimateBulkOperationBytes(document);

		assertThat(operationBytes).isGreaterThan(sourceBytes);
		assertThat(operationBytes - sourceBytes).isGreaterThanOrEqualTo(64);
	}

	@Test
	@DisplayName("Defense-in-depth отклоняет oversized command до Elasticsearch call")
	void rejectsOversizedCommandBeforeSending() throws IOException {
		when(client._jsonpMapper()).thenReturn(jsonpMapper());
		ElasticsearchGdeltIndexWriter indexWriter = writer(new IndexingProperties(100, 1));
		BulkIndexCommand<IndexedEventDocument> command = new BulkIndexCommand<>(
				EVENT,
				List.of(event()));

		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> indexWriter.write(command))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_REQUEST_REJECTED);
					assertThat(exception.retryable()).isFalse();
				});
		verify(client, never()).bulk(any(BulkRequest.class));
	}

	@Test
	@DisplayName("Defense-in-depth проверяет суммарный byte size всей порции")
	void rejectsAggregateBulkBytesBeforeSending() throws IOException {
		when(client._jsonpMapper()).thenReturn(jsonpMapper());
		IndexedEventDocument document = event();
		long operationBytes = writer().estimateBulkOperationBytes(document);
		long aggregateLimit = Math.subtractExact(
				Math.multiplyExact(operationBytes, 2),
				1);
		ElasticsearchGdeltIndexWriter indexWriter = writer(
				new IndexingProperties(100, aggregateLimit));
		BulkIndexCommand<IndexedEventDocument> command = new BulkIndexCommand<>(
				EVENT,
				List.of(document, document));

		assertThat(operationBytes).isLessThan(aggregateLimit);
		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> indexWriter.write(command))
				.satisfies(exception -> assertThat(exception.retryable()).isFalse());
		verify(client, never()).bulk(any(BulkRequest.class));
	}

	private ElasticsearchGdeltIndexWriter writer() {
		return writer(new IndexingProperties(
				100,
				IndexingProperties.DEFAULT_MAX_BULK_BYTES));
	}

	private ElasticsearchGdeltIndexWriter writer(IndexingProperties properties) {
		return new ElasticsearchGdeltIndexWriter(
				client,
				new ElasticsearchIndexTemplateInstaller(client),
				properties,
				new IndexingMetrics(new SimpleMeterRegistry()));
	}

	private static Jackson3JsonpMapper jsonpMapper() {
		return new Jackson3JsonpMapper();
	}

	private static IndexedEventDocument event() {
		return new IndexedEventDocument(
				1,
				LocalDate.of(2026, 7, 30),
				Instant.parse("2026-07-30T10:00:00Z"),
				"ACT1",
				"Actor 1",
				"US",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"ACT2",
				"Actor 2",
				"RS",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				1,
				"010",
				"01",
				"0",
				1,
				1.5,
				-2.0,
				3,
				2,
				2,
				null,
				"https://example.test/event",
				Instant.parse("2026-07-30T10:15:00Z"),
				"20260730101500.translation.export.CSV.zip",
				1,
				"a".repeat(64));
	}
}
