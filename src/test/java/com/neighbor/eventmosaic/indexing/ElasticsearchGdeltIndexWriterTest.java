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
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.GetIndexRequest;
import co.elastic.clients.elasticsearch.indices.GetIndexResponse;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateRequest;
import co.elastic.clients.elasticsearch.indices.PutIndexTemplateResponse;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.json.jackson.Jackson3JsonpMapper;
import co.elastic.clients.util.BinaryData;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProperties;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Граница official Elasticsearch client")
class ElasticsearchGdeltIndexWriterTest {
	private static final ExactIndexTarget TARGET = new ExactIndexTarget(
			"gdelt-events-v1-p20260727-g0001",
			"event-index-uuid");
	private static final String PROCESSING_FINGERPRINT = "a".repeat(64);

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final ElasticsearchIndicesClient indices = mock(ElasticsearchIndicesClient.class);

	@Test
	@DisplayName("Подготовка read model устанавливает templates без fixed indices")
	void installsTemplatesWithoutCreatingFixedIndices() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.putIndexTemplate(any(PutIndexTemplateRequest.class)))
				.thenReturn(PutIndexTemplateResponse.of(response -> response.acknowledged(true)));

		writer().prepareReadModel();

		verify(indices, never()).create(any(CreateIndexRequest.class));
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
		when(indices.get(any(GetIndexRequest.class)))
				.thenReturn(targetState(TARGET.indexUuid(), false));
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
				TARGET,
				"event-archive",
				PROCESSING_FINGERPRINT,
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
		when(indices.get(any(GetIndexRequest.class)))
				.thenReturn(targetState(TARGET.indexUuid(), false));
		when(indices.refresh(any(RefreshRequest.class)))
				.thenReturn(RefreshResponse.of(response -> response
						.shards(shards -> shards
								.total(2)
								.successful(1)
								.failed(1))));
		ElasticsearchGdeltIndexWriter indexWriter = writer();

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> indexWriter.refresh(EVENT, TARGET))
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
		ExactIndexTarget longerTarget = new ExactIndexTarget(
				"gdelt-events-v1-p20260727-g10000",
				"another-event-index-uuid");

		long sourceBytes = BinaryData.of(document, mapper).size();
		long operationBytes = indexWriter.estimateBulkOperationBytes(TARGET, document);
		long longerTargetBytes = indexWriter.estimateBulkOperationBytes(longerTarget, document);

		assertThat(operationBytes).isGreaterThan(sourceBytes);
		assertThat(operationBytes - sourceBytes).isGreaterThanOrEqualTo(64);
		assertThat(longerTargetBytes - operationBytes).isEqualTo(6);
	}

	@Test
	@DisplayName("Defense-in-depth отклоняет oversized command до Elasticsearch call")
	void rejectsOversizedCommandBeforeSending() throws IOException {
		when(client._jsonpMapper()).thenReturn(jsonpMapper());
		ElasticsearchGdeltIndexWriter indexWriter = writer(new IndexingProperties(100, 1));
		BulkIndexCommand<IndexedEventDocument> command = new BulkIndexCommand<>(
				EVENT,
				TARGET,
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
		long operationBytes = writer().estimateBulkOperationBytes(TARGET, document);
		long aggregateLimit = Math.subtractExact(
				Math.multiplyExact(operationBytes, 2),
				1);
		ElasticsearchGdeltIndexWriter indexWriter = writer(
				new IndexingProperties(100, aggregateLimit));
		BulkIndexCommand<IndexedEventDocument> command = new BulkIndexCommand<>(
				EVENT,
				TARGET,
				List.of(document, document));

		assertThat(operationBytes).isLessThan(aggregateLimit);
		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> indexWriter.write(command))
				.satisfies(exception -> assertThat(exception.retryable()).isFalse());
		verify(client, never()).bulk(any(BulkRequest.class));
	}

	@Test
	@DisplayName("Другой UUID exact имени отклоняется как потеря ownership")
	void rejectsReplacedExactTarget() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.get(any(GetIndexRequest.class)))
				.thenReturn(targetState("replacement-index-uuid", false));

		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer().refresh(EVENT, TARGET))
				.satisfies(exception -> {
					assertThat(exception.reason())
							.isEqualTo(IndexTargetUnavailableReason.REPLACED);
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEX_TARGET_REPLACED);
					assertThat(exception.retryable()).isFalse();
					assertThat(exception.getMessage())
							.doesNotContain(TARGET.indexName(), TARGET.indexUuid());
				});
		verify(indices, never()).refresh(any(RefreshRequest.class));
	}

	@Test
	@DisplayName("Write block exact target отклоняется как typed maintenance outcome")
	void rejectsWriteBlockedExactTarget() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.get(any(GetIndexRequest.class)))
				.thenReturn(targetState(TARGET.indexUuid(), true));

		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer().refresh(EVENT, TARGET))
				.satisfies(exception -> {
					assertThat(exception.reason())
							.isEqualTo(IndexTargetUnavailableReason.WRITE_BLOCKED);
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEX_TARGET_WRITE_BLOCKED);
				});
		verify(indices, never()).refresh(any(RefreshRequest.class));
	}

	@Test
	@DisplayName("Missing exact target отклоняется без попытки auto-create")
	void rejectsMissingExactTargetWithoutCreatingIndex() throws IOException {
		when(client.indices()).thenReturn(indices);
		when(indices.get(any(GetIndexRequest.class)))
				.thenReturn(GetIndexResponse.of(response -> response.indices(Map.of())));

		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer().refresh(EVENT, TARGET))
				.satisfies(exception -> assertThat(exception.reason())
						.isEqualTo(IndexTargetUnavailableReason.MISSING));
		verify(indices, never()).create(any(CreateIndexRequest.class));
		verify(indices, never()).refresh(any(RefreshRequest.class));
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

	private static GetIndexResponse targetState(String indexUuid, boolean writeBlocked) {
		return GetIndexResponse.of(response -> response.indices(
				TARGET.indexName(),
				state -> state.settings(settings -> settings.index(index -> index
						.uuid(indexUuid)
						.blocks(blocks -> blocks.write(writeBlocked))))));
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
