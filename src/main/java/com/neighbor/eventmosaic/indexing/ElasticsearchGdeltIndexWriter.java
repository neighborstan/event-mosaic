package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.indices.RefreshResponse;
import co.elastic.clients.util.BinaryData;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexedDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProperties;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import jakarta.json.JsonException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.stereotype.Component;

/**
 * Синхронный official-client adapter фиксированной Elasticsearch read model.
 */
@Component
final class ElasticsearchGdeltIndexWriter implements GdeltIndexWriter {

	private static final String SOURCE_ARCHIVE_KEY_FIELD = "sourceArchiveKey";
	private static final String INDEX_NOT_FOUND_ERROR_TYPE = "index_not_found_exception";
	private static final String INDEX_ALREADY_EXISTS_ERROR_TYPE =
			"resource_already_exists_exception";
	private static final long BULK_ACTION_FIXED_BYTES = 64;
	private static final long JSON_ESCAPE_EXPANSION_FACTOR = 6;

	private final ElasticsearchClient client;
	private final ElasticsearchIndexTemplateInstaller templateInstaller;
	private final IndexingProperties properties;
	private final IndexingMetrics metrics;
	private final Object preparationMonitor = new Object();

	private volatile boolean templatesPrepared;

	ElasticsearchGdeltIndexWriter(
			ElasticsearchClient client,
			ElasticsearchIndexTemplateInstaller templateInstaller,
			IndexingProperties properties,
			IndexingMetrics metrics
	) {
		this.client = Objects.requireNonNull(client, "client must not be null");
		this.templateInstaller = Objects.requireNonNull(
				templateInstaller, "templateInstaller must not be null");
		this.properties = Objects.requireNonNull(properties, "properties must not be null");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	@Override
	public int bulkSize() {
		return properties.bulkSize();
	}

	@Override
	public long maxBulkBytes() {
		return properties.maxBulkBytes();
	}

	@Override
	public long estimateBulkOperationBytes(GdeltIndexedDocument document) {
		Objects.requireNonNull(document, "document must not be null");
		BinaryData serializedDocument = serialize(document);
		return estimateBulkOperationBytes(document, serializedDocument);
	}

	private static long estimateBulkOperationBytes(
			GdeltIndexedDocument document,
			BinaryData serializedDocument
	) {
		try {
			long metadataValueBytes = Math.addExact(
					utf8Length(document.kind().indexName()),
					utf8Length(document.documentId()));
			long escapedMetadataUpperBound = Math.multiplyExact(
					metadataValueBytes,
					JSON_ESCAPE_EXPANSION_FACTOR);
			return Math.addExact(
					serializedDocument.size(),
					Math.addExact(BULK_ACTION_FIXED_BYTES, escapedMetadataUpperBound));
		}
		catch (ArithmeticException exception) {
			throw new IndexingProtocolException(
					IndexingErrorCode.INDEXING_REQUEST_REJECTED,
					exception);
		}
	}

	@Override
	public void prepareReadModel() {
		checkInterrupted();
		synchronized (preparationMonitor) {
			templatesPrepared = false;
			try {
				templateInstaller.install();
				createFixedIndices();
				templatesPrepared = true;
			}
			catch (IOException exception) {
				throw ioFailure(exception);
			}
			catch (ElasticsearchException exception) {
				throw classify(exception);
			}
		}
	}

	@Override
	public BulkIndexResult write(
			BulkIndexCommand<? extends GdeltIndexedDocument> command
	) {
		Objects.requireNonNull(command, "command must not be null");
		checkInterrupted();
		if (command.documents().size() > properties.bulkSize()) {
			throw new IllegalArgumentException("bulk command exceeds configured bulkSize");
		}
		List<BinaryData> serializedDocuments = validateAndSerialize(command);
		ensureTemplatesPrepared();

		BulkRequest request = request(command, serializedDocuments);
		try {
			BulkResponse response = client.bulk(request);
			BulkIndexResult result = BulkResponseAnalyzer.analyze(command, response.items());
			metrics.completed(result);
			return result;
		}
		catch (IOException exception) {
			metrics.failed(command.kind(), command.documents().size(), true);
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			metrics.failed(
					command.kind(),
					command.documents().size(),
					isTransient(exception.status()));
			throw classify(exception);
		}
		catch (IndexingAccessException
				| IndexingInterruptedException
				| IndexingProtocolException exception) {
			metrics.failed(command.kind(), command.documents().size(), exception.retryable());
			throw exception;
		}
	}

	@Override
	public void refresh(GdeltIndexKind kind) {
		Objects.requireNonNull(kind, "kind must not be null");
		checkInterrupted();
		try {
			RefreshResponse response = client.indices()
					.refresh(request -> request.index(kind.indexName()));
			rejectFailedShards(response.shards());
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			if (isIndexNotFound(exception)) {
				throw new IndexingAccessException(
						IndexingErrorCode.INDEXING_UNAVAILABLE,
						exception);
			}
			throw classify(exception);
		}
	}

	@Override
	public ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query) {
		Objects.requireNonNull(query, "query must not be null");
		checkInterrupted();
		try {
			boolean exists = client.indices()
					.exists(request -> request.index(query.kind().indexName()))
					.value();
			if (!exists) {
				return absentReceipt(query);
			}
			CountResponse response = client.count(request -> request
					.index(query.kind().indexName())
					.query(term -> term.term(value -> value
							.field(SOURCE_ARCHIVE_KEY_FIELD)
							.value(query.sourceArchiveKey()))));
			rejectFailedShards(response.shards());
			long actualCount = response.count();
			ArchiveReceiptStatus status = actualCount == query.expectedDocumentCount()
					? ArchiveReceiptStatus.MATCHED
					: ArchiveReceiptStatus.MISMATCHED;
			return new ArchiveReceiptVerification(
					query.kind(),
					query.expectedDocumentCount(),
					actualCount,
					status);
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			if (isIndexNotFound(exception)) {
				return absentReceipt(query);
			}
			throw classify(exception);
		}
	}

	private void ensureTemplatesPrepared() {
		if (!templatesPrepared) {
			prepareReadModel();
		}
	}

	private List<BinaryData> validateAndSerialize(
			BulkIndexCommand<? extends GdeltIndexedDocument> command
	) {
		List<BinaryData> serializedDocuments = new ArrayList<>(command.documents().size());
		long estimatedBytes = 0;
		for (GdeltIndexedDocument document : command.documents()) {
			BinaryData serializedDocument = serialize(document);
			long operationBytes = estimateBulkOperationBytes(document, serializedDocument);
			if (operationBytes > properties.maxBulkBytes() - estimatedBytes) {
				throw new IndexingProtocolException(
						IndexingErrorCode.INDEXING_REQUEST_REJECTED);
			}
			estimatedBytes = Math.addExact(estimatedBytes, operationBytes);
			serializedDocuments.add(serializedDocument);
		}
		return serializedDocuments;
	}

	private void createFixedIndices() throws IOException {
		for (GdeltIndexKind kind : GdeltIndexKind.values()) {
			try {
				boolean acknowledged = client.indices()
						.create(request -> request.index(kind.indexName()))
						.acknowledged();
				if (!acknowledged) {
					throw new IndexingAccessException(
							IndexingErrorCode.INDEXING_UNAVAILABLE);
				}
			}
			catch (ElasticsearchException exception) {
				if (!isIndexAlreadyCreated(exception)) {
					throw exception;
				}
			}
		}
	}

	private static BulkRequest request(
			BulkIndexCommand<? extends GdeltIndexedDocument> command,
			List<BinaryData> serializedDocuments
	) {
		BulkRequest.Builder builder = new BulkRequest.Builder()
				.includeSourceOnError(false);
		for (int position = 0; position < command.documents().size(); position++) {
			GdeltIndexedDocument document = command.documents().get(position);
			BinaryData serializedDocument = serializedDocuments.get(position);
			builder.operations(operation -> operation.index(index -> index
					.index(command.kind().indexName())
					.id(document.documentId())
					.document(serializedDocument)));
		}
		return builder.build();
	}

	private static void rejectFailedShards(ShardStatistics shards) {
		if (shards.failed().longValue() > 0) {
			throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		}
	}

	private static ArchiveReceiptVerification absentReceipt(ArchiveReceiptQuery query) {
		return new ArchiveReceiptVerification(
				query.kind(),
				query.expectedDocumentCount(),
				0,
				ArchiveReceiptStatus.INDEX_ABSENT);
	}

	private static boolean isIndexNotFound(ElasticsearchException exception) {
		return exception.error() != null
				&& INDEX_NOT_FOUND_ERROR_TYPE.equals(exception.error().type());
	}

	private static boolean isIndexAlreadyCreated(ElasticsearchException exception) {
		return exception.error() != null
				&& INDEX_ALREADY_EXISTS_ERROR_TYPE.equals(exception.error().type());
	}

	private static RuntimeException classify(ElasticsearchException exception) {
		if (isTransient(exception.status())) {
			return new IndexingAccessException(
					IndexingErrorCode.INDEXING_UNAVAILABLE,
					exception);
		}
		return new IndexingProtocolException(
				IndexingErrorCode.INDEXING_REQUEST_REJECTED,
				exception);
	}

	private static RuntimeException ioFailure(IOException exception) {
		if (Thread.currentThread().isInterrupted()) {
			return new IndexingInterruptedException(exception);
		}
		return new IndexingAccessException(
				IndexingErrorCode.INDEXING_UNAVAILABLE,
				exception);
	}

	private static void checkInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throw new IndexingInterruptedException();
		}
	}

	private static boolean isTransient(int status) {
		return status == 408 || status == 429 || (status >= 500 && status <= 599);
	}

	private static long utf8Length(String value) {
		return value.getBytes(StandardCharsets.UTF_8).length;
	}

	private BinaryData serialize(GdeltIndexedDocument document) {
		try {
			return BinaryData.of(document, client._jsonpMapper());
		}
		catch (JsonException exception) {
			throw new IndexingProtocolException(
					IndexingErrorCode.INDEXING_REQUEST_REJECTED,
					exception);
		}
	}

}
