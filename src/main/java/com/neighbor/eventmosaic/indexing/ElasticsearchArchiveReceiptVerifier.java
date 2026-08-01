package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Потоково строит фактический receipt exact target через bounded PIT/search_after.
 */
final class ElasticsearchArchiveReceiptVerifier {

	private static final String SOURCE_ARCHIVE_KEY_FIELD = "sourceArchiveKey";
	private static final String SOURCE_LINE_NUMBER_FIELD = "sourceLineNumber";
	private static final String PROCESSING_FINGERPRINT_FIELD = "processingFingerprint";
	private static final String PIT_KEEP_ALIVE = "1m";

	private final ElasticsearchClient client;

	ElasticsearchArchiveReceiptVerifier(ElasticsearchClient client) {
		this.client = Objects.requireNonNull(client, "client must not be null");
	}

	ArchiveReceiptVerification verify(ArchiveReceiptQuery query) throws IOException {
		OpenPointInTimeResponse opened = client.openPointInTime(request -> request
				.index(query.target().indexName())
				.keepAlive(time -> time.time(PIT_KEEP_ALIVE))
				.allowPartialSearchResults(false)
				.ignoreUnavailable(false));
		PitCursor pit = new PitCursor(requirePitId(opened.id()));
		Throwable primaryFailure = null;
		try {
			requireComplete(opened.shards());
			return scan(query, pit);
		}
		catch (IOException | RuntimeException exception) {
			primaryFailure = exception;
			throw exception;
		}
		finally {
			closePit(pit.id, primaryFailure);
		}
	}

	private ArchiveReceiptVerification scan(
			ArchiveReceiptQuery query,
			PitCursor pit
	) throws IOException {
		List<FieldValue> searchAfter = List.of();
		long actualCount = 0;
		ArchiveIdentityDigest.Accumulator digest = ArchiveIdentityDigest.accumulator();
		while (true) {
			SearchRequest request = searchRequest(query, pit.id, searchAfter);
			SearchResponse<Void> response = client.search(request);
			requireComplete(response);
			if (response.pitId() != null && !response.pitId().isBlank()) {
				pit.id = response.pitId();
			}
			List<Hit<Void>> hits = response.hits().hits();
			for (Hit<Void> hit : hits) {
				if (hit.id() == null || hit.id().isBlank()) {
					throw invalidResponse();
				}
				digest.addIdentity(hit.id());
				actualCount = Math.incrementExact(actualCount);
			}
			if (hits.size() < query.pageSize()) {
				ArchiveIdentityDigest actualDigest = digest.finish();
				return verification(query, actualCount, actualDigest);
			}
			List<FieldValue> lastSort = hits.get(hits.size() - 1).sort();
			if (lastSort == null || lastSort.size() < 2) {
				throw invalidResponse();
			}
			searchAfter = List.copyOf(lastSort);
		}
	}

	private static SearchRequest searchRequest(
			ArchiveReceiptQuery query,
			String pitId,
			List<FieldValue> searchAfter
	) {
		SearchRequest.Builder builder = new SearchRequest.Builder()
				.pit(pit -> pit.id(pitId)
						.keepAlive(time -> time.time(PIT_KEEP_ALIVE)))
				.size(query.pageSize())
				.allowPartialSearchResults(false)
				.source(source -> source.fetch(false))
				.trackTotalHits(track -> track.enabled(false))
				.query(root -> root.bool(bool -> bool
						.filter(filter -> filter.term(term -> term
								.field(SOURCE_ARCHIVE_KEY_FIELD)
								.value(query.sourceArchiveKey())))
						.filter(filter -> filter.term(term -> term
								.field(PROCESSING_FINGERPRINT_FIELD)
								.value(query.processingFingerprint())))))
				.sort(sort -> sort.field(field -> field
						.field(SOURCE_LINE_NUMBER_FIELD)
						.order(SortOrder.Asc)))
				.sort(sort -> sort.field(field -> field
						.field(query.kind().identityField())
						.order(SortOrder.Asc)));
		if (!searchAfter.isEmpty()) {
			builder.searchAfter(searchAfter);
		}
		return builder.build();
	}

	private void closePit(String pitId, Throwable primaryFailure) throws IOException {
		try {
			var response = client.closePointInTime(request -> request.id(pitId));
			if (!response.succeeded()) {
				throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
			}
		}
		catch (IOException | RuntimeException closeFailure) {
			if (primaryFailure != null) {
				primaryFailure.addSuppressed(closeFailure);
				return;
			}
			throw closeFailure;
		}
	}

	private static ArchiveReceiptVerification verification(
			ArchiveReceiptQuery query,
			long actualCount,
			ArchiveIdentityDigest actualDigest
	) {
		ArchiveReceiptStatus status;
		if (actualCount < query.expectedDocumentCount()) {
			status = ArchiveReceiptStatus.SHORTAGE;
		}
		else if (actualCount > query.expectedDocumentCount()) {
			status = ArchiveReceiptStatus.SURPLUS;
		}
		else if (!actualDigest.equals(query.expectedDigest())) {
			status = ArchiveReceiptStatus.IDENTITY_MISMATCH;
		}
		else {
			status = ArchiveReceiptStatus.MATCHED;
		}
		return new ArchiveReceiptVerification(
				query.kind(),
				query.expectedDocumentCount(),
				actualCount,
				query.expectedDigest(),
				actualDigest,
				status);
	}

	private static void requireComplete(SearchResponse<Void> response) {
		if (response.timedOut()
				|| Boolean.TRUE.equals(response.terminatedEarly())) {
			throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		}
		requireComplete(response.shards());
	}

	private static void requireComplete(ShardStatistics shards) {
		if (shards == null || shards.failed().longValue() > 0) {
			throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		}
	}

	private static String requirePitId(String pitId) {
		if (pitId == null || pitId.isBlank()) {
			throw invalidResponse();
		}
		return pitId;
	}

	private static IndexingProtocolException invalidResponse() {
		return new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
	}

	/** Mutable PIT id, который Elasticsearch может ротировать между страницами. */
	private static final class PitCursor {

		private String id;

		private PitCursor(String id) {
			this.id = id;
		}
	}
}
