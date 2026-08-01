package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonpMappingException;
import com.neighbor.eventmosaic.indexing.api.EventIdentityConflictException;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Строит fail-closed Event write plan по stable read alias без перезаписи.
 */
final class ElasticsearchEventIdentityGuard {

	private static final String EVENT_READ_ALIAS = "gdelt-events-read";
	private static final List<String> PROVENANCE_SOURCE_FIELDS = List.of(
			"sourceArchiveKey",
			"sourceLineNumber",
			"processingFingerprint");

	private final ElasticsearchClient client;

	ElasticsearchEventIdentityGuard(ElasticsearchClient client) {
		this.client = Objects.requireNonNull(client, "client must not be null");
	}

	/**
	 * Находит exact replay и документы, которые разрешено создать в current target.
	 *
	 * @param target exact Event target current ACTIVE generation
	 * @param documents bounded непустая Event bulk-порция
	 * @return immutable write plan в порядке исходной порции
	 */
	EventIdentityGuardPlan plan(
			ExactIndexTarget target,
			List<IndexedEventDocument> documents
	) {
		Objects.requireNonNull(target, "target must not be null");
		if (!GdeltIndexKind.EVENT.accepts(target)) {
			throw new IllegalArgumentException("target must be an Event physical index");
		}
		List<IndexedEventDocument> boundedDocuments = List.copyOf(
				Objects.requireNonNull(documents, "documents must not be null"));
		if (boundedDocuments.isEmpty()) {
			throw new IllegalArgumentException("documents must not be empty");
		}
		checkInterrupted();

		BatchCandidates candidates = collectCandidates(boundedDocuments);
		SearchResponse<EventIdentityProjection> response = search(candidates.ids());
		Set<String> existingIds = validateResponse(target, candidates, response);

		List<IndexedEventDocument> documentsToCreate = boundedDocuments.stream()
				.filter(document -> !existingIds.contains(document.documentId()))
				.toList();
		List<Integer> replayPositions = new ArrayList<>();
		for (int position = 0; position < boundedDocuments.size(); position++) {
			if (existingIds.contains(boundedDocuments.get(position).documentId())) {
				replayPositions.add(position);
			}
		}
		replayPositions.sort(Comparator.naturalOrder());
		return new EventIdentityGuardPlan(documentsToCreate, replayPositions);
	}

	private SearchResponse<EventIdentityProjection> search(List<String> ids) {
		SearchRequest request = SearchRequest.of(builder -> builder
				.index(EVENT_READ_ALIAS)
				.allowNoIndices(false)
				.ignoreUnavailable(false)
				.allowPartialSearchResults(false)
				.size(ids.size())
				.trackTotalHits(track -> track.enabled(true))
				.source(source -> source.filter(filter -> filter
						.includes(PROVENANCE_SOURCE_FIELDS)))
				.query(query -> query.ids(idsQuery -> idsQuery.values(ids))));
		try {
			return client.search(request, EventIdentityProjection.class);
		}
		catch (IOException exception) {
			throw ioFailure(exception);
		}
		catch (ElasticsearchException exception) {
			throw classify(exception);
		}
		catch (JsonpMappingException exception) {
			throw new IndexingProtocolException(
					IndexingErrorCode.INDEXING_RESPONSE_INVALID,
					exception);
		}
	}

	private static BatchCandidates collectCandidates(
			List<IndexedEventDocument> documents
	) {
		Map<String, Candidate> byId = new LinkedHashMap<>();
		for (int position = 0; position < documents.size(); position++) {
			IndexedEventDocument document = documents.get(position);
			Candidate existing = byId.putIfAbsent(
					document.documentId(),
					new Candidate(document, position));
			if (existing == null) {
				continue;
			}
			if (!sameProvenance(existing.document(), document)) {
				throw new EventIdentityConflictException(document.sourceLineNumber());
			}
		}
		return new BatchCandidates(
				Collections.unmodifiableMap(new LinkedHashMap<>(byId)),
				List.copyOf(byId.keySet()));
	}

	private static Set<String> validateResponse(
			ExactIndexTarget target,
			BatchCandidates candidates,
			SearchResponse<EventIdentityProjection> response
	) {
		if (response == null) {
			throw invalidResponse();
		}
		requireComplete(response);
		TotalHits total = response.hits().total();
		List<Hit<EventIdentityProjection>> hits = response.hits().hits();
		if (total == null
				|| total.relation() != TotalHitsRelation.Eq) {
			throw invalidResponse();
		}
		if (total.value() > candidates.byId().size()) {
			throw conflict(candidates);
		}
		if (total.value() != hits.size()) {
			throw invalidResponse();
		}

		Set<String> existingIds = new LinkedHashSet<>();
		for (Hit<EventIdentityProjection> hit : hits) {
			Candidate candidate = candidates.byId().get(hit.id());
			if (candidate == null) {
				throw invalidResponse();
			}
			if (!existingIds.add(hit.id())) {
				throw new EventIdentityConflictException(
						candidate.document().sourceLineNumber());
			}
			EventIdentityProjection projection = hit.source();
			if (!target.indexName().equals(hit.index())
					|| !validProjection(projection)
					|| !sameProvenance(candidate.document(), projection)) {
				throw new EventIdentityConflictException(
						candidate.document().sourceLineNumber());
			}
		}
		return Set.copyOf(existingIds);
	}

	private static void requireComplete(
			SearchResponse<EventIdentityProjection> response
	) {
		ShardStatistics shards = response.shards();
		if (response.timedOut()
				|| Boolean.TRUE.equals(response.terminatedEarly())
				|| shards == null
				|| shards.failed().longValue() > 0) {
			throw new IndexingAccessException(IndexingErrorCode.INDEXING_UNAVAILABLE);
		}
	}

	private static boolean validProjection(EventIdentityProjection projection) {
		return projection != null
				&& projection.sourceArchiveKey() != null
				&& !projection.sourceArchiveKey().isBlank()
				&& projection.sourceLineNumber() != null
				&& projection.sourceLineNumber() > 0
				&& projection.processingFingerprint() != null
				&& !projection.processingFingerprint().isBlank();
	}

	private static boolean sameProvenance(
			IndexedEventDocument left,
			IndexedEventDocument right
	) {
		return left.sourceArchiveKey().equals(right.sourceArchiveKey())
				&& left.sourceLineNumber() == right.sourceLineNumber()
				&& left.processingFingerprint().equals(right.processingFingerprint());
	}

	private static boolean sameProvenance(
			IndexedEventDocument document,
			EventIdentityProjection projection
	) {
		return document.sourceArchiveKey().equals(projection.sourceArchiveKey())
				&& document.sourceLineNumber() == projection.sourceLineNumber()
				&& document.processingFingerprint().equals(
						projection.processingFingerprint());
	}

	private static EventIdentityConflictException conflict(BatchCandidates candidates) {
		return new EventIdentityConflictException(
				candidates.byId().values().iterator().next().document().sourceLineNumber());
	}

	private static IndexingProtocolException invalidResponse() {
		return new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
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

	private record Candidate(IndexedEventDocument document, int firstPosition) {
	}

	private record BatchCandidates(
			Map<String, Candidate> byId,
			List<String> ids
	) {
	}
}
