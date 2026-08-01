package com.neighbor.eventmosaic.indexing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.neighbor.eventmosaic.indexing.api.EventIdentityConflictException;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@DisplayName("Fail-closed Event identity guard")
class ElasticsearchEventIdentityGuardTest {

	private static final ExactIndexTarget TARGET = new ExactIndexTarget(
			"gdelt-events-v1-p20260727-g0001",
			"event-index-uuid");
	private static final String PROCESSING_FINGERPRINT = "a".repeat(64);

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final ElasticsearchEventIdentityGuard guard =
			new ElasticsearchEventIdentityGuard(client);

	@Test
	@DisplayName("Одним bounded search планирует unique creates и внутрипакетный replay")
	void plansUniqueCreatesAndBatchReplayWithOneBoundedSearch() throws IOException {
		IndexedEventDocument first = event(1, "archive-a", 11);
		IndexedEventDocument second = event(2, "archive-a", 12);
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(false, 0, 0, TotalHitsRelation.Eq, List.of()));

		EventIdentityGuardPlan plan = guard.plan(
				TARGET,
				List.of(first, first, second));

		assertThat(plan.documentsToCreate()).containsExactly(first, first, second);
		assertThat(plan.replayPositions()).isEmpty();
		assertThat(plan.replayCount()).isZero();

		ArgumentCaptor<SearchRequest> requestCaptor =
				ArgumentCaptor.forClass(SearchRequest.class);
		verify(client).search(
				requestCaptor.capture(),
				eq(EventIdentityProjection.class));
		SearchRequest request = requestCaptor.getValue();
		assertThat(request.index()).containsExactly("gdelt-events-read");
		assertThat(request.allowNoIndices()).isFalse();
		assertThat(request.ignoreUnavailable()).isFalse();
		assertThat(request.allowPartialSearchResults()).isFalse();
		assertThat(request.size()).isEqualTo(2);
		assertThat(request.trackTotalHits().isEnabled()).isTrue();
		assertThat(request.trackTotalHits().enabled()).isTrue();
		assertThat(request.source().filter().includes())
				.containsExactly(
						"sourceArchiveKey",
						"sourceLineNumber",
						"processingFingerprint");
		assertThat(request.query().ids().values()).containsExactly("1", "2");
	}

	@Test
	@DisplayName("Exact provenance в current target становится идемпотентным replay")
	void treatsExactCurrentTargetHitAsReplay() throws IOException {
		IndexedEventDocument document = event(1, "archive-a", 11);
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(
						false,
						0,
						1,
						TotalHitsRelation.Eq,
						List.of(hit(TARGET.indexName(), document))));

		EventIdentityGuardPlan plan = guard.plan(TARGET, List.of(document));

		assertThat(plan.documentsToCreate()).isEmpty();
		assertThat(plan.replayPositions()).containsExactly(0);
	}

	@Test
	@DisplayName("Другая physical line одного ID отклоняется до Elasticsearch")
	void rejectsDifferentBatchLineBeforeSearch() {
		IndexedEventDocument first = event(1, "archive-a", 11);
		IndexedEventDocument collision = event(1, "archive-a", 12);

		assertThatExceptionOfType(EventIdentityConflictException.class)
				.isThrownBy(() -> guard.plan(TARGET, List.of(first, collision)))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.EVENT_IDENTITY_CONFLICT);
					assertThat(exception.retryable()).isFalse();
					assertThat(exception.sourceLineNumber()).isEqualTo(12);
				});
		verifyNoInteractions(client);
	}

	@Test
	@DisplayName("Сохраненная provenance другого archive отклоняется как conflict")
	void rejectsStoredProvenanceMismatch() throws IOException {
		IndexedEventDocument candidate = event(1, "archive-a", 11);
		IndexedEventDocument stored = event(1, "archive-b", 11);
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(
						false,
						0,
						1,
						TotalHitsRelation.Eq,
						List.of(hit(TARGET.indexName(), stored))));

		assertConflict(candidate);
	}

	@Test
	@DisplayName("Exact provenance вне current target отклоняется как conflict")
	void rejectsExactProvenanceOutsideCurrentTarget() throws IOException {
		IndexedEventDocument candidate = event(1, "archive-a", 11);
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(
						false,
						0,
						1,
						TotalHitsRelation.Eq,
						List.of(hit(
								"gdelt-events-v1-p20260727-g0002",
								candidate))));

		assertConflict(candidate);
	}

	@Test
	@DisplayName("Два видимых hit одного ID отклоняются как identity conflict")
	void rejectsDuplicateVisibleIdentity() throws IOException {
		IndexedEventDocument candidate = event(1, "archive-a", 11);
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(
						false,
						0,
						2,
						TotalHitsRelation.Eq,
						List.of(
								hit(TARGET.indexName(), candidate),
								hit(
										"gdelt-events-v1-p20260727-g0002",
										candidate))));

		assertConflict(candidate);
	}

	@Test
	@DisplayName("Неполная сохраненная provenance отклоняется как identity conflict")
	void rejectsMalformedStoredProvenance() throws IOException {
		IndexedEventDocument candidate = event(1, "archive-a", 11);
		Hit<EventIdentityProjection> malformed = Hit.of(hit -> hit
				.index(TARGET.indexName())
				.id(candidate.documentId())
				.source(new EventIdentityProjection("archive-a", 11L, null)));
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response(
						false,
						0,
						1,
						TotalHitsRelation.Eq,
						List.of(malformed)));

		assertConflict(candidate);
	}

	@Test
	@DisplayName("Search timeout остается retryable infrastructure failure")
	void rejectsTimedOutSearch() throws IOException {
		stubResponse(response(true, 0, 0, TotalHitsRelation.Eq, List.of()));

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> guard.plan(
						TARGET,
						List.of(event(1, "archive-a", 11))))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("Failed shard остается retryable infrastructure failure")
	void rejectsFailedShard() throws IOException {
		stubResponse(response(false, 1, 0, TotalHitsRelation.Eq, List.of()));

		assertThatExceptionOfType(IndexingAccessException.class)
				.isThrownBy(() -> guard.plan(
						TARGET,
						List.of(event(1, "archive-a", 11))))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.INDEXING_UNAVAILABLE);
					assertThat(exception.retryable()).isTrue();
				});
	}

	@Test
	@DisplayName("Не exact total hits отклоняется как malformed response")
	void rejectsInexactTotalHits() throws IOException {
		stubResponse(response(false, 0, 0, TotalHitsRelation.Gte, List.of()));

		assertThatExceptionOfType(IndexingProtocolException.class)
				.isThrownBy(() -> guard.plan(
						TARGET,
						List.of(event(1, "archive-a", 11))))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(IndexingErrorCode.INDEXING_RESPONSE_INVALID));
	}

	private void assertConflict(IndexedEventDocument candidate) {
		assertThatExceptionOfType(EventIdentityConflictException.class)
				.isThrownBy(() -> guard.plan(TARGET, List.of(candidate)))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IndexingErrorCode.EVENT_IDENTITY_CONFLICT);
					assertThat(exception.retryable()).isFalse();
					assertThat(exception.sourceLineNumber())
							.isEqualTo(candidate.sourceLineNumber());
				});
	}

	private void stubResponse(SearchResponse<EventIdentityProjection> response)
			throws IOException {
		when(client.search(
				any(SearchRequest.class),
				eq(EventIdentityProjection.class)))
				.thenReturn(response);
	}

	private static SearchResponse<EventIdentityProjection> response(
			boolean timedOut,
			int failedShards,
			long total,
			TotalHitsRelation relation,
			List<Hit<EventIdentityProjection>> hits
	) {
		return SearchResponse.of(response -> response
				.took(1)
				.timedOut(timedOut)
				.shards(shards -> shards
						.total(1)
						.successful(failedShards == 0 ? 1 : 0)
						.failed(failedShards))
				.hits(metadata -> metadata
						.total(totalHits -> totalHits
								.value(total)
								.relation(relation))
						.hits(hits)));
	}

	private static Hit<EventIdentityProjection> hit(
			String indexName,
			IndexedEventDocument document
	) {
		return Hit.of(hit -> hit
				.index(indexName)
				.id(document.documentId())
				.source(new EventIdentityProjection(
						document.sourceArchiveKey(),
						document.sourceLineNumber(),
						document.processingFingerprint())));
	}

	private static IndexedEventDocument event(
			long globalEventId,
			String sourceArchiveKey,
			long sourceLineNumber
	) {
		return new IndexedEventDocument(
				globalEventId,
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
				sourceArchiveKey,
				sourceLineNumber,
				PROCESSING_FINGERPRINT);
	}
}
