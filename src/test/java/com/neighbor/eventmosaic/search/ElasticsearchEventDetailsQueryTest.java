package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonpMappingException;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.json.stream.JsonLocation;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@DisplayName("Поиск деталей Event в Elasticsearch")
class ElasticsearchEventDetailsQueryTest {

	private static final String DETAILS_DURATION_METER =
			"event_mosaic.search.details.duration";

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
	private final ElasticsearchEventDetailsQuery query =
			new ElasticsearchEventDetailsQuery(
					client,
					new EventSearchProperties(20),
					new SearchMetrics(meterRegistry));

	@Test
	@DisplayName("Отклоняет неположительный Event ID до обращения к Elasticsearch")
	void rejectsNonPositiveIdBeforeClientCall() {
		assertThatThrownBy(() -> query.findById(0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("eventId must be positive");

		verifyNoInteractions(client);
		assertThat(meterRegistry.find(DETAILS_DURATION_METER).timers()).isEmpty();
	}

	@Test
	@DisplayName("Преобразует transport failure в безопасную search exception")
	void convertsTransportFailureToSafeException() throws IOException {
		when(client.search(any(SearchRequest.class), eq(IndexedEventDocument.class)))
				.thenThrow(new IOException("secret transport detail"));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasMessageNotContaining("secret")
				.hasCauseInstanceOf(IOException.class);
		assertDetailsTimer("unavailable");
	}

	@Test
	@DisplayName("Отсутствующее событие один раз завершает запрос с результатом not found")
	void recordsNotFoundDetailsOnce() throws IOException {
		when(client.search(any(SearchRequest.class), eq(IndexedEventDocument.class)))
				.thenReturn(eventSearchResponse(List.of()));

		assertThat(query.findById(42)).isEmpty();

		assertDetailsTimer("not_found");
		verify(client, never()).search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class));
	}

	@Test
	@DisplayName("Не маскирует произвольный Elasticsearch 404 как отсутствие Event")
	void convertsNonIndexNotFoundResponseToSafeException() throws IOException {
		ElasticsearchException responseFailure = new ElasticsearchException(
				"event-details-get",
				ErrorResponse.of(response -> response
						.status(404)
						.error(error -> error
								.type("resource_not_found_exception")
								.reason("secret backend detail"))));
		when(client.search(any(SearchRequest.class), eq(IndexedEventDocument.class)))
				.thenThrow(responseFailure);

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasMessageNotContaining("secret")
				.hasCause(responseFailure);
	}

	@Test
	@DisplayName("Преобразует schema drift read model в безопасную search exception")
	void convertsMappingFailureToSafeException() throws IOException {
		JsonpMappingException mappingFailure = new JsonpMappingException(
				"secret schema detail",
				mock(JsonLocation.class));
		when(client.search(any(SearchRequest.class), eq(IndexedEventDocument.class)))
				.thenThrow(mappingFailure);

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasMessageNotContaining("secret")
				.hasCause(mappingFailure);
	}

	@Test
	@DisplayName("Не маскирует неожиданный runtime defect как доступность поиска")
	void propagatesUnexpectedRuntimeFailure() throws IOException {
		IllegalStateException defect = new IllegalStateException("unexpected defect");
		when(client.search(any(SearchRequest.class), eq(IndexedEventDocument.class)))
				.thenThrow(defect);

		assertThatThrownBy(() -> query.findById(42))
				.isSameAs(defect);
	}

	@Test
	@DisplayName("Запрашивает только нужные Mention fields без partial results и total hits")
	void requestsNarrowCompleteMentionProjection() throws IOException {
		stubFoundEvent();
		when(client.search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class)))
				.thenReturn(mentionSearchResponse(
						false,
						0,
						List.of(new MentionSourceProjection(
								"example.test",
								"https://example.test/article",
								Instant.parse("2026-07-30T10:05:00Z"),
								1.5))));

		var details = query.findById(42).orElseThrow();

		ArgumentCaptor<SearchRequest> eventRequestCaptor =
				ArgumentCaptor.forClass(SearchRequest.class);
		verify(client).search(
				eventRequestCaptor.capture(),
				eq(IndexedEventDocument.class));
		SearchRequest eventRequest = eventRequestCaptor.getValue();
		assertThat(eventRequest.index())
				.containsExactly(GdeltIndexKind.EVENT.readAlias());
		assertThat(eventRequest.size()).isEqualTo(2);
		assertThat(eventRequest.allowNoIndices()).isFalse();
		assertThat(eventRequest.ignoreUnavailable()).isFalse();
		assertThat(eventRequest.allowPartialSearchResults()).isFalse();
		assertThat(eventRequest.trackTotalHits().isEnabled()).isTrue();
		assertThat(eventRequest.trackTotalHits().enabled()).isTrue();
		assertThat(eventRequest.query().ids().values()).containsExactly("42");

		ArgumentCaptor<SearchRequest> mentionRequestCaptor =
				ArgumentCaptor.forClass(SearchRequest.class);
		verify(client).search(
				mentionRequestCaptor.capture(),
				eq(MentionSourceProjection.class));
		SearchRequest mentionRequest = mentionRequestCaptor.getValue();
		assertThat(mentionRequest.index())
				.containsExactly(GdeltIndexKind.MENTION.readAlias());
		assertThat(mentionRequest.allowNoIndices()).isFalse();
		assertThat(mentionRequest.ignoreUnavailable()).isFalse();
		assertThat(mentionRequest.allowPartialSearchResults()).isFalse();
		assertThat(mentionRequest.source().isFilter()).isTrue();
		assertThat(mentionRequest.source().filter().includes())
				.containsExactly(
						"mentionSourceName",
						"mentionIdentifier",
						"mentionTimeDate",
						"mentionDocTone");
		assertThat(mentionRequest.trackTotalHits().isEnabled()).isTrue();
		assertThat(mentionRequest.trackTotalHits().enabled()).isFalse();
		assertThat(details.sources()).singleElement().satisfies(source -> {
			assertThat(source.sourceName()).isEqualTo("example.test");
			assertThat(source.identifier()).isEqualTo("https://example.test/article");
			assertThat(source.observedAt())
					.isEqualTo(Instant.parse("2026-07-30T10:05:00Z"));
			assertThat(source.tone()).isEqualTo(1.5);
		});
		assertDetailsTimer("found");
	}

	@Test
	@DisplayName("Не выбирает произвольный Event при одинаковом ID в двух разделах")
	void rejectsAmbiguousEventAcrossPartitions() throws IOException {
		when(client.search(
				any(SearchRequest.class),
				eq(IndexedEventDocument.class)))
				.thenReturn(eventSearchResponse(List.of(
						eventHit("gdelt-events-v1-p20260720-g0001", event()),
						eventHit("gdelt-events-v1-p20260727-g0001", event()))));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasCauseInstanceOf(IllegalStateException.class);

		verify(client, never()).search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class));
	}

	@Test
	@DisplayName("Отклоняет неполный Event search до запроса связанных источников")
	void rejectsIncompleteEventSearchBeforeMentionQuery() throws IOException {
		when(client.search(
				any(SearchRequest.class),
				eq(IndexedEventDocument.class)))
				.thenReturn(eventSearchResponse(true, 0, List.of()));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasCauseInstanceOf(IllegalStateException.class);

		verify(client, never()).search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class));
	}

	@Test
	@DisplayName("Отклоняет timed out Mention search как временную недоступность")
	void rejectsTimedOutMentionSearch() throws IOException {
		stubFoundEvent();
		when(client.search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class)))
				.thenReturn(mentionSearchResponse(true, 0, List.of()));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasCauseInstanceOf(IllegalStateException.class);
	}

	@Test
	@DisplayName("Отклоняет failed shard Mention search как временную недоступность")
	void rejectsMentionSearchWithFailedShard() throws IOException {
		stubFoundEvent();
		when(client.search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class)))
				.thenReturn(mentionSearchResponse(false, 1, List.of()));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasCauseInstanceOf(IllegalStateException.class);
	}

	private void stubFoundEvent() throws IOException {
		when(client.search(
				any(SearchRequest.class),
				eq(IndexedEventDocument.class)))
				.thenReturn(eventSearchResponse(List.of(eventHit(
						"gdelt-events-v1-p20260720-g0001",
						event()))));
	}

	private void assertDetailsTimer(String outcome) {
		Timer timer = meterRegistry.find(DETAILS_DURATION_METER)
				.tag("outcome", outcome)
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(timer.getId().getTags())
				.extracting(Tag::getKey)
				.containsExactly("outcome");
		assertThat(meterRegistry.find(DETAILS_DURATION_METER).timers()).hasSize(1);
	}

	private static SearchResponse<IndexedEventDocument> eventSearchResponse(
			List<Hit<IndexedEventDocument>> hits
	) {
		return eventSearchResponse(false, 0, hits);
	}

	private static SearchResponse<IndexedEventDocument> eventSearchResponse(
			boolean timedOut,
			int failedShards,
			List<Hit<IndexedEventDocument>> hits
	) {
		return SearchResponse.of(response -> response
				.took(1)
				.timedOut(timedOut)
				.shards(shards -> shards
						.total(2)
						.successful(failedShards == 0 ? 2 : 1)
						.failed(failedShards))
				.hits(metadata -> metadata
						.total(total -> total
								.value(hits.size())
								.relation(TotalHitsRelation.Eq))
						.hits(hits)));
	}

	private static Hit<IndexedEventDocument> eventHit(
			String indexName,
			IndexedEventDocument source
	) {
		return Hit.of(hit -> hit
				.index(indexName)
				.id(source.documentId())
				.source(source));
	}

	private static SearchResponse<MentionSourceProjection> mentionSearchResponse(
			boolean timedOut,
			int failedShards,
			List<MentionSourceProjection> sources
	) {
		List<Hit<MentionSourceProjection>> hits = sources.stream()
				.map(source -> Hit.<MentionSourceProjection>of(hit -> hit
						.index("gdelt-mentions-v1-p20260720-g0001")
						.id(source.mentionIdentifier())
						.source(source)))
				.toList();
		return SearchResponse.of(response -> response
				.took(1)
				.timedOut(timedOut)
				.shards(shards -> shards
						.total(1)
						.successful(failedShards == 0 ? 1 : 0)
						.failed(failedShards))
				.hits(metadata -> metadata.hits(hits)));
	}

	private static IndexedEventDocument event() {
		return new IndexedEventDocument(
				42,
				LocalDate.of(2026, 7, 30),
				Instant.parse("2026-07-30T10:00:00Z"),
				"ACT1",
				"Actor 1",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"ACT2",
				"Actor 2",
				"",
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
				"01",
				1,
				1.5,
				-2.0,
				3,
				2,
				2,
				null,
				"",
				Instant.parse("2026-07-30T10:15:00Z"),
				"event-archive",
				1,
				"f".repeat(64));
	}
}
