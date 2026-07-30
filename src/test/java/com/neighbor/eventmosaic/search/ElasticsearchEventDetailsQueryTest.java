package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonpMappingException;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
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

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final ElasticsearchEventDetailsQuery query =
			new ElasticsearchEventDetailsQuery(client, new EventSearchProperties(20));

	@Test
	@DisplayName("Отклоняет неположительный Event ID до обращения к Elasticsearch")
	void rejectsNonPositiveIdBeforeClientCall() {
		assertThatThrownBy(() -> query.findById(0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessage("eventId must be positive");

		verifyNoInteractions(client);
	}

	@Test
	@DisplayName("Преобразует transport failure в безопасную search exception")
	void convertsTransportFailureToSafeException() throws IOException {
		when(client.get(any(GetRequest.class), eq(IndexedEventDocument.class)))
				.thenThrow(new IOException("secret transport detail"));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasMessageNotContaining("secret")
				.hasCauseInstanceOf(IOException.class);
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
		when(client.get(any(GetRequest.class), eq(IndexedEventDocument.class)))
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
		when(client.get(any(GetRequest.class), eq(IndexedEventDocument.class)))
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
		when(client.get(any(GetRequest.class), eq(IndexedEventDocument.class)))
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
				.thenReturn(searchResponse(
						false,
						0,
						List.of(new MentionSourceProjection(
								"example.test",
								"https://example.test/article",
								Instant.parse("2026-07-30T10:05:00Z"),
								1.5))));

		var details = query.findById(42).orElseThrow();

		ArgumentCaptor<SearchRequest> requestCaptor =
				ArgumentCaptor.forClass(SearchRequest.class);
		verify(client).search(
				requestCaptor.capture(),
				eq(MentionSourceProjection.class));
		SearchRequest request = requestCaptor.getValue();
		assertThat(request.allowPartialSearchResults()).isFalse();
		assertThat(request.source().isFilter()).isTrue();
		assertThat(request.source().filter().includes())
				.containsExactly(
						"mentionSourceName",
						"mentionIdentifier",
						"mentionTimeDate",
						"mentionDocTone");
		assertThat(request.trackTotalHits().isEnabled()).isTrue();
		assertThat(request.trackTotalHits().enabled()).isFalse();
		assertThat(details.sources()).singleElement().satisfies(source -> {
			assertThat(source.sourceName()).isEqualTo("example.test");
			assertThat(source.identifier()).isEqualTo("https://example.test/article");
			assertThat(source.observedAt())
					.isEqualTo(Instant.parse("2026-07-30T10:05:00Z"));
			assertThat(source.tone()).isEqualTo(1.5);
		});
	}

	@Test
	@DisplayName("Отклоняет timed out Mention search как временную недоступность")
	void rejectsTimedOutMentionSearch() throws IOException {
		stubFoundEvent();
		when(client.search(
				any(SearchRequest.class),
				eq(MentionSourceProjection.class)))
				.thenReturn(searchResponse(true, 0, List.of()));

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
				.thenReturn(searchResponse(false, 1, List.of()));

		assertThatThrownBy(() -> query.findById(42))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен")
				.hasCauseInstanceOf(IllegalStateException.class);
	}

	private void stubFoundEvent() throws IOException {
		when(client.get(any(GetRequest.class), eq(IndexedEventDocument.class)))
				.thenReturn(GetResponse.of(response -> response
						.index(GdeltIndexKind.EVENT.indexName())
						.id("42")
						.found(true)
						.source(event())));
	}

	private static SearchResponse<MentionSourceProjection> searchResponse(
			boolean timedOut,
			int failedShards,
			List<MentionSourceProjection> sources
	) {
		List<Hit<MentionSourceProjection>> hits = sources.stream()
				.map(source -> Hit.<MentionSourceProjection>of(hit -> hit
						.index(GdeltIndexKind.MENTION.indexName())
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
				"Actor 1",
				"ACT1",
				"Actor 2",
				"ACT2",
				"010",
				"01",
				"01",
				1,
				1.5,
				-2.0,
				null,
				Instant.parse("2026-07-30T10:15:00Z"),
				"event-archive",
				1);
	}
}
