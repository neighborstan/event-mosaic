package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import com.neighbor.eventmosaic.search.api.EventDetails;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("Интеграция Event details query с Elasticsearch")
class ElasticsearchEventDetailsQueryIntegrationTest {

	private static final long EVENT_ID = 1_234_567_890L;
	private static final Instant UPDATE_TIME = Instant.parse("2026-07-21T14:45:00Z");
	private static final String EVENT_ARCHIVE = "event-archive";
	private static final String MENTION_ARCHIVE = "mention-archive";

	@Autowired
	private ElasticsearchClient client;

	@Autowired
	private GdeltIndexWriter indexWriter;

	private ElasticsearchEventDetailsQuery eventDetailsQuery;

	@BeforeEach
	void resetReadModel() throws IOException {
		client.indices().delete(request -> request
				.index(
						GdeltIndexKind.EVENT.indexName(),
						GdeltIndexKind.MENTION.indexName())
				.allowNoIndices(true)
				.ignoreUnavailable(true));
		indexWriter.prepareReadModel();
		eventDetailsQuery =
				new ElasticsearchEventDetailsQuery(client, new EventSearchProperties(2));
	}

	@Test
	@DisplayName("Возвращает bounded уникальные sources с новым observation группы")
	void returnsCollapsedAndLimitedSources() {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				List.of(event())));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.MENTION,
				List.of(
						mention(
								"rm1-a-old",
								"sd1-a",
								"https://example.test/a",
								"2026-07-21T14:40:00Z",
								0.5,
								1),
						mention(
								"rm1-z",
								"sd1-a",
								"https://example.test/a",
								"2026-07-21T14:49:00Z",
								9.5,
								2),
						mention(
								"rm1-a",
								"sd1-a",
								"https://example.test/a",
								"2026-07-21T14:49:00Z",
								1.5,
								3),
						mention(
								"rm1-b",
								"sd1-b",
								"https://example.test/b",
								"2026-07-21T14:48:00Z",
								2.5,
								4),
						mention(
								"rm1-c",
								"sd1-c",
								"https://example.test/c",
								"2026-07-21T14:47:00Z",
								3.5,
								5))));
		indexWriter.refresh(GdeltIndexKind.EVENT);
		indexWriter.refresh(GdeltIndexKind.MENTION);

		var details = eventDetailsQuery.findById(EVENT_ID).orElseThrow();

		assertThat(details.event().eventId()).isEqualTo(EVENT_ID);
		assertThat(details.actors().actor1().code()).isEqualTo("USA");
		assertThat(details.location().role()).isEqualTo("ACTION");
		assertThat(details.sources())
				.extracting(EventDetails.SourceDocument::identifier)
				.containsExactly(
						"https://example.test/a",
						"https://example.test/b");
		assertThat(details.sources().getFirst().observedAt())
				.isEqualTo(Instant.parse("2026-07-21T14:49:00Z"));
		assertThat(details.sources().getFirst().tone()).isEqualTo(1.5);
	}

	@Test
	@DisplayName("Отсутствующий Event index возвращает not found")
	void returnsNotFoundWhenEventIndexIsAbsent() throws IOException {
		client.indices().delete(request -> request.index(GdeltIndexKind.EVENT.indexName()));

		assertThat(eventDetailsQuery.findById(EVENT_ID)).isEmpty();
	}

	@Test
	@DisplayName("Неполный read model без Mention index возвращает not found")
	void returnsNotFoundWhenMentionIndexIsAbsent() throws IOException {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT);
		client.indices().delete(request -> request.index(GdeltIndexKind.MENTION.indexName()));

		assertThat(eventDetailsQuery.findById(EVENT_ID)).isEmpty();
	}

	@Test
	@DisplayName("Отсутствующий документ в существующем Event index возвращает not found")
	void returnsNotFoundWhenEventDocumentIsAbsent() {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT);

		assertThat(eventDetailsQuery.findById(EVENT_ID + 1)).isEmpty();
	}

	@Test
	@DisplayName("Не маскирует реальный Elasticsearch read failure как not found")
	void reportsExistingIndexReadFailure() throws IOException {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT);
		client.indices().close(
				request -> request.index(GdeltIndexKind.EVENT.indexName()));
		try {
			assertThatThrownBy(() -> eventDetailsQuery.findById(EVENT_ID))
					.isInstanceOf(SearchAccessException.class)
					.hasMessage("Сервис поиска временно недоступен");
		} finally {
			client.indices().open(
					request -> request.index(GdeltIndexKind.EVENT.indexName()));
		}
	}

	private static IndexedEventDocument event() {
		return new IndexedEventDocument(
				EVENT_ID,
				LocalDate.of(2026, 7, 21),
				UPDATE_TIME,
				"USA",
				"UNITED STATES",
				"US",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"RUS",
				"RUSSIA",
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
				"01",
				1,
				-1.5,
				2.25,
				3,
				2,
				2,
				new IndexedEventLocation(
						IndexedLocationRole.ACTION,
						1,
						"Moscow, Moskva, Russia",
						"RS",
						"RS48",
						"",
						"-2960561",
						new IndexedGeoPoint(55.7558, 37.6173)),
				"https://example.test/event",
				UPDATE_TIME,
				EVENT_ARCHIVE,
				1,
				"e".repeat(64));
	}

	private static IndexedMentionDocument mention(
			String rawMentionId,
			String sourceDocumentKey,
			String identifier,
			String observedAt,
			double tone,
			long lineNumber
	) {
		return new IndexedMentionDocument(
				rawMentionId,
				sourceDocumentKey,
				EVENT_ID,
				UPDATE_TIME,
				Instant.parse(observedAt),
				1,
				"example.test",
				identifier,
				1,
				0,
				0,
				0,
				1,
				100,
				500,
				tone,
				"",
				"",
				UPDATE_TIME,
				MENTION_ARCHIVE,
				lineNumber,
				"f".repeat(64));
	}
}
