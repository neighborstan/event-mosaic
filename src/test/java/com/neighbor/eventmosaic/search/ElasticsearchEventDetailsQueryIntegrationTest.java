package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import com.neighbor.eventmosaic.search.api.EventDetails;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
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
	private static final String EVENT_PHYSICAL =
			"gdelt-events-v1-p20260720-g9999";
	private static final String MENTION_PHYSICAL =
			"gdelt-mentions-v1-p20260720-g9999";
	private static final String SECOND_EVENT_PHYSICAL =
			"gdelt-events-v1-p20260727-g9999";
	private static final String SECOND_MENTION_PHYSICAL =
			"gdelt-mentions-v1-p20260727-g9999";
	private static final String SHADOW_EVENT_PHYSICAL =
			"gdelt-events-v1-p20260727-g10000";
	private static final String SHADOW_MENTION_PHYSICAL =
			"gdelt-mentions-v1-p20260727-g10000";
	private static final List<String> TEST_INDEX_NAMES = List.of(
			EVENT_PHYSICAL,
			MENTION_PHYSICAL,
			SECOND_EVENT_PHYSICAL,
			SECOND_MENTION_PHYSICAL,
			SHADOW_EVENT_PHYSICAL,
			SHADOW_MENTION_PHYSICAL);

	@Autowired
	private ElasticsearchClient client;

	@Autowired
	private GdeltIndexWriter indexWriter;

	private ElasticsearchEventDetailsQuery eventDetailsQuery;
	private ExactIndexTarget eventTarget;
	private ExactIndexTarget mentionTarget;

	@BeforeEach
	void resetReadModel() throws IOException {
		removeStableAliasMemberships();
		deleteTestIndices();
		indexWriter.prepareReadModel();
		eventTarget = createTarget(EVENT_PHYSICAL);
		mentionTarget = createTarget(MENTION_PHYSICAL);
		addStableAliases(EVENT_PHYSICAL, MENTION_PHYSICAL);
		eventDetailsQuery =
				new ElasticsearchEventDetailsQuery(
						client,
						new EventSearchProperties(2),
						new SearchMetrics(new SimpleMeterRegistry()));
	}

	@AfterEach
	void cleanReadModel() throws IOException {
		removeStableAliasMemberships();
		deleteTestIndices();
	}

	@Test
	@DisplayName("Возвращает bounded уникальные sources с новым observation группы")
	void returnsCollapsedAndLimitedSources() {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.MENTION,
				mentionTarget,
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
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
		indexWriter.refresh(GdeltIndexKind.MENTION, mentionTarget);

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
	@DisplayName("Для двух разделов читает только текущие поколения и не видит теневое")
	void readsOnlyCurrentGenerationsAcrossTwoPartitions() throws IOException {
		ExactIndexTarget secondEventTarget = createTarget(SECOND_EVENT_PHYSICAL);
		ExactIndexTarget secondMentionTarget = createTarget(SECOND_MENTION_PHYSICAL);
		ExactIndexTarget shadowEventTarget = createTarget(SHADOW_EVENT_PHYSICAL);
		ExactIndexTarget shadowMentionTarget = createTarget(SHADOW_MENTION_PHYSICAL);

		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				shadowEventTarget,
				List.of(event(
						EVENT_ID,
						"SHADOW EVENT",
						"shadow-event-archive",
						3))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				secondEventTarget,
				List.of(event(
						EVENT_ID + 1,
						"SECOND ACTIVE EVENT",
						"second-event-archive",
						2))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.MENTION,
				mentionTarget,
				List.of(mention(
						"rm-active-first",
						"sd-active-first",
						"https://example.test/active-first",
						"2026-07-21T14:40:00Z",
						1.0,
						1))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.MENTION,
				secondMentionTarget,
				List.of(mention(
						"rm-active-second",
						"sd-active-second",
						"https://example.test/active-second",
						"2026-07-28T14:40:00Z",
						2.0,
						2))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.MENTION,
				shadowMentionTarget,
				List.of(mention(
						"rm-shadow",
						"sd-shadow",
						"https://example.test/shadow",
						"2026-07-29T14:40:00Z",
						3.0,
						3))));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
		indexWriter.refresh(GdeltIndexKind.EVENT, secondEventTarget);
		indexWriter.refresh(GdeltIndexKind.EVENT, shadowEventTarget);
		indexWriter.refresh(GdeltIndexKind.MENTION, mentionTarget);
		indexWriter.refresh(GdeltIndexKind.MENTION, secondMentionTarget);
		indexWriter.refresh(GdeltIndexKind.MENTION, shadowMentionTarget);
		addStableAliases(SECOND_EVENT_PHYSICAL, SECOND_MENTION_PHYSICAL);

		var details = eventDetailsQuery.findById(EVENT_ID).orElseThrow();

		assertThat(stableAliasIndices(GdeltIndexKind.EVENT))
				.containsExactly(EVENT_PHYSICAL, SECOND_EVENT_PHYSICAL);
		assertThat(stableAliasIndices(GdeltIndexKind.MENTION))
				.containsExactly(MENTION_PHYSICAL, SECOND_MENTION_PHYSICAL);
		assertThat(details.actors().actor1().name()).isEqualTo("UNITED STATES");
		assertThat(details.sources())
				.extracting(EventDetails.SourceDocument::identifier)
				.containsExactly(
						"https://example.test/active-second",
						"https://example.test/active-first")
				.doesNotContain("https://example.test/shadow");
	}

	@Test
	@DisplayName("Возвращает безопасную ошибку, если один Event виден в двух разделах")
	void rejectsEventVisibleInTwoPartitions() throws IOException {
		ExactIndexTarget secondEventTarget = createTarget(SECOND_EVENT_PHYSICAL);
		createTarget(SECOND_MENTION_PHYSICAL);
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				secondEventTarget,
				List.of(event(
						EVENT_ID,
						"DUPLICATE EVENT",
						"duplicate-event-archive",
						2))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
		indexWriter.refresh(GdeltIndexKind.EVENT, secondEventTarget);
		addStableAliases(SECOND_EVENT_PHYSICAL, SECOND_MENTION_PHYSICAL);

		assertThatThrownBy(() -> eventDetailsQuery.findById(EVENT_ID))
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен");
	}

	@Test
	@DisplayName("Отсутствующий Event index возвращает not found")
	void returnsNotFoundWhenEventIndexIsAbsent() throws IOException {
		client.indices().delete(request -> request.index(EVENT_PHYSICAL));

		assertThat(eventDetailsQuery.findById(EVENT_ID)).isEmpty();
	}

	@Test
	@DisplayName("Неполный read model без Mention index возвращает not found")
	void returnsNotFoundWhenMentionIndexIsAbsent() throws IOException {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
		client.indices().delete(request -> request.index(MENTION_PHYSICAL));

		assertThat(eventDetailsQuery.findById(EVENT_ID)).isEmpty();
	}

	@Test
	@DisplayName("Отсутствующий документ в существующем Event index возвращает not found")
	void returnsNotFoundWhenEventDocumentIsAbsent() {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);

		assertThat(eventDetailsQuery.findById(EVENT_ID + 1)).isEmpty();
	}

	@Test
	@DisplayName("Не маскирует реальный Elasticsearch read failure как not found")
	void reportsExistingIndexReadFailure() throws IOException {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				List.of(event())));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
		client.indices().close(
				request -> request.index(EVENT_PHYSICAL));
		try {
			assertThatThrownBy(() -> eventDetailsQuery.findById(EVENT_ID))
					.isInstanceOf(SearchAccessException.class)
					.hasMessage("Сервис поиска временно недоступен");
		} finally {
			client.indices().open(
					request -> request.index(EVENT_PHYSICAL));
		}
	}

	private ExactIndexTarget createTarget(String indexName) throws IOException {
		client.indices().create(request -> request.index(indexName));
		return target(indexName);
	}

	private ExactIndexTarget target(String indexName) throws IOException {
		var settings = client.indices()
				.get(request -> request.index(indexName))
				.get(indexName)
				.settings();
		var indexSettings = settings.index() == null ? settings : settings.index();
		return new ExactIndexTarget(indexName, indexSettings.uuid());
	}

	private void addStableAliases(String eventIndexName, String mentionIndexName)
			throws IOException {
		client.indices().updateAliases(request -> request
				.actions(action -> action.add(add -> add
						.index(eventIndexName)
						.alias(GdeltIndexKind.EVENT.readAlias())))
				.actions(action -> action.add(add -> add
						.index(mentionIndexName)
						.alias(GdeltIndexKind.MENTION.readAlias()))));
	}

	private void removeStableAliasMemberships() throws IOException {
		for (GdeltIndexKind kind : GdeltIndexKind.values()) {
			for (String indexName : stableAliasIndices(kind)) {
				client.indices().deleteAlias(request -> request
						.index(indexName)
						.name(kind.readAlias()));
			}
		}
	}

	private List<String> stableAliasIndices(GdeltIndexKind kind) throws IOException {
		try {
			var response = client.indices().getAlias(request -> request
					.name(kind.readAlias())
					.allowNoIndices(true)
					.ignoreUnavailable(true));
			return response.aliases().entrySet().stream()
					.filter(entry -> entry.getValue().aliases().containsKey(kind.readAlias()))
					.map(java.util.Map.Entry::getKey)
					.sorted()
					.toList();
		}
		catch (ElasticsearchException exception) {
			if (exception.status() == 404) {
				return List.of();
			}
			throw exception;
		}
	}

	private void deleteTestIndices() throws IOException {
		client.indices().delete(request -> request
				.index(TEST_INDEX_NAMES)
				.allowNoIndices(true)
				.ignoreUnavailable(true));
	}

	private static IndexedEventDocument event() {
		return event(EVENT_ID, "UNITED STATES", EVENT_ARCHIVE, 1);
	}

	private static IndexedEventDocument event(
			long eventId,
			String actor1Name,
			String sourceArchiveKey,
			long sourceLineNumber
	) {
		return new IndexedEventDocument(
				eventId,
				LocalDate.of(2026, 7, 21),
				UPDATE_TIME,
				"USA",
				actor1Name,
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
				sourceArchiveKey,
				sourceLineNumber,
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
