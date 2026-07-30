package com.neighbor.eventmosaic.indexing;

import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT;
import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.MENTION;
import static org.assertj.core.api.Assertions.assertThat;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@DisplayName("Elasticsearch adapter индексирования GDELT")
class ElasticsearchGdeltIndexWriterIntegrationTest {

	private static final String EVENT_TEMPLATE = "gdelt-events-v1-template";
	private static final String MENTION_TEMPLATE = "gdelt-mentions-v1-template";
	private static final String STRICT_DYNAMIC_MAPPING_ERROR_TYPE =
			"strict_dynamic_mapping_exception";
	private static final String EVENT_ARCHIVE = "20260730101500.translation.export.CSV.zip";
	private static final String MENTION_ARCHIVE = "20260730101500.translation.mentions.CSV.zip";

	@Autowired
	private GdeltIndexWriter writer;

	@Autowired
	private ElasticsearchClient client;

	@BeforeEach
	void resetIndicesAndPrepareTemplates() throws IOException {
		client.indices().delete(request -> request
				.index(EVENT.indexName(), MENTION.indexName())
				.ignoreUnavailable(true)
				.allowNoIndices(true));
		writer.prepareReadModel();
	}

	@Test
	@DisplayName("Повторная установка templates идемпотентна, а strict mapping дает permanent item")
	void installsStrictTemplatesIdempotently() throws IOException {
		writer.prepareReadModel();

		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.value()).isTrue();
		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(MENTION_TEMPLATE))
				.value()).isTrue();
		assertThat(client.indices()
				.exists(request -> request.index(EVENT.indexName()))
				.value()).isTrue();
		assertThat(client.indices()
				.exists(request -> request.index(MENTION.indexName()))
				.value()).isTrue();

		BulkResponse eventFailure = indexUnknownField(EVENT.indexName(), "unknown-event");
		BulkResponse mentionFailure = indexUnknownField(MENTION.indexName(), "unknown-mention");

		assertThat(eventFailure.items()).singleElement().satisfies(item -> {
			assertThat(item.status()).isEqualTo(400);
			assertThat(item.error()).isNotNull();
		});
		assertThat(mentionFailure.items()).singleElement().satisfies(item -> {
			assertThat(item.status()).isEqualTo(400);
			assertThat(item.error()).isNotNull();
		});

		var classified = BulkResponseAnalyzer.analyze(
				new BulkIndexCommand<>(EVENT, List.of(event(1001, 1, "Actor before"))),
				eventFailure.items());
		assertThat(classified.outcome())
				.isEqualTo(BulkIndexOutcome.NON_RETRYABLE_PARTIAL_FAILURE);
	}

	@Test
	@DisplayName("Восстанавливает strict template до повторного создания удаленного индекса")
	void restoresStrictTemplateBeforeRecreatingDeletedIndex() throws IOException {
		client.indices().deleteIndexTemplate(request -> request.name(EVENT_TEMPLATE));
		client.indices().delete(request -> request.index(EVENT.indexName()));

		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.value()).isFalse();
		assertThat(client.indices()
				.exists(request -> request.index(EVENT.indexName()))
				.value()).isFalse();

		writer.prepareReadModel();

		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.value()).isTrue();
		assertThat(client.indices()
				.exists(request -> request.index(EVENT.indexName()))
				.value()).isTrue();
		BulkResponse strictMappingFailure = indexUnknownField(
				EVENT.indexName(),
				"unknown-event-after-self-heal");
		assertThat(strictMappingFailure.errors()).isTrue();
		assertThat(strictMappingFailure.items()).singleElement().satisfies(item -> {
			assertThat(item.status()).isEqualTo(400);
			assertThat(item.error()).isNotNull();
			assertThat(item.error().type()).isEqualTo(STRICT_DYNAMIC_MAPPING_ERROR_TYPE);
		});
	}

	@Test
	@DisplayName("Стабильные IDs перезаписывают Event и Mention без дубликатов")
	void overwritesDocumentsByStableIds() throws IOException {
		IndexedEventDocument initialEvent = event(2001, 2, "Actor before");
		IndexedEventDocument updatedEvent = event(2001, 2, "Actor after");
		IndexedMentionDocument initialMention = mention("m1", 2001, 3, -1.5);
		IndexedMentionDocument updatedMention = mention("m1", 2001, 3, 2.5);

		assertThat(writer.write(new BulkIndexCommand<>(EVENT, List.of(initialEvent))).successful())
				.isTrue();
		assertThat(writer.write(new BulkIndexCommand<>(EVENT, List.of(updatedEvent))).successful())
				.isTrue();
		assertThat(writer.write(new BulkIndexCommand<>(MENTION, List.of(initialMention))).successful())
				.isTrue();
		assertThat(writer.write(new BulkIndexCommand<>(MENTION, List.of(updatedMention))).successful())
				.isTrue();
		writer.refresh(EVENT);
		writer.refresh(MENTION);

		assertThat(client.count(request -> request.index(EVENT.indexName())).count()).isEqualTo(1);
		assertThat(client.count(request -> request.index(MENTION.indexName())).count()).isEqualTo(1);
		var storedEvent = client.get(
				request -> request.index(EVENT.indexName()).id(initialEvent.documentId()),
				IndexedEventDocument.class);
		var storedMention = client.get(
				request -> request.index(MENTION.indexName()).id(initialMention.documentId()),
				IndexedMentionDocument.class);
		assertThat(storedEvent.found()).isTrue();
		assertThat(storedEvent.source())
				.isNotNull()
				.extracting(IndexedEventDocument::actor1Name)
				.isEqualTo("Actor after");
		assertThat(storedMention.found()).isTrue();
		assertThat(storedMention.source())
				.isNotNull()
				.extracting(IndexedMentionDocument::mentionDocTone)
				.isEqualTo(2.5);
	}

	@Test
	@DisplayName("Receipt использует точный archive key и различает удаленный индекс")
	void verifiesExactReceiptAndDeletedIndex() throws IOException {
		IndexedEventDocument exactArchive = event(3001, 4, "Exact archive");
		IndexedEventDocument similarArchive = event(
				3002,
				5,
				"Similar archive",
				EVENT_ARCHIVE + ".other");
		writer.write(new BulkIndexCommand<>(
				EVENT,
				List.of(exactArchive, similarArchive)));
		writer.refresh(EVENT);

		var matched = writer.verifyReceipt(new ArchiveReceiptQuery(EVENT, EVENT_ARCHIVE, 1));
		var mismatched = writer.verifyReceipt(new ArchiveReceiptQuery(EVENT, EVENT_ARCHIVE, 2));

		assertThat(matched.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
		assertThat(matched.actualDocumentCount()).isEqualTo(1);
		assertThat(mismatched.status()).isEqualTo(ArchiveReceiptStatus.MISMATCHED);
		assertThat(mismatched.actualDocumentCount()).isEqualTo(1);

		client.indices().delete(request -> request.index(EVENT.indexName()));

		var absent = writer.verifyReceipt(new ArchiveReceiptQuery(EVENT, EVENT_ARCHIVE, 1));
		assertThat(absent.status()).isEqualTo(ArchiveReceiptStatus.INDEX_ABSENT);
		assertThat(absent.actualDocumentCount()).isZero();
	}

	@Test
	@DisplayName("Явный refresh делает видимыми все shards целевого индекса")
	void refreshesEveryTargetShardBeforeReceipt() throws IOException {
		client.indices().delete(request -> request.index(EVENT.indexName()));
		client.indices().create(request -> request
				.index(EVENT.indexName())
				.settings(settings -> settings
						.numberOfShards("2")
						.refreshInterval(interval -> interval.time("-1"))));
		List<IndexedEventDocument> documents = java.util.stream.LongStream
				.rangeClosed(1, 32)
				.mapToObj(value -> event(
						4_000 + value,
						10 + value,
						"Actor " + value))
				.toList();

		assertThat(writer.write(new BulkIndexCommand<>(EVENT, documents)).successful())
				.isTrue();
		assertThat(writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				EVENT_ARCHIVE,
				documents.size())).status())
				.isEqualTo(ArchiveReceiptStatus.MISMATCHED);

		writer.refresh(EVENT);

		var receipt = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				EVENT_ARCHIVE,
				documents.size()));
		assertThat(receipt.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
		assertThat(receipt.actualDocumentCount()).isEqualTo(documents.size());
	}

	private BulkResponse indexUnknownField(String indexName, String id) throws IOException {
		return client.bulk(request -> request
				.refresh(Refresh.WaitFor)
				.operations(operation -> operation.index(index -> index
						.index(indexName)
						.id(id)
						.document(Map.of("unexpectedField", "must be rejected")))));
	}

	private static IndexedEventDocument event(long id, long line, String actor1Name) {
		return event(id, line, actor1Name, EVENT_ARCHIVE);
	}

	private static IndexedEventDocument event(
			long id,
			long line,
			String actor1Name,
			String archiveKey
	) {
		return new IndexedEventDocument(
				id,
				LocalDate.of(2026, 7, 30),
				Instant.parse("2026-07-30T10:00:00Z"),
				actor1Name,
				"ACT1",
				"Actor 2",
				"ACT2",
				"010",
				"01",
				"0",
				1,
				1.5,
				-2.0,
				new IndexedEventLocation(
						IndexedLocationRole.ACTION,
						"Moscow",
						"RS",
						"RS48",
						"",
						"524901",
						new IndexedGeoPoint(55.7522, 37.6156)),
				Instant.parse("2026-07-30T10:15:00Z"),
				archiveKey,
				line);
	}

	private static IndexedMentionDocument mention(
			String rawMentionId,
			long eventId,
			long line,
			double tone
	) {
		return new IndexedMentionDocument(
				rawMentionId,
				"sd1-source-document",
				eventId,
				Instant.parse("2026-07-30T10:00:00Z"),
				Instant.parse("2026-07-30T10:05:00Z"),
				1,
				"example.org",
				"https://example.org/article",
				2,
				10,
				20,
				30,
				1,
				90,
				1000,
				tone,
				"",
				"",
				Instant.parse("2026-07-30T10:15:00Z"),
				MENTION_ARCHIVE,
				line);
	}

}
