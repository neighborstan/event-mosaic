package com.neighbor.eventmosaic.indexing;

import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT;
import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.MENTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
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
import java.util.Set;
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
	private static final String EVENT_GENERATION =
			"gdelt-events-v1-p20260727-g0001";
	private static final String MENTION_GENERATION =
			"gdelt-mentions-v1-p20260727-g0001";
	private static final String MISSING_EVENT_GENERATION =
			"gdelt-events-v1-p20260727-g0002";
	private static final String STRICT_DYNAMIC_MAPPING_ERROR_TYPE =
			"strict_dynamic_mapping_exception";
	private static final String INDEX_NOT_FOUND_ERROR_TYPE =
			"index_not_found_exception";
	private static final String EVENT_ARCHIVE = "20260730101500.translation.export.CSV.zip";
	private static final String MENTION_ARCHIVE = "20260730101500.translation.mentions.CSV.zip";
	private static final String EVENT_PROCESSING_FINGERPRINT = "a".repeat(64);
	private static final String MENTION_PROCESSING_FINGERPRINT = "b".repeat(64);
	private static final Set<String> EVENT_KEYWORD_FIELDS = Set.of(
			"actor1Code",
			"actor1Name",
			"actor1CountryCode",
			"actor1KnownGroupCode",
			"actor1EthnicCode",
			"actor1Religion1Code",
			"actor1Religion2Code",
			"actor1Type1Code",
			"actor1Type2Code",
			"actor1Type3Code",
			"actor2Code",
			"actor2Name",
			"actor2CountryCode",
			"actor2KnownGroupCode",
			"actor2EthnicCode",
			"actor2Religion1Code",
			"actor2Religion2Code",
			"actor2Type1Code",
			"actor2Type2Code",
			"actor2Type3Code",
			"eventCode",
			"eventBaseCode",
			"eventRootCode",
			"sourceUrl",
			"sourceArchiveKey",
			"processingFingerprint");
	private static final Set<String> EVENT_INTEGER_FIELDS = Set.of(
			"isRootEvent",
			"quadClass",
			"numMentions",
			"numSources",
			"numArticles");
	private static final Set<String> EVENT_DATE_FIELDS = Set.of(
			"eventDay",
			"dateAdded",
			"sourceUpdateTime");
	private static final Set<String> EVENT_LONG_FIELDS = Set.of(
			"globalEventId",
			"sourceLineNumber");
	private static final Set<String> EVENT_DOUBLE_FIELDS = Set.of(
			"goldsteinScale",
			"averageTone");
	private static final Set<String> MENTION_KEYWORD_FIELDS = Set.of(
			"rawMentionId",
			"sourceDocumentKey",
			"mentionSourceName",
			"mentionIdentifier",
			"mentionDocTranslationInfo",
			"extras",
			"sourceArchiveKey",
			"processingFingerprint");
	private static final Set<String> MENTION_INTEGER_FIELDS = Set.of(
			"mentionType",
			"sentenceId",
			"actor1CharOffset",
			"actor2CharOffset",
			"actionCharOffset",
			"inRawText",
			"confidence",
			"mentionDocLen");
	private static final Set<String> MENTION_DATE_FIELDS = Set.of(
			"eventTimeDate",
			"mentionTimeDate",
			"sourceUpdateTime");
	private static final Set<String> MENTION_LONG_FIELDS = Set.of(
			"globalEventId",
			"sourceLineNumber");
	private static final Set<String> LOCATION_KEYWORD_FIELDS = Set.of(
			"role",
			"name",
			"countryCode",
			"admin1Code",
			"admin2Code",
			"featureId");

	@Autowired
	private GdeltIndexWriter writer;

	@Autowired
	private ElasticsearchClient client;

	@BeforeEach
	void resetIndicesAndPrepareTemplates() throws IOException {
		client.indices().delete(request -> request
				.index(
						EVENT.indexName(),
						MENTION.indexName(),
						EVENT_GENERATION,
						MENTION_GENERATION,
						MISSING_EVENT_GENERATION)
				.ignoreUnavailable(true)
				.allowNoIndices(true));
		writer.prepareReadModel();
	}

	@Test
	@DisplayName("Strict generation mappings отклоняют неизвестные root и nested поля")
	void rejectsUnknownRootAndNestedFields() throws IOException {
		writer.prepareReadModel();
		createPhysicalIndices();

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

		BulkResponse eventFailure = indexUnknownField(EVENT_GENERATION, "unknown-event");
		BulkResponse mentionFailure = indexUnknownField(
				MENTION_GENERATION,
				"unknown-mention");
		BulkResponse nestedFailure = indexDocument(
				EVENT_GENERATION,
				"unknown-location",
				Map.of("location", Map.of("unexpectedField", "must be rejected")));

		assertStrictMappingFailure(eventFailure);
		assertStrictMappingFailure(mentionFailure);
		assertStrictMappingFailure(nestedFailure);

		var classified = BulkResponseAnalyzer.analyze(
				new BulkIndexCommand<>(EVENT, List.of(event(1001, 1, "Actor before"))),
				eventFailure.items());
		assertThat(classified.outcome())
				.isEqualTo(BulkIndexOutcome.NON_RETRYABLE_PARTIAL_FAILURE);
	}

	@Test
	@DisplayName("Templates задают точную Event и Mention field matrix")
	void installsExactGenerationFieldMatrix() throws IOException {
		createPhysicalIndices();

		var eventTemplate = client.indices()
				.getIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.indexTemplates()
				.getFirst()
				.indexTemplate();
		var mentionTemplate = client.indices()
				.getIndexTemplate(request -> request.name(MENTION_TEMPLATE))
				.indexTemplates()
				.getFirst()
				.indexTemplate();

		assertThat(eventTemplate.indexPatterns())
				.containsExactly(EVENT.indexName(), "gdelt-events-v1-*-g*");
		assertThat(mentionTemplate.indexPatterns())
				.containsExactly(MENTION.indexName(), "gdelt-mentions-v1-*-g*");
		assertThat(eventTemplate.allowAutoCreate()).isFalse();
		assertThat(mentionTemplate.allowAutoCreate()).isFalse();

		TypeMapping eventMapping = mapping(EVENT_GENERATION);
		TypeMapping mentionMapping = mapping(MENTION_GENERATION);
		assertThat(eventMapping.dynamic()).isEqualTo(DynamicMapping.Strict);
		assertThat(mentionMapping.dynamic()).isEqualTo(DynamicMapping.Strict);

		Map<String, Property> eventFields = eventMapping.properties();
		assertThat(eventFields).hasSize(39);
		assertFieldKinds(eventFields, EVENT_KEYWORD_FIELDS, Property.Kind.Keyword);
		assertFieldKinds(eventFields, EVENT_INTEGER_FIELDS, Property.Kind.Integer);
		assertFieldKinds(eventFields, EVENT_DATE_FIELDS, Property.Kind.Date);
		assertFieldKinds(eventFields, EVENT_LONG_FIELDS, Property.Kind.Long);
		assertFieldKinds(eventFields, EVENT_DOUBLE_FIELDS, Property.Kind.Double);
		assertThat(eventFields.get("location")._kind()).isEqualTo(Property.Kind.Object);
		Map<String, Property> locationFields = eventFields.get("location")
				.object()
				.properties();
		assertThat(eventFields.get("location").object().dynamic())
				.isEqualTo(DynamicMapping.Strict);
		assertThat(locationFields).hasSize(8);
		assertFieldKinds(locationFields, LOCATION_KEYWORD_FIELDS, Property.Kind.Keyword);
		assertFieldKinds(locationFields, Set.of("geoType"), Property.Kind.Integer);
		assertFieldKinds(locationFields, Set.of("point"), Property.Kind.GeoPoint);

		Map<String, Property> mentionFields = mentionMapping.properties();
		assertThat(mentionFields).hasSize(22);
		assertFieldKinds(mentionFields, MENTION_KEYWORD_FIELDS, Property.Kind.Keyword);
		assertFieldKinds(mentionFields, MENTION_INTEGER_FIELDS, Property.Kind.Integer);
		assertFieldKinds(mentionFields, MENTION_DATE_FIELDS, Property.Kind.Date);
		assertFieldKinds(mentionFields, MENTION_LONG_FIELDS, Property.Kind.Long);
		assertFieldKinds(
				mentionFields,
				Set.of("mentionDocTone"),
				Property.Kind.Double);

		assertReceiptAndSourceFieldOptions(eventFields, mentionFields);
	}

	@Test
	@DisplayName("Полные Event и Mention сериализуются и читаются из physical generation")
	void roundTripsCompleteDocumentsThroughPhysicalGeneration() throws IOException {
		createPhysicalIndices();
		IndexedEventDocument event = event(1_001, 1, "Actor round-trip");
		IndexedMentionDocument mention = mention("rm1-round-trip", 1_001, 2, -1.5);

		client.index(request -> request
				.index(EVENT_GENERATION)
				.id(event.documentId())
				.refresh(Refresh.WaitFor)
				.document(event));
		client.index(request -> request
				.index(MENTION_GENERATION)
				.id(mention.documentId())
				.refresh(Refresh.WaitFor)
				.document(mention));

		var storedEvent = client.get(
				request -> request.index(EVENT_GENERATION).id(event.documentId()),
				IndexedEventDocument.class);
		var storedMention = client.get(
				request -> request.index(MENTION_GENERATION).id(mention.documentId()),
				IndexedMentionDocument.class);
		assertThat(storedEvent.found()).isTrue();
		assertThat(storedEvent.source()).isEqualTo(event);
		assertThat(storedMention.found()).isTrue();
		assertThat(storedMention.source()).isEqualTo(mention);
	}

	@Test
	@DisplayName("Missing physical generation не создается автоматически")
	void rejectsAutoCreateForMissingPhysicalGeneration() throws IOException {
		assertThat(client.indices()
				.exists(request -> request.index(MISSING_EVENT_GENERATION))
				.value()).isFalse();

		assertThatThrownBy(() -> client.index(request -> request
				.index(MISSING_EVENT_GENERATION)
				.id("missing-generation")
				.document(Map.of("globalEventId", 1))))
				.isInstanceOf(ElasticsearchException.class)
				.satisfies(throwable -> {
					ElasticsearchException exception = (ElasticsearchException) throwable;
					assertThat(exception.status()).isEqualTo(404);
					assertThat(exception.error().type())
							.isEqualTo(INDEX_NOT_FOUND_ERROR_TYPE);
				});
		assertThat(client.indices()
				.exists(request -> request.index(MISSING_EVENT_GENERATION))
				.value()).isFalse();
	}

	@Test
	@DisplayName("Восстанавливает strict template до создания physical generation")
	void restoresStrictTemplateBeforeCreatingPhysicalGeneration() throws IOException {
		client.indices().deleteIndexTemplate(request -> request.name(EVENT_TEMPLATE));

		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.value()).isFalse();
		assertThat(client.indices()
				.exists(request -> request.index(EVENT_GENERATION))
				.value()).isFalse();

		writer.prepareReadModel();

		assertThat(client.indices()
				.existsIndexTemplate(request -> request.name(EVENT_TEMPLATE))
				.value()).isTrue();
		client.indices().create(request -> request.index(EVENT_GENERATION));
		assertThat(client.indices()
				.exists(request -> request.index(EVENT_GENERATION))
				.value()).isTrue();
		BulkResponse strictMappingFailure = indexUnknownField(
				EVENT_GENERATION,
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

	private void createPhysicalIndices() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		client.indices().create(request -> request.index(MENTION_GENERATION));
	}

	private TypeMapping mapping(String indexName) throws IOException {
		return client.indices()
				.getMapping(request -> request.index(indexName))
				.get(indexName)
				.mappings();
	}

	private BulkResponse indexUnknownField(String indexName, String id) throws IOException {
		return indexDocument(
				indexName,
				id,
				Map.of("unexpectedField", "must be rejected"));
	}

	private BulkResponse indexDocument(
			String indexName,
			String id,
			Map<String, ?> document
	) throws IOException {
		return client.bulk(request -> request
				.refresh(Refresh.WaitFor)
				.operations(operation -> operation.index(index -> index
						.index(indexName)
						.id(id)
						.document(document))));
	}

	private static void assertStrictMappingFailure(BulkResponse response) {
		assertThat(response.items()).singleElement().satisfies(item -> {
			assertThat(item.status()).isEqualTo(400);
			assertThat(item.error()).isNotNull();
			assertThat(item.error().type()).isEqualTo(STRICT_DYNAMIC_MAPPING_ERROR_TYPE);
		});
	}

	private static void assertFieldKinds(
			Map<String, Property> fields,
			Set<String> expectedNames,
			Property.Kind expectedKind
	) {
		assertThat(fields).containsKeys(expectedNames.toArray(String[]::new));
		for (String fieldName : expectedNames) {
			assertThat(fields.get(fieldName)._kind())
					.as("mapping type of %s", fieldName)
					.isEqualTo(expectedKind);
		}
	}

	private static void assertReceiptAndSourceFieldOptions(
			Map<String, Property> eventFields,
			Map<String, Property> mentionFields
	) {
		assertDocValuesEnabled(eventFields.get("globalEventId").long_().docValues());
		assertDocValuesEnabled(eventFields.get("sourceArchiveKey").keyword().docValues());
		assertDocValuesEnabled(eventFields.get("sourceLineNumber").long_().docValues());
		assertDocValuesEnabled(
				eventFields.get("processingFingerprint").keyword().docValues());
		assertDocValuesEnabled(mentionFields.get("rawMentionId").keyword().docValues());
		assertDocValuesEnabled(
				mentionFields.get("sourceArchiveKey").keyword().docValues());
		assertDocValuesEnabled(mentionFields.get("sourceLineNumber").long_().docValues());
		assertDocValuesEnabled(
				mentionFields.get("processingFingerprint").keyword().docValues());
		assertThat(eventFields.get("sourceUrl").keyword().index()).isFalse();
		assertThat(eventFields.get("sourceUrl").keyword().docValues()).isFalse();
	}

	private static void assertDocValuesEnabled(Boolean docValues) {
		// Elasticsearch не возвращает значение true по умолчанию в installed mapping.
		assertThat(docValues).isNotEqualTo(Boolean.FALSE);
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
				"ACT1",
				actor1Name,
				"US",
				"GROUP1",
				"ETH1",
				"REL1",
				"REL2",
				"TYPE1",
				"TYPE2",
				"TYPE3",
				"ACT2",
				"Actor 2",
				"RS",
				"GROUP2",
				"ETH2",
				"REL3",
				"REL4",
				"TYPE4",
				"TYPE5",
				"TYPE6",
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
				new IndexedEventLocation(
						IndexedLocationRole.ACTION,
						1,
						"Moscow",
						"RS",
						"RS48",
						"",
						"524901",
						new IndexedGeoPoint(55.7522, 37.6156)),
				"https://example.test/events/" + id,
				Instant.parse("2026-07-30T10:15:00Z"),
				archiveKey,
				line,
				EVENT_PROCESSING_FINGERPRINT);
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
				line,
				MENTION_PROCESSING_FINGERPRINT);
	}

}
