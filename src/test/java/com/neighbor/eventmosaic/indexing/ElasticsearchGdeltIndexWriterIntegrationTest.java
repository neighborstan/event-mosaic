package com.neighbor.eventmosaic.indexing;

import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.EVENT;
import static com.neighbor.eventmosaic.indexing.api.GdeltIndexKind.MENTION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.EventIdentityConflictException;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
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
	private static final String EVENT_READ_ALIAS = "gdelt-events-read";
	private static final String STRICT_DYNAMIC_MAPPING_ERROR_TYPE =
			"strict_dynamic_mapping_exception";
	private static final String INDEX_NOT_FOUND_ERROR_TYPE =
			"index_not_found_exception";
	private static final String EVENT_ARCHIVE = "20260730101500.translation.export.CSV.zip";
	private static final String MENTION_ARCHIVE = "20260730101500.translation.mentions.CSV.zip";
	private static final String EVENT_PROCESSING_FINGERPRINT = "a".repeat(64);
	private static final String MENTION_PROCESSING_FINGERPRINT = "b".repeat(64);
	private static final int RECEIPT_PAGE_SIZE = 500;
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
				.value()).isFalse();
		assertThat(client.indices()
				.exists(request -> request.index(MENTION.indexName()))
				.value()).isFalse();

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
				new BulkIndexCommand<>(
						EVENT,
						target(EVENT_GENERATION),
						List.of(event(1001, 1, "Actor before"))),
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
				.containsExactly("gdelt-events-v1-p*-g*");
		assertThat(mentionTemplate.indexPatterns())
				.containsExactly("gdelt-mentions-v1-p*-g*");
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
		ExactIndexTarget missingTarget = new ExactIndexTarget(
				MISSING_EVENT_GENERATION,
				"missing-event-index-uuid");
		assertThat(client.indices()
				.exists(request -> request.index(MISSING_EVENT_GENERATION))
				.value()).isFalse();

		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer.write(new BulkIndexCommand<>(
						EVENT,
						missingTarget,
						List.of(event(1, 1, "Missing target")))))
				.satisfies(exception -> assertThat(exception.reason())
						.isEqualTo(IndexTargetUnavailableReason.MISSING));

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
	@DisplayName("Templates и exact writer сохраняют legacy fixed indices без изменений")
	void preservesLegacyFixedIndices() throws IOException {
		client.indices().create(request -> request.index(EVENT.indexName()));
		client.indices().create(request -> request.index(MENTION.indexName()));
		client.index(request -> request
				.index(EVENT.indexName())
				.id("legacy-event")
				.refresh(Refresh.WaitFor)
				.document(Map.of("legacyMarker", "event")));
		client.index(request -> request
				.index(MENTION.indexName())
				.id("legacy-mention")
				.refresh(Refresh.WaitFor)
				.document(Map.of("legacyMarker", "mention")));
		String legacyEventUuid = indexUuid(EVENT.indexName());
		String legacyMentionUuid = indexUuid(MENTION.indexName());

		writer.prepareReadModel();
		createPhysicalIndices();
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		writer.write(new BulkIndexCommand<>(
				EVENT,
				eventTarget,
				List.of(event(1501, 15, "Physical event"))));
		writer.refresh(EVENT, eventTarget);

		assertThat(indexUuid(EVENT.indexName())).isEqualTo(legacyEventUuid);
		assertThat(indexUuid(MENTION.indexName())).isEqualTo(legacyMentionUuid);
		assertThat(client.count(request -> request.index(EVENT.indexName())).count())
				.isEqualTo(1);
		assertThat(client.count(request -> request.index(MENTION.indexName())).count())
				.isEqualTo(1);
		assertThat(client.count(request -> request.index(EVENT_GENERATION)).count())
				.isEqualTo(1);
	}

	@Test
	@DisplayName("Exact Event provenance повторяется без перезаписи документа")
	void replaysExactEventProvenanceWithoutOverwrite() throws IOException {
		createPhysicalIndices();
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		IndexedEventDocument initialEvent = event(2001, 2, "Actor before");
		IndexedEventDocument replay = event(2001, 2, "Actor must not overwrite");

		assertThat(writer.write(new BulkIndexCommand<>(
				EVENT, eventTarget, List.of(initialEvent))).successful())
				.isTrue();
		assertThat(writer.write(new BulkIndexCommand<>(
				EVENT, eventTarget, List.of(replay))).successful())
				.isTrue();
		writer.refresh(EVENT, eventTarget);

		assertThat(client.count(request -> request.index(EVENT_GENERATION)).count()).isEqualTo(1);
		var storedEvent = client.get(
				request -> request.index(EVENT_GENERATION).id(initialEvent.documentId()),
				IndexedEventDocument.class);
		assertThat(storedEvent.found()).isTrue();
		assertThat(storedEvent.source())
				.isNotNull()
				.extracting(IndexedEventDocument::actor1Name)
				.isEqualTo("Actor before");
	}

	@Test
	@DisplayName("Другая Event provenance отклоняется без изменения документа")
	void rejectsDifferentEventProvenanceWithoutOverwrite() throws IOException {
		createPhysicalIndices();
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		IndexedEventDocument initialEvent = event(2002, 2, "Actor before");
		IndexedEventDocument collision = event(2002, 3, "Actor collision");
		writer.write(new BulkIndexCommand<>(
				EVENT,
				eventTarget,
				List.of(initialEvent)));

		assertThatExceptionOfType(EventIdentityConflictException.class)
				.isThrownBy(() -> writer.write(new BulkIndexCommand<>(
						EVENT,
						eventTarget,
						List.of(collision))))
				.satisfies(exception -> {
					assertThat(exception.retryable()).isFalse();
					assertThat(exception.sourceLineNumber()).isEqualTo(3);
				});
		writer.refresh(EVENT, eventTarget);

		assertThat(client.count(request -> request.index(EVENT_GENERATION)).count())
				.isEqualTo(1);
		var storedEvent = client.get(
				request -> request.index(EVENT_GENERATION).id(initialEvent.documentId()),
				IndexedEventDocument.class);
		assertThat(storedEvent.source())
				.isNotNull()
				.extracting(IndexedEventDocument::actor1Name)
				.isEqualTo("Actor before");
	}

	@Test
	@DisplayName("Стабильный Mention ID сохраняет overwrite без дубликатов")
	void overwritesMentionByStableId() throws IOException {
		createPhysicalIndices();
		ExactIndexTarget mentionTarget = target(MENTION_GENERATION);
		IndexedMentionDocument initialMention = mention("m1", 2001, 3, -1.5);
		IndexedMentionDocument updatedMention = mention("m1", 2001, 3, 2.5);

		assertThat(writer.write(new BulkIndexCommand<>(
				MENTION, mentionTarget, List.of(initialMention))).successful())
				.isTrue();
		assertThat(writer.write(new BulkIndexCommand<>(
				MENTION, mentionTarget, List.of(updatedMention))).successful())
				.isTrue();
		writer.refresh(MENTION, mentionTarget);

		assertThat(client.count(request -> request.index(MENTION_GENERATION)).count())
				.isEqualTo(1);
		var storedMention = client.get(
				request -> request.index(MENTION_GENERATION).id(initialMention.documentId()),
				IndexedMentionDocument.class);
		assertThat(storedMention.found()).isTrue();
		assertThat(storedMention.source())
				.isNotNull()
				.extracting(IndexedMentionDocument::mentionDocTone)
				.isEqualTo(2.5);
	}

	@Test
	@DisplayName("Receipt использует exact target, archive key и processing fingerprint")
	void verifiesExactReceiptAndDeletedIndex() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		addEventReadAlias(EVENT_GENERATION);
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		IndexedEventDocument exactArchive = event(3001, 4, "Exact archive");
		IndexedEventDocument similarArchive = event(
				3002,
				5,
				"Similar archive",
				EVENT_ARCHIVE + ".other");
		IndexedEventDocument otherFingerprint = event(
				3003,
				6,
				"Other fingerprint",
				EVENT_ARCHIVE,
				"c".repeat(64));
		writer.write(new BulkIndexCommand<>(
				EVENT,
				eventTarget,
				List.of(exactArchive, similarArchive, otherFingerprint)));
		writer.refresh(EVENT, eventTarget);

		var matched = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				1,
				digestOfIds(exactArchive.documentId()),
				RECEIPT_PAGE_SIZE));
		var shortage = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				2,
				digestOfIds(exactArchive.documentId(), "3999"),
				RECEIPT_PAGE_SIZE));
		var surplus = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				0,
				digestOfIds(),
				RECEIPT_PAGE_SIZE));
		var identityMismatch = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				1,
				digestOfIds("3999"),
				RECEIPT_PAGE_SIZE));

		assertThat(matched.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
		assertThat(matched.actualDocumentCount()).isEqualTo(1);
		assertThat(shortage.status()).isEqualTo(ArchiveReceiptStatus.SHORTAGE);
		assertThat(shortage.actualDocumentCount()).isEqualTo(1);
		assertThat(surplus.status()).isEqualTo(ArchiveReceiptStatus.SURPLUS);
		assertThat(surplus.actualDocumentCount()).isEqualTo(1);
		assertThat(identityMismatch.status())
				.isEqualTo(ArchiveReceiptStatus.IDENTITY_MISMATCH);
		assertThat(identityMismatch.actualDocumentCount()).isEqualTo(1);

		client.indices().delete(request -> request.index(EVENT_GENERATION));

		ArchiveReceiptQuery missingQuery = new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				1,
				digestOfIds(exactArchive.documentId()),
				RECEIPT_PAGE_SIZE);
		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer.verifyReceipt(missingQuery))
				.satisfies(exception -> assertThat(exception.reason())
						.isEqualTo(IndexTargetUnavailableReason.MISSING));
	}

	@Test
	@DisplayName("Равное количество документов не скрывает замену Event")
	void detectsReplacedIdentityWithEqualDocumentCount() throws IOException {
		createPhysicalIndices();
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		IndexedEventDocument expected = event(3051, 9, "Expected event");
		IndexedEventDocument replacement = event(3052, 9, "Replacement event");
		ArchiveIdentityDigest expectedDigest = digestOfIds(expected.documentId());

		writer.write(new BulkIndexCommand<>(EVENT, eventTarget, List.of(expected)));
		writer.refresh(EVENT, eventTarget);
		client.delete(request -> request
				.index(EVENT_GENERATION)
				.id(expected.documentId())
				.refresh(Refresh.WaitFor));
		client.index(request -> request
				.index(EVENT_GENERATION)
				.id(replacement.documentId())
				.refresh(Refresh.WaitFor)
				.document(replacement));

		var verification = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				1,
				expectedDigest,
				RECEIPT_PAGE_SIZE));

		assertThat(verification.status())
				.isEqualTo(ArchiveReceiptStatus.IDENTITY_MISMATCH);
		assertThat(verification.expectedDocumentCount()).isEqualTo(1);
		assertThat(verification.actualDocumentCount()).isEqualTo(1);
		assertThat(verification.expectedDigest()).isEqualTo(expectedDigest);
		assertThat(verification.actualDigest())
				.isEqualTo(digestOfIds(replacement.documentId()));
	}

	@Test
	@DisplayName("Receipt считает archive только в exact physical generation")
	void countsReceiptOnlyInExactTargetGeneration() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		client.indices().create(request -> request.index(MISSING_EVENT_GENERATION));
		addEventReadAlias(EVENT_GENERATION);
		ExactIndexTarget activeTarget = target(EVENT_GENERATION);
		ExactIndexTarget otherGeneration = target(MISSING_EVENT_GENERATION);
		IndexedEventDocument activeDocument = event(3101, 7, "Active generation");
		IndexedEventDocument otherDocument = event(3102, 8, "Other generation");
		writer.write(new BulkIndexCommand<>(EVENT, activeTarget, List.of(activeDocument)));
		writer.write(new BulkIndexCommand<>(EVENT, otherGeneration, List.of(otherDocument)));
		writer.refresh(EVENT, activeTarget);
		writer.refresh(EVENT, otherGeneration);

		var receipt = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				activeTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				1,
				digestOfIds(activeDocument.documentId()),
				RECEIPT_PAGE_SIZE));
		long bothGenerationsCount = client.count(request -> request
				.index(EVENT_GENERATION, MISSING_EVENT_GENERATION)
				.query(root -> root.bool(bool -> bool
						.filter(filter -> filter.term(term -> term
								.field("sourceArchiveKey")
								.value(EVENT_ARCHIVE)))
						.filter(filter -> filter.term(term -> term
								.field("processingFingerprint")
								.value(EVENT_PROCESSING_FINGERPRINT))))))
				.count();

		assertThat(bothGenerationsCount).isEqualTo(2);
		assertThat(receipt.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
		assertThat(receipt.actualDocumentCount()).isEqualTo(1);
	}

	@Test
	@DisplayName("Явный refresh делает видимыми все shards целевого индекса")
	void refreshesEveryTargetShardBeforeReceipt() throws IOException {
		client.indices().create(request -> request
				.index(EVENT_GENERATION)
				.settings(settings -> settings
						.numberOfShards("2")
						.refreshInterval(interval -> interval.time("-1"))));
		addEventReadAlias(EVENT_GENERATION);
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		List<IndexedEventDocument> documents = java.util.stream.LongStream
				.rangeClosed(1, 32)
				.mapToObj(value -> event(
						4_000 + value,
						10 + value,
						"Actor " + value))
				.toList();

		assertThat(writer.write(new BulkIndexCommand<>(
				EVENT,
				eventTarget,
				documents)).successful())
				.isTrue();
		assertThat(writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				documents.size(),
				digestOfIds(documents.stream()
						.map(IndexedEventDocument::documentId)
						.toArray(String[]::new)),
				1)).status())
				.isEqualTo(ArchiveReceiptStatus.SHORTAGE);

		writer.refresh(EVENT, eventTarget);

		var receipt = writer.verifyReceipt(new ArchiveReceiptQuery(
				EVENT,
				eventTarget,
				EVENT_ARCHIVE,
				EVENT_PROCESSING_FINGERPRINT,
				documents.size(),
				digestOfIds(documents.stream()
						.map(IndexedEventDocument::documentId)
						.toArray(String[]::new)),
				1));
		assertThat(receipt.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
		assertThat(receipt.actualDocumentCount()).isEqualTo(documents.size());
	}

	@Test
	@DisplayName("Writer отклоняет exact имя после замены Elasticsearch UUID")
	void rejectsRecreatedExactTarget() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		ExactIndexTarget staleTarget = target(EVENT_GENERATION);
		client.indices().delete(request -> request.index(EVENT_GENERATION));
		client.indices().create(request -> request.index(EVENT_GENERATION));
		ExactIndexTarget replacement = target(EVENT_GENERATION);

		assertThat(replacement.indexUuid()).isNotEqualTo(staleTarget.indexUuid());
		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer.refresh(EVENT, staleTarget))
				.satisfies(exception -> assertThat(exception.reason())
						.isEqualTo(IndexTargetUnavailableReason.REPLACED));
	}

	@Test
	@DisplayName("Writer отклоняет следующий bulk после exact write block")
	void rejectsWriteBlockedExactTarget() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		ExactIndexTarget eventTarget = target(EVENT_GENERATION);
		client.indices().putSettings(request -> request
				.index(EVENT_GENERATION)
				.settings(settings -> settings.blocks(blocks -> blocks.write(true))));

		assertThatExceptionOfType(IndexTargetUnavailableException.class)
				.isThrownBy(() -> writer.write(new BulkIndexCommand<>(
						EVENT,
						eventTarget,
						List.of(event(5001, 50, "Blocked target")))))
				.satisfies(exception -> assertThat(exception.reason())
						.isEqualTo(IndexTargetUnavailableReason.WRITE_BLOCKED));
		assertThat(client.count(request -> request.index(EVENT_GENERATION)).count())
				.isZero();
	}

	private void createPhysicalIndices() throws IOException {
		client.indices().create(request -> request.index(EVENT_GENERATION));
		client.indices().create(request -> request.index(MENTION_GENERATION));
		addEventReadAlias(EVENT_GENERATION);
	}

	private void addEventReadAlias(String indexName) throws IOException {
		client.indices().updateAliases(request -> request.actions(action -> action.add(add -> add
				.index(indexName)
				.alias(EVENT_READ_ALIAS))));
	}

	private ExactIndexTarget target(String indexName) throws IOException {
		return new ExactIndexTarget(indexName, indexUuid(indexName));
	}

	private String indexUuid(String indexName) throws IOException {
		var settings = client.indices()
				.get(request -> request.index(indexName))
				.get(indexName)
				.settings();
		var indexSettings = settings.index() == null ? settings : settings.index();
		return indexSettings.uuid();
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

	private static ArchiveIdentityDigest digestOfIds(String... orderedIds) {
		ArchiveIdentityDigest.Accumulator accumulator = ArchiveIdentityDigest.accumulator();
		for (String identity : orderedIds) {
			accumulator.addIdentity(identity);
		}
		return accumulator.finish();
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
		return event(
				id,
				line,
				actor1Name,
				archiveKey,
				EVENT_PROCESSING_FINGERPRINT);
	}

	private static IndexedEventDocument event(
			long id,
			long line,
			String actor1Name,
			String archiveKey,
			String processingFingerprint
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
				processingFingerprint);
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
