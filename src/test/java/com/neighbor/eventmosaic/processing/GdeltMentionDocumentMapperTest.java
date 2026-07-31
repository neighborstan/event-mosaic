package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Преобразование GDELT Mention в индексируемый документ")
class GdeltMentionDocumentMapperTest {

	private static final Instant EVENT_TIME = Instant.parse("2026-07-21T14:30:00Z");
	private static final Instant MENTION_TIME = Instant.parse("2026-07-21T14:44:00Z");

	private final GdeltMentionDocumentMapper mapper = new GdeltMentionDocumentMapper();

	@Test
	@DisplayName("Сохраняет все 16 Mention values, identities и source provenance")
	void mapsEveryMentionFieldAndProvenance() {
		GdeltMention mention = ProcessingTestFixtures.mention(
				700_000_001L,
				EVENT_TIME,
				MENTION_TIME,
				1,
				"  citation value  ");

		DocumentMappingResult<IndexedMentionDocument> result = mapper.map(
				new GdeltCsvRecord<>(23, mention),
				ProcessingTestFixtures.mentionRequest());

		assertThat(result.accepted()).isTrue();
		assertThat(result.document()).satisfies(document -> {
			assertThat(document.rawMentionId()).startsWith("rm1-").hasSize(68);
			assertThat(document.sourceDocumentKey()).startsWith("sd1-").hasSize(68);
			assertThat(document.globalEventId()).isEqualTo(700_000_001L);
			assertThat(document.eventTimeDate()).isEqualTo(EVENT_TIME);
			assertThat(document.mentionTimeDate()).isEqualTo(MENTION_TIME);
			assertThat(document.mentionType()).isEqualTo(1);
			assertThat(document.mentionSourceName()).isEqualTo("source-name");
			assertThat(document.mentionIdentifier()).isEqualTo("  citation value  ");
			assertThat(document.sentenceId()).isEqualTo(7);
			assertThat(document.actor1CharOffset()).isEqualTo(11);
			assertThat(document.actor2CharOffset()).isEqualTo(12);
			assertThat(document.actionCharOffset()).isEqualTo(13);
			assertThat(document.inRawText()).isEqualTo(1);
			assertThat(document.confidence()).isEqualTo(85);
			assertThat(document.mentionDocLen()).isEqualTo(1_024);
			assertThat(document.mentionDocTone()).isEqualTo(-1.25);
			assertThat(document.mentionDocTranslationInfo()).isEqualTo("translation-info");
			assertThat(document.extras()).isEqualTo("extras");
			assertThat(document.sourceUpdateTime())
					.isEqualTo(ProcessingTestFixtures.SOURCE_UPDATE_TIME);
			assertThat(document.sourceArchiveKey())
					.isEqualTo(ProcessingTestFixtures.SOURCE_ARCHIVE_KEY);
			assertThat(document.sourceLineNumber()).isEqualTo(23);
			assertThat(document.processingFingerprint())
					.isEqualTo(ProcessingTestFixtures.mentionRequest().processingFingerprint());
		});
	}

	@Test
	@DisplayName("Сохраняет пустые Mention strings и nullable optional numbers")
	void preservesEmptyStringsAndNullableNumbers() {
		GdeltMention mention = ProcessingTestFixtures
				.mentionWithNullableNumericsAndEmptyStrings(
						700_000_001L,
						EVENT_TIME,
						MENTION_TIME,
						1,
						"identifier");

		DocumentMappingResult<IndexedMentionDocument> result = map(mention);

		assertThat(result.accepted()).isTrue();
		assertThat(result.document()).satisfies(document -> {
			assertThat(document.mentionSourceName()).isEmpty();
			assertThat(document.sentenceId()).isNull();
			assertThat(document.actor1CharOffset()).isNull();
			assertThat(document.actor2CharOffset()).isNull();
			assertThat(document.actionCharOffset()).isNull();
			assertThat(document.inRawText()).isNull();
			assertThat(document.confidence()).isNull();
			assertThat(document.mentionDocLen()).isNull();
			assertThat(document.mentionDocTone()).isNull();
			assertThat(document.mentionDocTranslationInfo()).isEmpty();
			assertThat(document.extras()).isEmpty();
			assertThat(document.processingFingerprint())
					.isEqualTo(ProcessingTestFixtures.mentionRequest().processingFingerprint());
		});
	}

	@Test
	@DisplayName("Различает raw observation и общий source document")
	void separatesObservationAndSourceDocumentIdentities() {
		GdeltMention mention = validMention();

		IndexedMentionDocument first = mapper.map(
				new GdeltCsvRecord<>(10, mention),
				ProcessingTestFixtures.mentionRequest()).document();
		IndexedMentionDocument second = mapper.map(
				new GdeltCsvRecord<>(11, mention),
				ProcessingTestFixtures.mentionRequest()).document();

		assertThat(first.rawMentionId()).isNotEqualTo(second.rawMentionId());
		assertThat(first.sourceDocumentKey()).isEqualTo(second.sourceDocumentKey());
	}

	@Test
	@DisplayName("Отклоняет Mention без обязательных processing fields")
	void rejectsMissingRequiredFields() {
		DocumentMappingResult<IndexedMentionDocument> invalidId = map(
				ProcessingTestFixtures.mention(0, EVENT_TIME, MENTION_TIME, 1, "id"));
		DocumentMappingResult<IndexedMentionDocument> missingEventTime = map(
				ProcessingTestFixtures.mention(
						700_000_001L,
						null,
						MENTION_TIME,
						1,
						"id"));
		DocumentMappingResult<IndexedMentionDocument> missingMentionTime = map(
				ProcessingTestFixtures.mention(
						700_000_001L,
						EVENT_TIME,
						null,
						1,
						"id"));
		DocumentMappingResult<IndexedMentionDocument> missingMentionType = map(
				ProcessingTestFixtures.mention(
						700_000_001L,
						EVENT_TIME,
						MENTION_TIME,
						null,
						"id"));

		assertThat(invalidId.rejection())
				.isEqualTo(ProcessingMappingRejection.MENTION_ID_INVALID);
		assertThat(missingEventTime.rejection())
				.isEqualTo(ProcessingMappingRejection.MENTION_EVENT_TIME_MISSING);
		assertThat(missingMentionTime.rejection())
				.isEqualTo(ProcessingMappingRejection.MENTION_TIME_MISSING);
		assertThat(missingMentionType.rejection())
				.isEqualTo(ProcessingMappingRejection.MENTION_TYPE_MISSING);
	}

	private DocumentMappingResult<IndexedMentionDocument> map(GdeltMention mention) {
		return mapper.map(
				new GdeltCsvRecord<>(1, mention),
				ProcessingTestFixtures.mentionRequest());
	}

	private static GdeltMention validMention() {
		return ProcessingTestFixtures.mention(
				700_000_001L,
				EVENT_TIME,
				MENTION_TIME,
				1,
				"identifier");
	}
}
