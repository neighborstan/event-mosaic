package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import java.time.Instant;
import java.time.LocalDate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Преобразование GDELT Event в индексируемый документ")
class GdeltEventDocumentMapperTest {

	private static final LocalDate EVENT_DAY = LocalDate.of(2026, 7, 21);
	private static final Instant DATE_ADDED = Instant.parse("2026-07-21T14:46:00Z");

	private final GdeltEventDocumentMapper mapper = new GdeltEventDocumentMapper();

	@Test
	@DisplayName("Преобразует все целевые Event fields с source provenance")
	void mapsEventDocumentAndProvenance() {
		GdeltEvent event = eventWithGeography(
				ProcessingTestFixtures.geo("Actor 1 place", 10.0, 20.0),
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.absentGeo());

		DocumentMappingResult<IndexedEventDocument> result = mapper.map(
				new GdeltCsvRecord<>(17, event),
				ProcessingTestFixtures.eventRequest());

		assertThat(result.accepted()).isTrue();
		assertThat(result.invalidGeoCandidates()).isZero();
		assertThat(result.document()).satisfies(document -> {
			assertThat(document.documentId()).isEqualTo("700000001");
			assertThat(document.eventDay()).isEqualTo(EVENT_DAY);
			assertThat(document.dateAdded()).isEqualTo(DATE_ADDED);
			assertThat(document.actor1Code()).isEqualTo("A1-CODE");
			assertThat(document.actor1Name()).isEqualTo("Actor 1");
			assertThat(document.actor1CountryCode()).isEqualTo("A1-COUNTRY");
			assertThat(document.actor1KnownGroupCode()).isEqualTo("A1-GROUP");
			assertThat(document.actor1EthnicCode()).isEqualTo("A1-ETHNIC");
			assertThat(document.actor1Religion1Code()).isEqualTo("A1-RELIGION-1");
			assertThat(document.actor1Religion2Code()).isEqualTo("A1-RELIGION-2");
			assertThat(document.actor1Type1Code()).isEqualTo("A1-TYPE-1");
			assertThat(document.actor1Type2Code()).isEqualTo("A1-TYPE-2");
			assertThat(document.actor1Type3Code()).isEqualTo("A1-TYPE-3");
			assertThat(document.actor2Code()).isEqualTo("A2-CODE");
			assertThat(document.actor2Name()).isEqualTo("Actor 2");
			assertThat(document.actor2CountryCode()).isEqualTo("A2-COUNTRY");
			assertThat(document.actor2KnownGroupCode()).isEqualTo("A2-GROUP");
			assertThat(document.actor2EthnicCode()).isEqualTo("A2-ETHNIC");
			assertThat(document.actor2Religion1Code()).isEqualTo("A2-RELIGION-1");
			assertThat(document.actor2Religion2Code()).isEqualTo("A2-RELIGION-2");
			assertThat(document.actor2Type1Code()).isEqualTo("A2-TYPE-1");
			assertThat(document.actor2Type2Code()).isEqualTo("A2-TYPE-2");
			assertThat(document.actor2Type3Code()).isEqualTo("A2-TYPE-3");
			assertThat(document.isRootEvent()).isEqualTo(1);
			assertThat(document.eventCode()).isEqualTo("042");
			assertThat(document.eventBaseCode()).isEqualTo("042");
			assertThat(document.eventRootCode()).isEqualTo("04");
			assertThat(document.quadClass()).isEqualTo(1);
			assertThat(document.goldsteinScale()).isEqualTo(-2.5);
			assertThat(document.averageTone()).isEqualTo(-1.75);
			assertThat(document.numMentions()).isEqualTo(3);
			assertThat(document.numSources()).isEqualTo(2);
			assertThat(document.numArticles()).isEqualTo(2);
			assertThat(document.location().role()).isEqualTo(IndexedLocationRole.ACTOR1);
			assertThat(document.location().geoType()).isEqualTo(1);
			assertThat(document.location().name()).isEqualTo("Actor 1 place");
			assertThat(document.location().countryCode()).isEqualTo("US");
			assertThat(document.location().admin1Code()).isEqualTo("USCA");
			assertThat(document.location().admin2Code()).isEmpty();
			assertThat(document.location().featureId()).isEqualTo("feature");
			assertThat(document.location().point().lat()).isEqualTo(10.0);
			assertThat(document.location().point().lon()).isEqualTo(20.0);
			assertThat(document.sourceUrl()).isEqualTo("https://source.example/event");
			assertThat(document.sourceUpdateTime())
					.isEqualTo(ProcessingTestFixtures.SOURCE_UPDATE_TIME);
			assertThat(document.sourceArchiveKey())
					.isEqualTo(ProcessingTestFixtures.eventRequest().sourceArchiveKey());
			assertThat(document.sourceLineNumber()).isEqualTo(17);
			assertThat(document.processingFingerprint())
					.isEqualTo(ProcessingTestFixtures.eventRequest().processingFingerprint());
		});
	}

	@Test
	@DisplayName("Сохраняет пустые provider strings и nullable Event numbers")
	void preservesEmptyStringsAndNullableNumbers() {
		GdeltEvent event = ProcessingTestFixtures.eventWithNullableNumericsAndEmptyStrings(
				700_000_001L,
				EVENT_DAY,
				DATE_ADDED);

		DocumentMappingResult<IndexedEventDocument> result = map(event);

		assertThat(result.accepted()).isTrue();
		assertThat(result.document()).satisfies(document -> {
			assertThat(document.actor1Code()).isEmpty();
			assertThat(document.actor1Name()).isEmpty();
			assertThat(document.actor1CountryCode()).isEmpty();
			assertThat(document.actor1KnownGroupCode()).isEmpty();
			assertThat(document.actor1EthnicCode()).isEmpty();
			assertThat(document.actor1Religion1Code()).isEmpty();
			assertThat(document.actor1Religion2Code()).isEmpty();
			assertThat(document.actor1Type1Code()).isEmpty();
			assertThat(document.actor1Type2Code()).isEmpty();
			assertThat(document.actor1Type3Code()).isEmpty();
			assertThat(document.actor2Code()).isEmpty();
			assertThat(document.actor2Name()).isEmpty();
			assertThat(document.actor2CountryCode()).isEmpty();
			assertThat(document.actor2KnownGroupCode()).isEmpty();
			assertThat(document.actor2EthnicCode()).isEmpty();
			assertThat(document.actor2Religion1Code()).isEmpty();
			assertThat(document.actor2Religion2Code()).isEmpty();
			assertThat(document.actor2Type1Code()).isEmpty();
			assertThat(document.actor2Type2Code()).isEmpty();
			assertThat(document.actor2Type3Code()).isEmpty();
			assertThat(document.eventCode()).isEmpty();
			assertThat(document.eventBaseCode()).isEmpty();
			assertThat(document.eventRootCode()).isEmpty();
			assertThat(document.sourceUrl()).isEmpty();
			assertThat(document.isRootEvent()).isNull();
			assertThat(document.quadClass()).isNull();
			assertThat(document.goldsteinScale()).isNull();
			assertThat(document.averageTone()).isNull();
			assertThat(document.numMentions()).isNull();
			assertThat(document.numSources()).isNull();
			assertThat(document.numArticles()).isNull();
			assertThat(document.location()).isNull();
		});
	}

	@Test
	@DisplayName("Выбирает Action location раньше Actor locations")
	void prefersActionLocation() {
		GdeltEvent event = eventWithGeography(
				ProcessingTestFixtures.geo("Actor 1 place", 10.0, 20.0),
				ProcessingTestFixtures.geo("Actor 2 place", 30.0, 40.0),
				ProcessingTestFixtures.geo("Action place", 50.0, 60.0));

		DocumentMappingResult<IndexedEventDocument> result = mapper.map(
				new GdeltCsvRecord<>(1, event),
				ProcessingTestFixtures.eventRequest());

		assertThat(result.document().location().role()).isEqualTo(IndexedLocationRole.ACTION);
		assertThat(result.document().location().geoType()).isEqualTo(1);
		assertThat(result.document().location().name()).isEqualTo("Action place");
		assertThat(result.document().location().point().lat()).isEqualTo(50.0);
		assertThat(result.document().location().point().lon()).isEqualTo(60.0);
	}

	@Test
	@DisplayName("Сохраняет nullable geo type у допустимой location")
	void preservesNullableGeoType() {
		GdeltEvent event = eventWithGeography(
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.geo(null, "Action place", 50.0, 60.0));

		DocumentMappingResult<IndexedEventDocument> result = map(event);

		assertThat(result.accepted()).isTrue();
		assertThat(result.document().location().role()).isEqualTo(IndexedLocationRole.ACTION);
		assertThat(result.document().location().geoType()).isNull();
	}

	@Test
	@DisplayName("Пропускает invalid Action location и выбирает Actor1")
	void fallsBackAfterInvalidActionLocation() {
		GdeltEvent event = eventWithGeography(
				ProcessingTestFixtures.geo("Actor 1 place", -10.0, -20.0),
				ProcessingTestFixtures.geo("Actor 2 place", 30.0, 40.0),
				ProcessingTestFixtures.geo("Invalid action", 91.0, 60.0));

		DocumentMappingResult<IndexedEventDocument> result = mapper.map(
				new GdeltCsvRecord<>(1, event),
				ProcessingTestFixtures.eventRequest());

		assertThat(result.invalidGeoCandidates()).isEqualTo(1);
		assertThat(result.document().location().role()).isEqualTo(IndexedLocationRole.ACTOR1);
		assertThat(result.document().location().name()).isEqualTo("Actor 1 place");
	}

	@Test
	@DisplayName("Сохраняет Event без location при отсутствии допустимых пар")
	void acceptsEventWithoutValidLocation() {
		GdeltEvent event = eventWithGeography(
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.partialGeo("Partial action", 10.0, null));

		DocumentMappingResult<IndexedEventDocument> result = mapper.map(
				new GdeltCsvRecord<>(1, event),
				ProcessingTestFixtures.eventRequest());

		assertThat(result.accepted()).isTrue();
		assertThat(result.invalidGeoCandidates()).isEqualTo(1);
		assertThat(result.document().location()).isNull();
	}

	@Test
	@DisplayName("Отклоняет Event без обязательных processing fields")
	void rejectsMissingRequiredFields() {
		DocumentMappingResult<IndexedEventDocument> invalidId = map(
				ProcessingTestFixtures.event(
						0,
						EVENT_DAY,
						DATE_ADDED,
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo()));
		DocumentMappingResult<IndexedEventDocument> missingDay = map(
				ProcessingTestFixtures.event(
						700_000_001L,
						null,
						DATE_ADDED,
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo()));
		DocumentMappingResult<IndexedEventDocument> missingDateAdded = map(
				ProcessingTestFixtures.event(
						700_000_001L,
						EVENT_DAY,
						null,
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo(),
						ProcessingTestFixtures.absentGeo()));

		assertThat(invalidId.rejection())
				.isEqualTo(ProcessingMappingRejection.EVENT_ID_INVALID);
		assertThat(missingDay.rejection())
				.isEqualTo(ProcessingMappingRejection.EVENT_DAY_MISSING);
		assertThat(missingDateAdded.rejection())
				.isEqualTo(ProcessingMappingRejection.EVENT_DATE_ADDED_MISSING);
	}

	private DocumentMappingResult<IndexedEventDocument> map(GdeltEvent event) {
		return mapper.map(
				new GdeltCsvRecord<>(1, event),
				ProcessingTestFixtures.eventRequest());
	}

	private static GdeltEvent eventWithGeography(
			ProcessingTestFixtures.Geo actor1,
			ProcessingTestFixtures.Geo actor2,
			ProcessingTestFixtures.Geo action
	) {
		return ProcessingTestFixtures.event(
				700_000_001L,
				EVENT_DAY,
				DATE_ADDED,
				actor1,
				actor2,
				action);
	}
}
