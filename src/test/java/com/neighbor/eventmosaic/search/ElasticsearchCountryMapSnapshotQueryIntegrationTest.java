package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import com.neighbor.eventmosaic.FixedClockTestConfiguration;
import com.neighbor.eventmosaic.TestcontainersConfiguration;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventLocation;
import com.neighbor.eventmosaic.indexing.api.IndexedGeoPoint;
import com.neighbor.eventmosaic.indexing.api.IndexedLocationRole;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Coverage;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.CoverageStatus;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Quality;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Region;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.ToneCounts;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnlocatedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnlocatedReasonCount;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnmappedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnmappedReasonCount;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshotQuery;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@Import({TestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@DisplayName("Построение снимка карты стран из настоящего Elasticsearch")
class ElasticsearchCountryMapSnapshotQueryIntegrationTest {

	private static final Instant FROM = Instant.parse("2026-07-19T12:45:00Z");
	private static final Instant TO = Instant.parse("2026-07-20T12:45:00Z");
	private static final String FIRST_EVENT_PHYSICAL =
			"gdelt-events-v1-p20260713-g20001";
	private static final String SECOND_EVENT_PHYSICAL =
			"gdelt-events-v1-p20260720-g20001";
	private static final List<String> TEST_INDEX_NAMES = List.of(
			FIRST_EVENT_PHYSICAL,
			SECOND_EVENT_PHYSICAL);
	private static final Set<String> REGIONS_WITH_EVENTS = Set.of(
			"country:psx",
			"country:rus",
			"country:usa");

	@Autowired
	private ElasticsearchClient client;

	@Autowired
	private GdeltIndexWriter indexWriter;

	@Autowired
	private CountryMapSnapshotQuery snapshotQuery;

	@MockitoBean
	private IngestionCoverageQuery coverageQuery;

	private ExactIndexTarget firstEventTarget;
	private ExactIndexTarget secondEventTarget;

	@BeforeEach
	void resetReadModel() throws IOException {
		removeStableAliasMemberships();
		deleteTestIndices();
		indexWriter.prepareReadModel();
		firstEventTarget = createTarget(FIRST_EVENT_PHYSICAL);
		secondEventTarget = createTarget(SECOND_EVENT_PHYSICAL);
		addStableAliases(FIRST_EVENT_PHYSICAL, SECOND_EVENT_PHYSICAL);
		when(coverageQuery.read(FROM, TO))
				.thenReturn(IngestionCoverageEvidence.complete());
	}

	@AfterEach
	void cleanReadModel() throws IOException {
		removeStableAliasMemberships();
		deleteTestIndices();
	}

	@Test
	@DisplayName("Одним запросом точно считает события двух недельных разделов и все причины без региона")
	void aggregatesTwoActivePartitionsIntoOneDenseSnapshot() throws IOException {
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				firstEventTarget,
				List.of(
						event(1_001, FROM, -2.0, actionLocation("GZ")),
						event(1_002, "2026-07-19T18:00:00Z", 0.0, actionLocation("WE")),
						event(1_005, "2026-07-19T18:15:00Z", 1.0, null),
						event(1_007, "2026-07-19T18:30:00Z", 1.0, actionLocation("")),
						event(1_009, "2026-07-19T18:45:00Z", 1.0, actionLocation("BQ")),
						event(1_010, "2026-07-19T19:00:00Z", 1.0, actionLocation("CK")))));
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				secondEventTarget,
				List.of(
						event(1_003, "2026-07-20T00:00:00Z", 3.0, actionLocation("US")),
						event(1_014, "2026-07-20T00:05:00Z", -0.0, actionLocation("US")),
						event(1_004, "2026-07-20T00:15:00Z", null, actionLocation("RS")),
						event(1_006, "2026-07-20T00:30:00Z", -1.0, actorLocation("US")),
						event(1_008, "2026-07-20T00:45:00Z", 1.0, actionLocation("ZZ")),
						event(1_011, "2026-07-20T01:00:00Z", 1.0, actionLocation("NT")),
						event(1_012, "2026-07-20T01:15:00Z", 1.0, actionLocation("NM")),
						event(1_013, TO, -4.0, actionLocation("US")))));
		indexWriter.refresh(GdeltIndexKind.EVENT, firstEventTarget);
		indexWriter.refresh(GdeltIndexKind.EVENT, secondEventTarget);

		CountryMapSnapshot snapshot = snapshotQuery.read();

		assertThat(stableAliasIndices())
				.containsExactly(FIRST_EVENT_PHYSICAL, SECOND_EVENT_PHYSICAL);
		assertThat(snapshot.from()).isEqualTo(FROM);
		assertThat(snapshot.to()).isEqualTo(TO);
		assertThat(snapshot.geometryVersion()).isEqualTo("country-v1");
		assertThat(snapshot.toneModelVersion())
				.isEqualTo(CountryMapSnapshot.TONE_MODEL_VERSION);
		assertThat(snapshot.coverage())
				.isEqualTo(new Coverage(CoverageStatus.COMPLETE, List.of()));
		assertThat(snapshot.quality()).isEqualTo(new Quality(
				13,
				5,
				2,
				6,
				List.of(
						new UnlocatedReasonCount(
								UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID, 1),
						new UnlocatedReasonCount(UnlocatedReason.ACTOR_FALLBACK, 1),
						new UnlocatedReasonCount(UnlocatedReason.OTHER, 0)),
				List.of(
						new UnmappedReasonCount(UnmappedReason.COUNTRY_CODE_MISSING, 1),
						new UnmappedReasonCount(UnmappedReason.UNKNOWN_COUNTRY_CODE, 1),
						new UnmappedReasonCount(UnmappedReason.NO_REGION_GEOMETRY, 2),
						new UnmappedReasonCount(UnmappedReason.AMBIGUOUS_REGION_MAPPING, 1),
						new UnmappedReasonCount(UnmappedReason.UNSUPPORTED_COUNTRY_CODE, 1),
						new UnmappedReasonCount(UnmappedReason.OTHER, 0))));

		assertThat(snapshot.regions())
				.hasSize(258)
				.extracting(Region::regionId)
				.isSorted()
				.doesNotHaveDuplicates();
		assertThat(region(snapshot, "country:psx")).isEqualTo(new Region(
				"country:psx",
				2,
				2,
				0,
				new ToneCounts(1, 1, 0)));
		assertThat(region(snapshot, "country:rus")).isEqualTo(new Region(
				"country:rus",
				1,
				0,
				1,
				new ToneCounts(0, 0, 0)));
		assertThat(region(snapshot, "country:usa")).isEqualTo(new Region(
				"country:usa",
				2,
				2,
				0,
				new ToneCounts(0, 1, 1)));
		assertThat(snapshot.regions())
				.filteredOn(region -> !REGIONS_WITH_EVENTS.contains(region.regionId()))
				.allSatisfy(region -> {
					assertThat(region.eventCount()).isZero();
					assertThat(region.coloredEventCount()).isZero();
					assertThat(region.missingToneEventCount()).isZero();
					assertThat(region.toneCounts()).isEqualTo(new ToneCounts(0, 0, 0));
				});
		verify(coverageQuery).read(FROM, TO);
	}

	@Test
	@DisplayName("Пустая поисковая модель сохраняет полный каталог стран с нулевыми счетчиками")
	void returnsAllZeroRowsForEmptyAlias() {
		CountryMapSnapshot snapshot = snapshotQuery.read();

		assertThat(snapshot.quality().eligibleEventCount()).isZero();
		assertThat(snapshot.quality().mappedEventCount()).isZero();
		assertThat(snapshot.quality().unlocatedEventCount()).isZero();
		assertThat(snapshot.quality().unmappedEventCount()).isZero();
		assertThat(snapshot.quality().unlocatedReasonCounts())
				.allSatisfy(reason -> assertThat(reason.eventCount()).isZero());
		assertThat(snapshot.quality().unmappedReasonCounts())
				.allSatisfy(reason -> assertThat(reason.eventCount()).isZero());
		assertThat(snapshot.regions())
				.hasSize(258)
				.allSatisfy(region -> {
					assertThat(region.eventCount()).isZero();
					assertThat(region.coloredEventCount()).isZero();
					assertThat(region.missingToneEventCount()).isZero();
					assertThat(region.toneCounts()).isEqualTo(new ToneCounts(0, 0, 0));
				});
		verify(coverageQuery).read(FROM, TO);
	}

	@Test
	@DisplayName("Закрытые индексы стабильного alias отклоняют весь снимок как недоступный")
	void rejectsClosedAlias() throws IOException {
		client.indices().close(request -> request.index(TEST_INDEX_NAMES));
		try {
			assertThatThrownBy(snapshotQuery::read)
					.isInstanceOf(SearchAccessException.class)
					.hasMessage("Сервис поиска временно недоступен");
		}
		finally {
			client.indices().open(request -> request.index(TEST_INDEX_NAMES));
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

	private void addStableAliases(String... indexNames) throws IOException {
		client.indices().updateAliases(request -> {
			for (String indexName : indexNames) {
				request.actions(action -> action.add(add -> add
						.index(indexName)
						.alias(GdeltIndexKind.EVENT.readAlias())));
			}
			return request;
		});
	}

	private void removeStableAliasMemberships() throws IOException {
		for (String indexName : stableAliasIndices()) {
			client.indices().deleteAlias(request -> request
					.index(indexName)
					.name(GdeltIndexKind.EVENT.readAlias()));
		}
	}

	private List<String> stableAliasIndices() throws IOException {
		try {
			var response = client.indices().getAlias(request -> request
					.name(GdeltIndexKind.EVENT.readAlias())
					.allowNoIndices(true)
					.ignoreUnavailable(true));
			return response.aliases().entrySet().stream()
					.filter(entry -> entry.getValue().aliases()
							.containsKey(GdeltIndexKind.EVENT.readAlias()))
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

	private static Region region(CountryMapSnapshot snapshot, String regionId) {
		return snapshot.regions().stream()
				.filter(region -> region.regionId().equals(regionId))
				.findFirst()
				.orElseThrow();
	}

	private static IndexedEventLocation actionLocation(String countryCode) {
		return location(IndexedLocationRole.ACTION, countryCode);
	}

	private static IndexedEventLocation actorLocation(String countryCode) {
		return location(IndexedLocationRole.ACTOR1, countryCode);
	}

	private static IndexedEventLocation location(
			IndexedLocationRole role,
			String countryCode
	) {
		return new IndexedEventLocation(
				role,
				1,
				"Тестовое место",
				countryCode,
				"",
				"",
				"test-feature",
				new IndexedGeoPoint(55.75, 37.62));
	}

	private static IndexedEventDocument event(
			long eventId,
			String dateAdded,
			Double averageTone,
			IndexedEventLocation location
	) {
		return event(eventId, Instant.parse(dateAdded), averageTone, location);
	}

	private static IndexedEventDocument event(
			long eventId,
			Instant dateAdded,
			Double averageTone,
			IndexedEventLocation location
	) {
		return new IndexedEventDocument(
				eventId,
				dateAdded.atZone(ZoneOffset.UTC).toLocalDate(),
				dateAdded,
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
				"01",
				1,
				-1.5,
				averageTone,
				3,
				2,
				2,
				location,
				"https://example.test/event/" + eventId,
				dateAdded,
				"snapshot-event-archive-" + eventId,
				eventId,
				"e".repeat(64));
	}
}
