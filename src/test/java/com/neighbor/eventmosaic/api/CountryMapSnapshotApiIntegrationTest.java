package com.neighbor.eventmosaic.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Import({TestcontainersConfiguration.class, FixedClockTestConfiguration.class})
@SpringBootTest
@AutoConfigureMockMvc
@DisplayName("Полный HTTP-путь снимка карты стран")
class CountryMapSnapshotApiIntegrationTest {

	private static final String ENDPOINT = "/api/v1/map/country-snapshot";
	private static final String MANIFEST_RESOURCE =
			"static/map/geometry/country-v1/manifest.json";
	private static final String EVENT_INDEX = "gdelt-events-v1-p20260713-g30001";
	private static final List<String> TEST_INDICES = List.of(EVENT_INDEX);
	private static final Instant FROM = Instant.parse("2026-07-19T12:45:00Z");
	private static final Instant TO = Instant.parse("2026-07-20T12:45:00Z");
	private static final Set<String> REGIONS_WITH_EVENTS = Set.of(
			"country:psx",
			"country:rus",
			"country:usa");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private ElasticsearchClient elasticsearchClient;

	@Autowired
	private GdeltIndexWriter indexWriter;

	@Autowired
	private JdbcClient jdbcClient;

	private ExactIndexTarget eventTarget;

	@BeforeEach
	void prepareReadModelAndEmptyLedger() throws IOException {
		truncateLedger();
		removeStableAliasMemberships();
		deleteTestIndices();
		indexWriter.prepareReadModel();
		elasticsearchClient.indices().create(request -> request.index(EVENT_INDEX));
		eventTarget = exactTarget(EVENT_INDEX);
		addStableAlias(EVENT_INDEX);
		indexWriter.write(new BulkIndexCommand<>(
				GdeltIndexKind.EVENT,
				eventTarget,
				representativeEvents()));
		indexWriter.refresh(GdeltIndexKind.EVENT, eventTarget);
	}

	@AfterEach
	void cleanReadModel() throws IOException {
		removeStableAliasMemberships();
		deleteTestIndices();
	}

	@Test
	@DisplayName("Возвращает полный каталог из опубликованного manifest и честную неизвестную полноту")
	void returnsDenseSnapshotFromRealBackendsAndManifest() throws Exception {
		var result = mockMvc.perform(get(ENDPOINT))
				.andExpect(status().isOk())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
				.andReturn();

		String responseJson = result.getResponse().getContentAsString();
		JsonNode response = objectMapper.readTree(responseJson);
		JsonNode manifest = objectMapper.readTree(
				new ClassPathResource(MANIFEST_RESOURCE).getContentAsByteArray());

		assertThat(response.path("snapshot").path("from").asString())
				.isEqualTo(FROM.toString());
		assertThat(response.path("snapshot").path("to").asString())
				.isEqualTo(TO.toString());
		assertThat(response.path("snapshot").path("geometryVersion").asString())
				.isEqualTo(manifest.path("geometryVersion").asString())
				.isEqualTo("country-v1");
		assertThat(response.path("snapshot").path("toneModelVersion").asString())
				.isEqualTo("sign-v1");
		assertThat(response.path("coverage").path("status").asString())
				.isEqualTo("UNKNOWN");
		JsonNode missingIntervals = response.path("coverage").get("missingIntervals");
		assertThat(missingIntervals)
				.as("UNKNOWN содержит явный JSON null, а не пропущенное поле")
				.isNotNull();
		assertThat(missingIntervals.isNull())
				.as("UNKNOWN содержит явный JSON null")
				.isTrue();

		JsonNode quality = response.path("quality");
		assertThat(requiredLong(quality, "eligibleEventCount")).isEqualTo(11);
		assertThat(requiredLong(quality, "mappedEventCount")).isEqualTo(4);
		assertThat(requiredLong(quality, "unlocatedEventCount")).isEqualTo(2);
		assertThat(requiredLong(quality, "unmappedEventCount")).isEqualTo(5);
		assertThat(textValues(
				quality.path("unlocatedReasonCounts"), "reason"))
				.containsExactly(
						"ACTION_GEO_MISSING_OR_INVALID",
						"ACTOR_FALLBACK",
						"OTHER");
		assertThat(longValues(
				quality.path("unlocatedReasonCounts"), "eventCount"))
				.containsExactly(1L, 1L, 0L);
		assertThat(textValues(
				quality.path("unmappedReasonCounts"), "reason"))
				.containsExactly(
						"COUNTRY_CODE_MISSING",
						"UNKNOWN_COUNTRY_CODE",
						"NO_REGION_GEOMETRY",
						"AMBIGUOUS_REGION_MAPPING",
						"UNSUPPORTED_COUNTRY_CODE",
						"OTHER");
		assertThat(longValues(
				quality.path("unmappedReasonCounts"), "eventCount"))
				.containsExactly(1L, 1L, 1L, 1L, 1L, 0L);

		JsonNode regions = response.path("regions");
		List<String> expectedRegionIds = textValues(
				manifest.path("regions"), "regionId");
		assertThat(expectedRegionIds).hasSize(258);
		assertThat(textValues(regions, "regionId"))
				.containsExactlyElementsOf(expectedRegionIds);
		assertRegionCounts(regions, quality);
		assertThat(zeroRegionCount(regions)).isEqualTo(255);
		JsonNode palestineToneCounts = region(
				regions, "country:psx").path("toneCounts");
		assertThat(palestineToneCounts.path("negative").asLong()).isEqualTo(1);
		assertThat(palestineToneCounts.path("zero").asLong()).isEqualTo(1);
		assertThat(palestineToneCounts.path("positive").asLong()).isZero();
		assertThat(requiredLong(
				region(regions, "country:rus"), "missingToneEventCount"))
				.isEqualTo(1);
		assertThat(requiredLong(
				region(regions, "country:usa").path("toneCounts"), "positive"))
				.isEqualTo(1);

		assertThat(responseJson).doesNotContain(
				"\"displayName\"",
				"\"geometry\"",
				"\"provenance\"",
				"\"disputeStatus\"",
				"\"disputeSource\"");
	}

	@Test
	@DisplayName("Закрытый поисковый индекс дает безопасный 503 без технической причины")
	void closedIndexReturnsSafeServiceUnavailableProblem() throws Exception {
		elasticsearchClient.indices().close(request -> request.index(EVENT_INDEX));
		try {
			mockMvc.perform(get(ENDPOINT))
					.andExpect(status().isServiceUnavailable())
					.andExpect(content().contentTypeCompatibleWith(
							MediaType.APPLICATION_PROBLEM_JSON))
					.andExpect(jsonPath("$.code").value("SEARCH_UNAVAILABLE"))
					.andExpect(jsonPath("$.detail")
							.value("Сервис поиска временно недоступен"))
					.andExpect(content().string(not(containsString(EVENT_INDEX))))
					.andExpect(content().string(not(containsString("index_closed"))))
					.andExpect(content().string(not(containsString("ElasticsearchException"))));
		}
		finally {
			elasticsearchClient.indices().open(request -> request.index(EVENT_INDEX));
		}
	}

	private void truncateLedger() {
		jdbcClient.sql("""
				truncate table
				    ingestion_archive_processing,
				    index_maintenance_operations,
				    index_generations,
				    index_logical_partitions,
				    ingestion_gaps,
				    ingestion_source_state,
				    ingestion_archives,
				    ingestion_runs
				restart identity cascade
				""").update();
	}

	private ExactIndexTarget exactTarget(String indexName) throws IOException {
		var settings = elasticsearchClient.indices()
				.get(request -> request.index(indexName))
				.get(indexName)
				.settings();
		var indexSettings = settings.index() == null ? settings : settings.index();
		return new ExactIndexTarget(indexName, indexSettings.uuid());
	}

	private void addStableAlias(String indexName) throws IOException {
		elasticsearchClient.indices().updateAliases(request -> request.actions(action ->
				action.add(add -> add
						.index(indexName)
						.alias(GdeltIndexKind.EVENT.readAlias()))));
	}

	private void removeStableAliasMemberships() throws IOException {
		for (String indexName : stableAliasIndices()) {
			elasticsearchClient.indices().deleteAlias(request -> request
					.index(indexName)
					.name(GdeltIndexKind.EVENT.readAlias()));
		}
	}

	private List<String> stableAliasIndices() throws IOException {
		try {
			var response = elasticsearchClient.indices().getAlias(request -> request
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
		elasticsearchClient.indices().delete(request -> request
				.index(TEST_INDICES)
				.allowNoIndices(true)
				.ignoreUnavailable(true));
	}

	private static List<IndexedEventDocument> representativeEvents() {
		return List.of(
				event(31_001, FROM, -2.0, actionLocation("GZ")),
				event(31_002, "2026-07-19T13:00:00Z", 0.0, actionLocation("WE")),
				event(31_003, "2026-07-19T13:15:00Z", 3.0, actionLocation("US")),
				event(31_004, "2026-07-19T13:30:00Z", null, actionLocation("RS")),
				event(31_005, "2026-07-19T13:45:00Z", 1.0, actorLocation("US")),
				event(31_006, "2026-07-19T14:00:00Z", 1.0, null),
				event(31_007, "2026-07-19T14:15:00Z", 1.0, actionLocation("ZZ")),
				event(31_008, "2026-07-19T14:30:00Z", 1.0, actionLocation("")),
				event(31_009, "2026-07-19T14:45:00Z", 1.0, actionLocation("BQ")),
				event(31_010, "2026-07-19T15:00:00Z", 1.0, actionLocation("NT")),
				event(31_011, "2026-07-19T15:15:00Z", 1.0, actionLocation("NM")));
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
				"snapshot-api-event-archive-" + eventId,
				eventId,
				"e".repeat(64));
	}

	private static List<String> textValues(JsonNode array, String field) {
		var values = new ArrayList<String>();
		for (JsonNode item : array) {
			values.add(item.path(field).asString());
		}
		return List.copyOf(values);
	}

	private static List<Long> longValues(JsonNode array, String field) {
		var values = new ArrayList<Long>();
		for (JsonNode item : array) {
			values.add(requiredLong(item, field));
		}
		return List.copyOf(values);
	}

	private static JsonNode region(JsonNode regions, String regionId) {
		for (JsonNode region : regions) {
			if (regionId.equals(region.path("regionId").asString())) {
				return region;
			}
		}
		throw new AssertionError("В ответе отсутствует регион " + regionId);
	}

	private static void assertRegionCounts(JsonNode regions, JsonNode quality) {
		long mappedFromRegions = 0;
		for (JsonNode region : regions) {
			long eventCount = requiredLong(region, "eventCount");
			long coloredEventCount = requiredLong(region, "coloredEventCount");
			long missingToneEventCount = requiredLong(
					region, "missingToneEventCount");
			JsonNode toneCounts = region.path("toneCounts");
			long coloredFromTones = requiredLong(toneCounts, "negative")
					+ requiredLong(toneCounts, "zero")
					+ requiredLong(toneCounts, "positive");

			assertThat(coloredEventCount)
					.as(region.path("regionId").asString())
					.isEqualTo(coloredFromTones);
			assertThat(eventCount)
					.as(region.path("regionId").asString())
					.isEqualTo(coloredEventCount + missingToneEventCount);
			mappedFromRegions += eventCount;
		}

		assertThat(mappedFromRegions)
				.isEqualTo(requiredLong(quality, "mappedEventCount"));
		assertThat(requiredLong(quality, "eligibleEventCount")).isEqualTo(
				requiredLong(quality, "mappedEventCount")
						+ requiredLong(quality, "unlocatedEventCount")
						+ requiredLong(quality, "unmappedEventCount"));
		assertThat(sum(quality.path("unlocatedReasonCounts"), "eventCount"))
				.isEqualTo(requiredLong(quality, "unlocatedEventCount"));
		assertThat(sum(quality.path("unmappedReasonCounts"), "eventCount"))
				.isEqualTo(requiredLong(quality, "unmappedEventCount"));
	}

	private static long zeroRegionCount(JsonNode regions) {
		long count = 0;
		for (JsonNode region : regions) {
			String regionId = region.path("regionId").asString();
			if (REGIONS_WITH_EVENTS.contains(regionId)) {
				continue;
			}
			assertThat(requiredLong(region, "eventCount")).as(regionId).isZero();
			assertThat(requiredLong(region, "coloredEventCount"))
					.as(regionId)
					.isZero();
			assertThat(requiredLong(region, "missingToneEventCount"))
					.as(regionId)
					.isZero();
			assertThat(sumToneCounts(region.path("toneCounts"))).as(regionId).isZero();
			count++;
		}
		return count;
	}

	private static long sum(JsonNode array, String field) {
		long total = 0;
		for (JsonNode item : array) {
			total += requiredLong(item, field);
		}
		return total;
	}

	private static long sumToneCounts(JsonNode toneCounts) {
		return requiredLong(toneCounts, "negative")
				+ requiredLong(toneCounts, "zero")
				+ requiredLong(toneCounts, "positive");
	}

	private static long requiredLong(JsonNode object, String field) {
		JsonNode value = object.get(field);
		assertThat(value)
				.as("Обязательное числовое поле %s", field)
				.isNotNull();
		assertThat(value.isIntegralNumber())
				.as("Поле %s содержит целое число", field)
				.isTrue();
		return value.asLong();
	}
}
