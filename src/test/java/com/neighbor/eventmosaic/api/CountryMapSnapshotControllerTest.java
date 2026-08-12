package com.neighbor.eventmosaic.api;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Coverage;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.CoverageStatus;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.MissingInterval;
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
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(CountryMapSnapshotController.class)
@DisplayName("HTTP API плотного снимка карты стран")
class CountryMapSnapshotControllerTest {

	private static final Instant FROM = Instant.parse("2026-08-10T12:15:00Z");
	private static final Instant TO = Instant.parse("2026-08-11T12:15:00Z");
	private static final long MAX_BROWSER_SAFE_INTEGER = 9_007_199_254_740_991L;

	@Autowired
	private MockMvc mockMvc;

	@MockitoBean
	private CountryMapSnapshotQuery snapshotQuery;

	@Test
	@DisplayName("Возвращает канонический плотный JSON со всеми счетчиками и без геометрии")
	void returnsCanonicalDenseResponse() throws Exception {
		when(snapshotQuery.read()).thenReturn(snapshot(
				new Coverage(CoverageStatus.COMPLETE, List.of())));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isOk())
				.andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
				.andExpect(jsonPath("$", aMapWithSize(4)))
				.andExpect(jsonPath("$.snapshot", aMapWithSize(4)))
				.andExpect(jsonPath("$.snapshot.from").value(FROM.toString()))
				.andExpect(jsonPath("$.snapshot.to").value(TO.toString()))
				.andExpect(jsonPath("$.snapshot.geometryVersion").value("country-v1"))
				.andExpect(jsonPath("$.snapshot.toneModelVersion").value("tone-bands-v1"))
				.andExpect(jsonPath("$.coverage.status").value("COMPLETE"))
				.andExpect(jsonPath("$.coverage", aMapWithSize(2)))
				.andExpect(jsonPath("$.coverage.missingIntervals.length()").value(0))
				.andExpect(jsonPath("$.quality.eligibleEventCount").value(13))
				.andExpect(jsonPath("$.quality.mappedEventCount").value(4))
				.andExpect(jsonPath("$.quality.unlocatedEventCount").value(3))
				.andExpect(jsonPath("$.quality.unmappedEventCount").value(6))
				.andExpect(jsonPath("$.quality", aMapWithSize(6)))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts.length()").value(3))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts[0].reason")
						.value("ACTION_GEO_MISSING_OR_INVALID"))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts[0].eventCount")
						.value(1))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts[1].reason")
						.value("ACTOR_FALLBACK"))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts[1].eventCount")
						.value(2))
				.andExpect(jsonPath("$.quality.unlocatedReasonCounts[2].reason")
						.value("OTHER"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts.length()").value(6))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[0].reason")
						.value("COUNTRY_CODE_MISSING"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[1].reason")
						.value("UNKNOWN_COUNTRY_CODE"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[2].reason")
						.value("NO_REGION_GEOMETRY"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[3].reason")
						.value("AMBIGUOUS_REGION_MAPPING"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[4].reason")
						.value("UNSUPPORTED_COUNTRY_CODE"))
				.andExpect(jsonPath("$.quality.unmappedReasonCounts[5].reason")
						.value("OTHER"))
				.andExpect(jsonPath("$.regions.length()").value(2))
				.andExpect(jsonPath("$.regions[0].regionId").value("country:aaa"))
				.andExpect(jsonPath("$.regions[0]", aMapWithSize(5)))
				.andExpect(jsonPath("$.regions[0].eventCount").value(4))
				.andExpect(jsonPath("$.regions[0].coloredEventCount").value(3))
				.andExpect(jsonPath("$.regions[0].missingToneEventCount").value(1))
				.andExpect(jsonPath("$.regions[0].toneCounts.NEGATIVE_EXTREME").value(1))
				.andExpect(jsonPath("$.regions[0].toneCounts.NEGATIVE_STRONG").value(0))
				.andExpect(jsonPath("$.regions[0].toneCounts.NEGATIVE_MILD").value(0))
				.andExpect(jsonPath("$.regions[0].toneCounts.ZERO").value(1))
				.andExpect(jsonPath("$.regions[0].toneCounts.POSITIVE_MILD").value(0))
				.andExpect(jsonPath("$.regions[0].toneCounts.POSITIVE_STRONG").value(0))
				.andExpect(jsonPath("$.regions[0].toneCounts.POSITIVE_EXTREME").value(1))
				.andExpect(jsonPath("$.regions[0].toneCounts", aMapWithSize(7)))
				.andExpect(jsonPath("$.regions[1].regionId").value("country:bbb"))
				.andExpect(jsonPath("$.regions[1].eventCount").value(0))
				.andExpect(jsonPath("$.regions[0].displayName").doesNotExist())
				.andExpect(jsonPath("$.regions[0].geometry").doesNotExist())
				.andExpect(jsonPath("$.regions[0].provenance").doesNotExist())
				.andExpect(jsonPath("$.regions[0].disputeStatus").doesNotExist())
				.andExpect(jsonPath("$.regions[0].disputeSource").doesNotExist());
	}

	@Test
	@DisplayName("Возвращает объединенные пропуски для частично покрытого окна")
	void returnsPartialCoverageIntervals() throws Exception {
		when(snapshotQuery.read()).thenReturn(snapshot(new Coverage(
				CoverageStatus.PARTIAL,
				List.of(new MissingInterval(
						Instant.parse("2026-08-10T18:00:00Z"),
						Instant.parse("2026-08-10T18:30:00Z"))))));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.coverage.status").value("PARTIAL"))
				.andExpect(jsonPath("$.coverage.missingIntervals.length()").value(1))
				.andExpect(jsonPath("$.coverage.missingIntervals[0].from")
						.value("2026-08-10T18:00:00Z"))
				.andExpect(jsonPath("$.coverage.missingIntervals[0].to")
						.value("2026-08-10T18:30:00Z"));
	}

	@Test
	@DisplayName("Сохраняет неизвестную полноту успешным ответом с явным null")
	void returnsUnknownCoverageWithNullIntervals() throws Exception {
		when(snapshotQuery.read()).thenReturn(snapshot(
				new Coverage(CoverageStatus.UNKNOWN, null)));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isOk())
				.andExpect(jsonPath("$.coverage.status").value("UNKNOWN"))
				.andExpect(jsonPath("$.coverage.missingIntervals").value(nullValue()))
				.andExpect(jsonPath("$.quality.eligibleEventCount").value(13));
	}

	@Test
	@DisplayName("Отклоняет любую query string до чтения поисковой модели")
	void rejectsQueryStringBeforeSearch() throws Exception {
		mockMvc.perform(get("/api/v1/map/country-snapshot")
						.queryParam("from", "2026-08-10T12:15:00Z"))
				.andExpect(status().isBadRequest())
				.andExpect(content().contentTypeCompatibleWith(
						MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.code")
						.value("INVALID_COUNTRY_SNAPSHOT_REQUEST"))
				.andExpect(jsonPath("$.detail")
						.value("Снимок карты не принимает параметры запроса"));

		verifyNoInteractions(snapshotQuery);
	}

	@Test
	@DisplayName("Не раскрывает техническую причину недоступности Elasticsearch")
	void returnsSafeServiceUnavailableProblem() throws Exception {
		when(snapshotQuery.read()).thenThrow(
				new SearchAccessException(new IOException("secret backend reason")));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isServiceUnavailable())
				.andExpect(content().contentTypeCompatibleWith(
						MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.code").value("SEARCH_UNAVAILABLE"))
				.andExpect(jsonPath("$.detail")
						.value("Сервис поиска временно недоступен"))
				.andExpect(content().string(not(containsString("secret"))));
	}

	@Test
	@DisplayName("Сохраняет точное максимальное число, которое безопасно читает браузер")
	void returnsExactMaximumBrowserSafeCount() throws Exception {
		when(snapshotQuery.read()).thenReturn(snapshotWithUnlocatedCount(
				MAX_BROWSER_SAFE_INTEGER));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isOk())
				.andExpect(content().string(containsString(
						"\"eligibleEventCount\":9007199254740991")))
				.andExpect(content().string(containsString(
						"\"eventCount\":9007199254740991")));
	}

	@Test
	@DisplayName("Небезопасный публичный счетчик отклоняет весь снимок безопасным ответом")
	void rejectsUnsafeCountWithSafeServiceUnavailableProblem() throws Exception {
		when(snapshotQuery.read()).thenReturn(snapshotWithUnlocatedCount(
				MAX_BROWSER_SAFE_INTEGER + 1));

		mockMvc.perform(get("/api/v1/map/country-snapshot"))
				.andExpect(status().isServiceUnavailable())
				.andExpect(content().contentTypeCompatibleWith(
						MediaType.APPLICATION_PROBLEM_JSON))
				.andExpect(jsonPath("$.code").value("SEARCH_UNAVAILABLE"))
				.andExpect(jsonPath("$.detail")
						.value("Сервис поиска временно недоступен"))
				.andExpect(content().string(not(containsString("9007199254740992"))));
	}

	@Test
	@DisplayName("Вложенный счетчик tone выше безопасной границы отклоняется до JSON")
	void rejectsUnsafeNestedToneCount() {
		assertThatThrownBy(() -> new CountryMapSnapshotResponse.ToneCountsResponse(
				MAX_BROWSER_SAFE_INTEGER + 1,
				0,
				0,
				0,
				0,
				0,
				0))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("browser-safe");
	}

	private static CountryMapSnapshot snapshot(Coverage coverage) {
		return new CountryMapSnapshot(
				FROM,
				TO,
				"country-v1",
				CountryMapSnapshot.TONE_MODEL_VERSION,
				coverage,
				new Quality(
						13,
						4,
						3,
						6,
						List.of(
								new UnlocatedReasonCount(
										UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID, 1),
								new UnlocatedReasonCount(UnlocatedReason.ACTOR_FALLBACK, 2),
								new UnlocatedReasonCount(UnlocatedReason.OTHER, 0)),
						List.of(
								new UnmappedReasonCount(UnmappedReason.COUNTRY_CODE_MISSING, 1),
								new UnmappedReasonCount(UnmappedReason.UNKNOWN_COUNTRY_CODE, 1),
								new UnmappedReasonCount(UnmappedReason.NO_REGION_GEOMETRY, 1),
								new UnmappedReasonCount(
										UnmappedReason.AMBIGUOUS_REGION_MAPPING, 1),
								new UnmappedReasonCount(
										UnmappedReason.UNSUPPORTED_COUNTRY_CODE, 1),
								new UnmappedReasonCount(UnmappedReason.OTHER, 1))),
				List.of(
						new Region(
								"country:aaa",
								4,
								3,
								1,
								new ToneCounts(1, 0, 0, 1, 0, 0, 1)),
						new Region(
								"country:bbb",
								0,
								0,
								0,
								new ToneCounts(0, 0, 0, 0, 0, 0, 0))));
	}

	private static CountryMapSnapshot snapshotWithUnlocatedCount(long count) {
		return new CountryMapSnapshot(
				FROM,
				TO,
				"country-v1",
				CountryMapSnapshot.TONE_MODEL_VERSION,
				new Coverage(CoverageStatus.COMPLETE, List.of()),
				new Quality(
						count,
						0,
						count,
						0,
						List.of(
								new UnlocatedReasonCount(
										UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID,
										count),
								new UnlocatedReasonCount(UnlocatedReason.ACTOR_FALLBACK, 0),
								new UnlocatedReasonCount(UnlocatedReason.OTHER, 0)),
						List.of(
								new UnmappedReasonCount(UnmappedReason.COUNTRY_CODE_MISSING, 0),
								new UnmappedReasonCount(UnmappedReason.UNKNOWN_COUNTRY_CODE, 0),
								new UnmappedReasonCount(UnmappedReason.NO_REGION_GEOMETRY, 0),
								new UnmappedReasonCount(
										UnmappedReason.AMBIGUOUS_REGION_MAPPING,
										0),
								new UnmappedReasonCount(
										UnmappedReason.UNSUPPORTED_COUNTRY_CODE,
										0),
								new UnmappedReasonCount(UnmappedReason.OTHER, 0))),
				List.of(new Region(
						"country:aaa",
						0,
						0,
						0,
						new ToneCounts(0, 0, 0, 0, 0, 0, 0))));
	}
}
