package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.FiltersBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageUnavailableException;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.CoverageStatus;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnlocatedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnmappedReason;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import tools.jackson.databind.json.JsonMapper;

@DisplayName("Построение плотного снимка карты через Elasticsearch")
class ElasticsearchCountryMapSnapshotQueryTest {

	private static final String SNAPSHOT_DURATION_METER =
			"event_mosaic.search.country_snapshot.duration";
	private static final String FIXTURE_RESOURCE =
			"search/country-geometry/valid-manifest.json";
	private static final Instant NOW = Instant.parse("2026-08-11T12:37:42Z");
	private static final Instant EXPECTED_FROM =
			Instant.parse("2026-08-10T12:15:00Z");
	private static final Instant EXPECTED_TO =
			Instant.parse("2026-08-11T12:15:00Z");
	private static final List<String> REGION_IDS = List.of(
			"country:aaa",
			"country:bbb",
			"country:psx");

	private final ElasticsearchClient client = mock(ElasticsearchClient.class);
	private final IngestionCoverageQuery coverageQuery =
			mock(IngestionCoverageQuery.class);
	private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
	private final CountryGeometryCatalogLoader catalogLoader = catalogLoader();
	private final ElasticsearchCountryMapSnapshotQuery snapshotQuery =
			new ElasticsearchCountryMapSnapshotQuery(
					client,
					coverageQuery,
					catalogLoader,
					new CountrySnapshotWindowPolicy(
							Clock.fixed(NOW, ZoneOffset.UTC),
							properties()),
					new CountryMapSnapshotAssembler(catalogLoader),
					new SearchMetrics(meterRegistry));

	@Test
	@DisplayName("Сначала проверяет полноту, затем отправляет один точный запрос без документов")
	void readsCoverageBeforeExactAggregationRequest() throws IOException {
		when(coverageQuery.read(EXPECTED_FROM, EXPECTED_TO))
				.thenReturn(IngestionCoverageEvidence.complete());
		when(client.search(any(SearchRequest.class))).thenReturn(validResponse());

		CountryMapSnapshot snapshot = snapshotQuery.read();

		var order = inOrder(coverageQuery, client);
		order.verify(coverageQuery).read(EXPECTED_FROM, EXPECTED_TO);
		ArgumentCaptor<SearchRequest> requestCaptor =
				ArgumentCaptor.forClass(SearchRequest.class);
		order.verify(client).search(requestCaptor.capture());
		assertExactRequest(requestCaptor.getValue());
		assertThat(snapshot.from()).isEqualTo(EXPECTED_FROM);
		assertThat(snapshot.to()).isEqualTo(EXPECTED_TO);
		assertThat(snapshot.coverage().status()).isEqualTo(CoverageStatus.COMPLETE);
		assertThat(snapshot.regions())
				.extracting(CountryMapSnapshot.Region::regionId)
				.containsExactlyElementsOf(REGION_IDS);
		assertSnapshotTimer("complete");
	}

	@Test
	@DisplayName("Недоступная диагностика полноты сохраняет агрегаты со статусом UNKNOWN")
	void convertsTypedCoverageOutageToUnknown() throws IOException {
		when(coverageQuery.read(EXPECTED_FROM, EXPECTED_TO))
				.thenThrow(new IngestionCoverageUnavailableException(
						new IllegalStateException("secret database detail")));
		when(client.search(any(SearchRequest.class))).thenReturn(validResponse());

		CountryMapSnapshot snapshot = snapshotQuery.read();

		assertThat(snapshot.coverage().status()).isEqualTo(CoverageStatus.UNKNOWN);
		assertThat(snapshot.coverage().missingIntervals()).isNull();
		assertThat(snapshot.quality().eligibleEventCount()).isZero();
		assertSnapshotTimer("unknown");
	}

	@Test
	@DisplayName("Неожиданная ошибка проверки полноты не маскируется как UNKNOWN")
	void propagatesUnexpectedCoverageDefect() {
		IllegalStateException defect = new IllegalStateException("unexpected defect");
		when(coverageQuery.read(EXPECTED_FROM, EXPECTED_TO)).thenThrow(defect);

		assertThatThrownBy(snapshotQuery::read).isSameAs(defect);

		verifyNoInteractions(client);
		assertSnapshotTimer("unavailable");
	}

	@Test
	@DisplayName("Отклоняет снимок, если Elasticsearch не завершил запрос вовремя")
	void rejectsTimedOutResponse() throws IOException {
		assertUnavailable(response(
				true,
				null,
				0,
				TotalHitsRelation.Eq,
				0,
				zeroAggregations()));
	}

	@Test
	@DisplayName("Отклоняет снимок, если Elasticsearch досрочно остановил сбор данных")
	void rejectsTerminatedEarlyResponse() throws IOException {
		assertUnavailable(response(
				false,
				true,
				0,
				TotalHitsRelation.Eq,
				0,
				zeroAggregations()));
	}

	@Test
	@DisplayName("Отклоняет снимок, если хотя бы один раздел Elasticsearch завершился ошибкой")
	void rejectsFailedShardResponse() throws IOException {
		assertUnavailable(response(
				false,
				null,
				1,
				TotalHitsRelation.Eq,
				0,
				zeroAggregations()));
	}

	@Test
	@DisplayName("Отклоняет приблизительное общее число событий вместо точного результата")
	void rejectsLowerBoundTotalHits() throws IOException {
		assertUnavailable(response(
				false,
				null,
				0,
				TotalHitsRelation.Gte,
				0,
				zeroAggregations()));
	}

	@Test
	@DisplayName("Отклоняет ответ без обязательной группы причин неразмещенных событий")
	void rejectsMissingAggregationBucket() throws IOException {
		Map<String, Aggregate> incomplete = new LinkedHashMap<>(zeroAggregations());
		incomplete.remove(ElasticsearchCountryMapSnapshotQuery.UNLOCATED_AGGREGATION);

		assertUnavailable(response(
				false,
				null,
				0,
				TotalHitsRelation.Eq,
				0,
				incomplete));
	}

	@Test
	@DisplayName("Отклоняет ответ, в котором общее число не равно сумме категорий")
	void rejectsInconsistentCategoryTotals() throws IOException {
		assertUnavailable(response(
				false,
				null,
				0,
				TotalHitsRelation.Eq,
				1,
				zeroAggregations()));
	}

	private void assertUnavailable(SearchResponse<Void> response) throws IOException {
		when(coverageQuery.read(EXPECTED_FROM, EXPECTED_TO))
				.thenReturn(IngestionCoverageEvidence.complete());
		when(client.search(any(SearchRequest.class))).thenReturn(response);

		assertThatThrownBy(snapshotQuery::read)
				.isInstanceOf(SearchAccessException.class)
				.hasMessage("Сервис поиска временно недоступен");

		assertSnapshotTimer("unavailable");
	}

	private void assertExactRequest(SearchRequest request) {
		assertThat(request.index())
				.containsExactly(GdeltIndexKind.EVENT.readAlias());
		assertThat(request.size()).isZero();
		assertThat(request.allowNoIndices()).isFalse();
		assertThat(request.ignoreUnavailable()).isFalse();
		assertThat(request.allowPartialSearchResults()).isFalse();
		assertThat(request.source().isFetch()).isTrue();
		assertThat(request.source().fetch()).isFalse();
		assertThat(request.trackTotalHits().isEnabled()).isTrue();
		assertThat(request.trackTotalHits().enabled()).isTrue();

		assertThat(request.query().isRange()).isTrue();
		assertThat(request.query().range().isDate()).isTrue();
		var dateRange = request.query().range().date();
		assertThat(dateRange.field()).isEqualTo("dateAdded");
		assertThat(dateRange.gte()).isEqualTo(EXPECTED_FROM.toString());
		assertThat(dateRange.lt()).isEqualTo(EXPECTED_TO.toString());

		assertThat(request.aggregations()).containsOnlyKeys(
				ElasticsearchCountryMapSnapshotQuery.REGIONS_AGGREGATION,
				ElasticsearchCountryMapSnapshotQuery.UNLOCATED_AGGREGATION,
				ElasticsearchCountryMapSnapshotQuery.UNMAPPED_AGGREGATION);
		var regions = request.aggregations()
				.get(ElasticsearchCountryMapSnapshotQuery.REGIONS_AGGREGATION);
		assertThat(regions.isFilters()).isTrue();
		assertThat(regions.filters().filters().isKeyed()).isTrue();
		Map<String, Query> regionFilters = regions.filters().filters().keyed();
		assertThat(regionFilters).containsOnlyKeys(REGION_IDS);
		assertThat(regionFilters.get("country:bbb").isMatchNone()).isTrue();
		assertThat(regionFilters.get("country:psx").bool().filter()).hasSize(2);
		Query palestineCodes = regionFilters.get("country:psx").bool().filter().get(1);
		assertThat(palestineCodes.isTerms()).isTrue();
		assertThat(palestineCodes.terms().field()).isEqualTo("location.countryCode");
		assertThat(palestineCodes.terms().terms().value())
				.extracting(value -> value.stringValue())
				.containsExactly("GZ", "WE");

		var tones = regions.aggregations()
				.get(ElasticsearchCountryMapSnapshotQuery.TONES_AGGREGATION);
		assertThat(tones.isFilters()).isTrue();
		Map<String, Query> toneFilters = tones.filters().filters().keyed();
		assertThat(toneFilters).containsOnlyKeys(
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_EXTREME_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_STRONG_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_MILD_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.ZERO_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_MILD_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_STRONG_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_EXTREME_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.MISSING_TONE_BUCKET);
		var negativeExtremeRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.NEGATIVE_EXTREME_TONE_BUCKET)
				.range()
				.number();
		assertThat(negativeExtremeRange.gt()).isNull();
		assertThat(negativeExtremeRange.gte()).isNull();
		assertThat(negativeExtremeRange.lt()).isNull();
		assertThat(negativeExtremeRange.lte()).isEqualTo(-8.0);

		var negativeStrongRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.NEGATIVE_STRONG_TONE_BUCKET)
				.range()
				.number();
		assertThat(negativeStrongRange.gt()).isEqualTo(-8.0);
		assertThat(negativeStrongRange.gte()).isNull();
		assertThat(negativeStrongRange.lt()).isNull();
		assertThat(negativeStrongRange.lte()).isEqualTo(-3.0);

		var negativeMildRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.NEGATIVE_MILD_TONE_BUCKET)
				.range()
				.number();
		assertThat(negativeMildRange.gt()).isEqualTo(-3.0);
		assertThat(negativeMildRange.gte()).isNull();
		assertThat(negativeMildRange.lte()).isNull();
		assertThat(Double.doubleToRawLongBits(negativeMildRange.lt()))
				.isEqualTo(Double.doubleToRawLongBits(-0.0));

		var zeroRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.ZERO_TONE_BUCKET)
				.range()
				.number();
		assertThat(Double.doubleToRawLongBits(zeroRange.gte()))
				.isEqualTo(Double.doubleToRawLongBits(-0.0));
		assertThat(Double.doubleToRawLongBits(zeroRange.lte()))
				.isEqualTo(Double.doubleToRawLongBits(0.0));

		var positiveMildRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.POSITIVE_MILD_TONE_BUCKET)
				.range()
				.number();
		assertThat(positiveMildRange.gt()).isEqualTo(0.0);
		assertThat(positiveMildRange.gte()).isNull();
		assertThat(positiveMildRange.lt()).isEqualTo(3.0);
		assertThat(positiveMildRange.lte()).isNull();

		var positiveStrongRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.POSITIVE_STRONG_TONE_BUCKET)
				.range()
				.number();
		assertThat(positiveStrongRange.gt()).isNull();
		assertThat(positiveStrongRange.gte()).isEqualTo(3.0);
		assertThat(positiveStrongRange.lt()).isEqualTo(8.0);
		assertThat(positiveStrongRange.lte()).isNull();

		var positiveExtremeRange = toneFilters
				.get(ElasticsearchCountryMapSnapshotQuery.POSITIVE_EXTREME_TONE_BUCKET)
				.range()
				.number();
		assertThat(positiveExtremeRange.gt()).isNull();
		assertThat(positiveExtremeRange.gte()).isEqualTo(8.0);
		assertThat(positiveExtremeRange.lt()).isNull();
		assertThat(positiveExtremeRange.lte()).isNull();

		Query missingTone = toneFilters.get(
				ElasticsearchCountryMapSnapshotQuery.MISSING_TONE_BUCKET);
		assertThat(missingTone.isBool()).isTrue();
		assertThat(missingTone.bool().mustNot()).singleElement()
				.satisfies(query -> {
					assertThat(query.isExists()).isTrue();
					assertThat(query.exists().field()).isEqualTo("averageTone");
				});

		assertReasonFilters(
				request,
				ElasticsearchCountryMapSnapshotQuery.UNLOCATED_AGGREGATION,
				Set.of(
						UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID.name(),
						UnlocatedReason.ACTOR_FALLBACK.name(),
						UnlocatedReason.OTHER.name()));
		assertReasonFilters(
				request,
				ElasticsearchCountryMapSnapshotQuery.UNMAPPED_AGGREGATION,
				Set.of(
						UnmappedReason.COUNTRY_CODE_MISSING.name(),
						UnmappedReason.UNKNOWN_COUNTRY_CODE.name(),
						UnmappedReason.NO_REGION_GEOMETRY.name(),
						UnmappedReason.AMBIGUOUS_REGION_MAPPING.name(),
						UnmappedReason.UNSUPPORTED_COUNTRY_CODE.name(),
						UnmappedReason.OTHER.name()));
	}

	private static void assertReasonFilters(
			SearchRequest request,
			String aggregationName,
			Set<String> expectedReasons
	) {
		var category = request.aggregations().get(aggregationName);
		assertThat(category.isFilter()).isTrue();
		var reasons = category.aggregations()
				.get(ElasticsearchCountryMapSnapshotQuery.REASONS_AGGREGATION);
		assertThat(reasons.isFilters()).isTrue();
		assertThat(reasons.filters().filters().isKeyed()).isTrue();
		assertThat(reasons.filters().filters().keyed().keySet())
				.containsExactlyInAnyOrderElementsOf(expectedReasons);
	}

	private void assertSnapshotTimer(String outcome) {
		Timer timer = meterRegistry.find(SNAPSHOT_DURATION_METER)
				.tag("outcome", outcome)
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(timer.getId().getTags())
				.extracting(Tag::getKey)
				.containsExactly("outcome");
		assertThat(meterRegistry.find(SNAPSHOT_DURATION_METER).timers()).hasSize(1);
	}

	private static SearchResponse<Void> validResponse() {
		return response(
				false,
				null,
				0,
				TotalHitsRelation.Eq,
				0,
				zeroAggregations());
	}

	private static SearchResponse<Void> response(
			boolean timedOut,
			Boolean terminatedEarly,
			int failedShards,
			TotalHitsRelation totalHitsRelation,
			long eligibleEventCount,
			Map<String, Aggregate> aggregations
	) {
		return SearchResponse.of(response -> {
			response.took(1)
					.timedOut(timedOut)
					.shards(shards -> shards
							.total(1)
							.successful(failedShards == 0 ? 1 : 0)
							.failed(failedShards))
					.hits(hits -> hits
							.total(total -> total
									.value(eligibleEventCount)
									.relation(totalHitsRelation))
							.hits(List.of()))
					.aggregations(aggregations);
			if (terminatedEarly != null) {
				response.terminatedEarly(terminatedEarly);
			}
			return response;
		});
	}

	private static Map<String, Aggregate> zeroAggregations() {
		Map<String, FiltersBucket> regionBuckets = new LinkedHashMap<>();
		for (String regionId : REGION_IDS) {
			regionBuckets.put(regionId, zeroRegionBucket());
		}

		Map<String, Aggregate> aggregations = new LinkedHashMap<>();
		aggregations.put(
				ElasticsearchCountryMapSnapshotQuery.REGIONS_AGGREGATION,
				keyedFilters(regionBuckets));
		aggregations.put(
				ElasticsearchCountryMapSnapshotQuery.UNLOCATED_AGGREGATION,
				zeroCategory(UnlocatedReason.values()));
		aggregations.put(
				ElasticsearchCountryMapSnapshotQuery.UNMAPPED_AGGREGATION,
				zeroCategory(UnmappedReason.values()));
		return aggregations;
	}

	private static FiltersBucket zeroRegionBucket() {
		Map<String, FiltersBucket> tones = new LinkedHashMap<>();
		for (String tone : List.of(
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_EXTREME_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_STRONG_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.NEGATIVE_MILD_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.ZERO_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_MILD_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_STRONG_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.POSITIVE_EXTREME_TONE_BUCKET,
				ElasticsearchCountryMapSnapshotQuery.MISSING_TONE_BUCKET)) {
			tones.put(tone, FiltersBucket.of(bucket -> bucket.docCount(0)));
		}
		return FiltersBucket.of(bucket -> bucket
				.docCount(0)
				.aggregations(
						ElasticsearchCountryMapSnapshotQuery.TONES_AGGREGATION,
						keyedFilters(tones)));
	}

	private static Aggregate zeroCategory(Enum<?>[] reasons) {
		Map<String, FiltersBucket> reasonBuckets = new LinkedHashMap<>();
		for (Enum<?> reason : reasons) {
			reasonBuckets.put(
					reason.name(),
					FiltersBucket.of(bucket -> bucket.docCount(0)));
		}
		return Aggregate.of(aggregate -> aggregate.filter(filter -> filter
				.docCount(0)
				.aggregations(
						ElasticsearchCountryMapSnapshotQuery.REASONS_AGGREGATION,
						keyedFilters(reasonBuckets))));
	}

	private static Aggregate keyedFilters(Map<String, FiltersBucket> buckets) {
		return Aggregate.of(aggregate -> aggregate.filters(filters -> filters
				.buckets(keyed -> keyed.keyed(buckets))));
	}

	private static CountryGeometryCatalogLoader catalogLoader() {
		Resource manifest = new ClassPathResource(FIXTURE_RESOURCE);
		var resourceLoader = new DefaultResourceLoader() {
			@Override
			public Resource getResource(String location) {
				return manifest;
			}
		};
		return new CountryGeometryCatalogLoader(
				JsonMapper.builder().build(),
				properties(),
				resourceLoader);
	}

	private static CountryMapSnapshotProperties properties() {
		return new CountryMapSnapshotProperties(
				"country-v1",
				Duration.ofMinutes(15));
	}
}
