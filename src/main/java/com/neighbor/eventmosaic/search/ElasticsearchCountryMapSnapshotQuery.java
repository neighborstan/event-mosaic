package com.neighbor.eventmosaic.search;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.FiltersBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.JsonpMappingException;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageQuery;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageUnavailableException;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnlocatedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnmappedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshotQuery;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.springframework.stereotype.Component;

/**
 * Строит один плотный снимок карты из полного ответа Elasticsearch и отдельно
 * прочитанного доказательства полноты ingestion. Любая неполнота поискового
 * ответа отклоняет снимок целиком.
 */
@Component
public class ElasticsearchCountryMapSnapshotQuery implements CountryMapSnapshotQuery {

	private static final String DATE_ADDED_FIELD = "dateAdded";
	private static final String LOCATION_ROLE_FIELD = "location.role";
	private static final String COUNTRY_CODE_FIELD = "location.countryCode";
	private static final String AVERAGE_TONE_FIELD = "averageTone";
	private static final String ACTION_ROLE = "ACTION";
	private static final List<String> ACTOR_ROLES = List.of("ACTOR1", "ACTOR2");
	private static final List<String> ALL_LOCATION_ROLES =
			List.of(ACTION_ROLE, "ACTOR1", "ACTOR2");

	static final String REGIONS_AGGREGATION = "regions";
	static final String TONES_AGGREGATION = "tones";
	static final String UNLOCATED_AGGREGATION = "unlocated";
	static final String UNMAPPED_AGGREGATION = "unmapped";
	static final String REASONS_AGGREGATION = "reasons";
	static final String NEGATIVE_EXTREME_TONE_BUCKET = "NEGATIVE_EXTREME";
	static final String NEGATIVE_STRONG_TONE_BUCKET = "NEGATIVE_STRONG";
	static final String NEGATIVE_MILD_TONE_BUCKET = "NEGATIVE_MILD";
	static final String ZERO_TONE_BUCKET = "ZERO";
	static final String POSITIVE_MILD_TONE_BUCKET = "POSITIVE_MILD";
	static final String POSITIVE_STRONG_TONE_BUCKET = "POSITIVE_STRONG";
	static final String POSITIVE_EXTREME_TONE_BUCKET = "POSITIVE_EXTREME";
	static final String MISSING_TONE_BUCKET = "MISSING";

	private static final Set<String> ROOT_AGGREGATIONS = Set.of(
			REGIONS_AGGREGATION,
			UNLOCATED_AGGREGATION,
			UNMAPPED_AGGREGATION);
	private static final Set<String> TONE_BUCKETS = Set.of(
			NEGATIVE_EXTREME_TONE_BUCKET,
			NEGATIVE_STRONG_TONE_BUCKET,
			NEGATIVE_MILD_TONE_BUCKET,
			ZERO_TONE_BUCKET,
			POSITIVE_MILD_TONE_BUCKET,
			POSITIVE_STRONG_TONE_BUCKET,
			POSITIVE_EXTREME_TONE_BUCKET,
			MISSING_TONE_BUCKET);

	private final ElasticsearchClient elasticsearchClient;
	private final IngestionCoverageQuery coverageQuery;
	private final CountryGeometryCatalog catalog;
	private final CountrySnapshotWindowPolicy windowPolicy;
	private final CountryMapSnapshotAssembler assembler;
	private final SearchMetrics metrics;

	/**
	 * Создает read-only query поверх стабильного Event alias.
	 *
	 * @param elasticsearchClient официальный Elasticsearch client
	 * @param coverageQuery проверка ожидаемых Event-интервалов
	 * @param catalogLoader проверенный runtime-каталог опубликованной геометрии
	 * @param windowPolicy правило выбора закрытого 24-часового окна
	 * @param assembler сборщик плотной модели и проверка ее равенств
	 * @param metrics ограниченные метрики поисковых запросов
	 */
	public ElasticsearchCountryMapSnapshotQuery(
			ElasticsearchClient elasticsearchClient,
			IngestionCoverageQuery coverageQuery,
			CountryGeometryCatalogLoader catalogLoader,
			CountrySnapshotWindowPolicy windowPolicy,
			CountryMapSnapshotAssembler assembler,
			SearchMetrics metrics
	) {
		this.elasticsearchClient = Objects.requireNonNull(
				elasticsearchClient, "elasticsearchClient must not be null");
		this.coverageQuery = Objects.requireNonNull(
				coverageQuery, "coverageQuery must not be null");
		this.catalog = Objects.requireNonNull(
				catalogLoader, "catalogLoader must not be null").catalog();
		this.windowPolicy = Objects.requireNonNull(
				windowPolicy, "windowPolicy must not be null");
		this.assembler = Objects.requireNonNull(assembler, "assembler must not be null");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	@Override
	public CountryMapSnapshot read() {
		Timer.Sample timer = metrics.startCountrySnapshotTimer();
		SearchSnapshotMetricOutcome metricOutcome = SearchSnapshotMetricOutcome.UNAVAILABLE;
		try {
			CountrySnapshotWindowPolicy.Window window = windowPolicy.currentWindow();
			IngestionCoverageEvidence coverage = readCoverage(window);
			SearchResponse<Void> response = elasticsearchClient.search(
					buildRequest(window, catalog));
			CountryMapSnapshot snapshot = assembler.assemble(
					window,
					coverage,
					readAggregation(response, catalog));
			metricOutcome = metricOutcome(snapshot);
			return snapshot;
		} catch (ElasticsearchException exception) {
			throw new SearchAccessException(exception);
		} catch (IOException | JsonpMappingException exception) {
			throw new SearchAccessException(exception);
		} finally {
			metrics.countrySnapshotDuration(timer, metricOutcome);
		}
	}

	private IngestionCoverageEvidence readCoverage(
			CountrySnapshotWindowPolicy.Window window
	) {
		try {
			return coverageQuery.read(window.from(), window.to());
		} catch (IngestionCoverageUnavailableException exception) {
			return IngestionCoverageEvidence.unknown();
		}
	}

	static SearchRequest buildRequest(
			CountrySnapshotWindowPolicy.Window window,
			CountryGeometryCatalog catalog
	) {
		Objects.requireNonNull(window, "window must not be null");
		Objects.requireNonNull(catalog, "catalog must not be null");
		return new SearchRequest.Builder()
				.index(GdeltIndexKind.EVENT.readAlias())
				.size(0)
				.allowNoIndices(false)
				.ignoreUnavailable(false)
				.allowPartialSearchResults(false)
				.source(source -> source.fetch(false))
				.trackTotalHits(total -> total.enabled(true))
				.query(dateWindow(window))
				.aggregations(REGIONS_AGGREGATION, regionsAggregation(catalog))
				.aggregations(
						UNLOCATED_AGGREGATION,
						categoryAggregation(
								unlocatedQuery(),
								unlocatedReasonFilters()))
				.aggregations(
						UNMAPPED_AGGREGATION,
						categoryAggregation(
								unmappedQuery(catalog),
								unmappedReasonFilters(catalog)))
				.build();
	}

	private static Aggregation regionsAggregation(CountryGeometryCatalog catalog) {
		Map<String, Query> regionFilters = new LinkedHashMap<>();
		for (CountryGeometryCatalog.Region region : catalog.regions()) {
			regionFilters.put(
					region.regionId(),
					region.gdeltCountryCodes().isEmpty()
							? matchNone()
							: and(actionQuery(), terms(COUNTRY_CODE_FIELD, region.gdeltCountryCodes())));
		}
		return Aggregation.of(aggregation -> aggregation
				.filters(filters -> filters
						.filters(buckets -> buckets.keyed(regionFilters))
						.keyed(true))
				.aggregations(TONES_AGGREGATION, keyedFilters(toneFilters())));
	}

	private static Aggregation categoryAggregation(
			Query category,
			Map<String, Query> reasonFilters
	) {
		return Aggregation.of(aggregation -> aggregation
				.filter(category)
				.aggregations(REASONS_AGGREGATION, keyedFilters(reasonFilters)));
	}

	private static Aggregation keyedFilters(Map<String, Query> filters) {
		return Aggregation.of(aggregation -> aggregation.filters(builder -> builder
				.filters(buckets -> buckets.keyed(filters))
				.keyed(true)));
	}

	private static Map<String, Query> toneFilters() {
		Map<String, Query> filters = new LinkedHashMap<>();
		filters.put(NEGATIVE_EXTREME_TONE_BUCKET, atMostNumberRange(-8.0));
		filters.put(
				NEGATIVE_STRONG_TONE_BUCKET,
				greaterThanAtMostNumberRange(-8.0, -3.0));
		filters.put(NEGATIVE_MILD_TONE_BUCKET, openNumberRange(-3.0, -0.0));
		filters.put(ZERO_TONE_BUCKET, zeroToneRange());
		filters.put(POSITIVE_MILD_TONE_BUCKET, openNumberRange(0.0, 3.0));
		filters.put(
				POSITIVE_STRONG_TONE_BUCKET,
				atLeastLessThanNumberRange(3.0, 8.0));
		filters.put(POSITIVE_EXTREME_TONE_BUCKET, atLeastNumberRange(8.0));
		filters.put(MISSING_TONE_BUCKET, not(exists(AVERAGE_TONE_FIELD)));
		return filters;
	}

	private static Map<String, Query> unlocatedReasonFilters() {
		Map<String, Query> filters = new LinkedHashMap<>();
		filters.put(
				UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID.name(),
				not(exists(LOCATION_ROLE_FIELD)));
		filters.put(
				UnlocatedReason.ACTOR_FALLBACK.name(),
				terms(LOCATION_ROLE_FIELD, ACTOR_ROLES));
		filters.put(
				UnlocatedReason.OTHER.name(),
				and(
						exists(LOCATION_ROLE_FIELD),
						not(terms(LOCATION_ROLE_FIELD, ALL_LOCATION_ROLES))));
		return filters;
	}

	private static Map<String, Query> unmappedReasonFilters(
			CountryGeometryCatalog catalog
	) {
		Map<String, Query> filters = new LinkedHashMap<>();
		Query category = unmappedQuery(catalog);
		filters.put(
				UnmappedReason.COUNTRY_CODE_MISSING.name(),
				and(category, missingOrEmptyCountryCode()));
		filters.put(
				UnmappedReason.UNKNOWN_COUNTRY_CODE.name(),
				and(
						category,
						exists(COUNTRY_CODE_FIELD),
						not(term(COUNTRY_CODE_FIELD, FieldValue.of(""))),
						not(terms(
								COUNTRY_CODE_FIELD,
								catalog.knownGdeltCountryCodes().stream().sorted().toList()))));
		filters.put(
				UnmappedReason.NO_REGION_GEOMETRY.name(),
				and(category, terms(
						COUNTRY_CODE_FIELD,
						explicitCodes(catalog, UnmappedReason.NO_REGION_GEOMETRY))));
		filters.put(
				UnmappedReason.AMBIGUOUS_REGION_MAPPING.name(),
				and(category, terms(
						COUNTRY_CODE_FIELD,
						explicitCodes(catalog, UnmappedReason.AMBIGUOUS_REGION_MAPPING))));
		filters.put(
				UnmappedReason.UNSUPPORTED_COUNTRY_CODE.name(),
				and(category, terms(
						COUNTRY_CODE_FIELD,
						explicitCodes(catalog, UnmappedReason.UNSUPPORTED_COUNTRY_CODE))));
		filters.put(UnmappedReason.OTHER.name(), matchNone());
		return filters;
	}

	private static List<String> explicitCodes(
			CountryGeometryCatalog catalog,
			UnmappedReason reason
	) {
		return catalog.unmappedReasonByGdeltCountryCode().entrySet().stream()
				.filter(entry -> publicReason(entry.getValue()) == reason)
				.map(Map.Entry::getKey)
				.sorted()
				.toList();
	}

	private static UnmappedReason publicReason(
			CountryGeometryCatalog.UnmappedReason reason
	) {
		return switch (reason) {
			case NO_TERRITORIAL_GEOMETRY,
					SOURCE_GEOMETRY_NOT_INDEPENDENT -> UnmappedReason.NO_REGION_GEOMETRY;
			case AMBIGUOUS_MULTIPLE_REGIONS -> UnmappedReason.AMBIGUOUS_REGION_MAPPING;
			case UNSUPPORTED_NON_COUNTRY_CODE -> UnmappedReason.UNSUPPORTED_COUNTRY_CODE;
		};
	}

	private static Query dateWindow(CountrySnapshotWindowPolicy.Window window) {
		return Query.of(query -> query.range(range -> range.date(date -> date
				.field(DATE_ADDED_FIELD)
				.gte(window.from().toString())
				.lt(window.to().toString()))));
	}

	private static Query unlocatedQuery() {
		return not(actionQuery());
	}

	private static Query unmappedQuery(CountryGeometryCatalog catalog) {
		return and(
				actionQuery(),
				not(terms(
						COUNTRY_CODE_FIELD,
						catalog.regionIdByGdeltCountryCode().keySet().stream()
								.sorted()
								.toList())));
	}

	private static Query missingOrEmptyCountryCode() {
		return Query.of(query -> query.bool(bool -> bool
				.should(not(exists(COUNTRY_CODE_FIELD)))
				.should(term(COUNTRY_CODE_FIELD, FieldValue.of("")))
				.minimumShouldMatch("1")));
	}

	private static Query actionQuery() {
		return term(LOCATION_ROLE_FIELD, FieldValue.of(ACTION_ROLE));
	}

	private static Query term(String field, FieldValue value) {
		return Query.of(query -> query.term(term -> term.field(field).value(value)));
	}

	private static Query terms(String field, List<String> values) {
		if (values.isEmpty()) {
			return matchNone();
		}
		return Query.of(query -> query.terms(terms -> terms
				.field(field)
				.terms(termsField -> termsField.value(values.stream()
						.map(FieldValue::of)
						.toList()))));
	}

	private static Query exists(String field) {
		return Query.of(query -> query.exists(exists -> exists.field(field)));
	}

	private static Query atMostNumberRange(double upperInclusive) {
		return Query.of(query -> query.range(range -> range.number(number -> number
				.field(AVERAGE_TONE_FIELD)
				.lte(upperInclusive))));
	}

	private static Query greaterThanAtMostNumberRange(
			double lowerExclusive,
			double upperInclusive
	) {
		return Query.of(query -> query.range(range -> range.number(number -> number
				.field(AVERAGE_TONE_FIELD)
				.gt(lowerExclusive)
				.lte(upperInclusive))));
	}

	private static Query openNumberRange(double lowerExclusive, double upperExclusive) {
		return Query.of(query -> query.range(range -> range.number(number -> {
			number.field(AVERAGE_TONE_FIELD);
			number.gt(lowerExclusive);
			number.lt(upperExclusive);
			return number;
		})));
	}

	private static Query atLeastLessThanNumberRange(
			double lowerInclusive,
			double upperExclusive
	) {
		return Query.of(query -> query.range(range -> range.number(number -> number
				.field(AVERAGE_TONE_FIELD)
				.gte(lowerInclusive)
				.lt(upperExclusive))));
	}

	private static Query atLeastNumberRange(double lowerInclusive) {
		return Query.of(query -> query.range(range -> range.number(number -> number
				.field(AVERAGE_TONE_FIELD)
				.gte(lowerInclusive))));
	}

	private static Query zeroToneRange() {
		return Query.of(query -> query.range(range -> range.number(number -> number
				.field(AVERAGE_TONE_FIELD)
				.gte(-0.0)
				.lte(0.0))));
	}

	private static Query and(Query... clauses) {
		return Query.of(query -> query.bool(bool -> bool.filter(List.of(clauses))));
	}

	private static Query not(Query clause) {
		return Query.of(query -> query.bool(bool -> bool.mustNot(clause)));
	}

	private static Query matchNone() {
		return Query.of(query -> query.matchNone(matchNone -> matchNone));
	}

	private static CountryMapSnapshotAssembler.Aggregation readAggregation(
			SearchResponse<Void> response,
			CountryGeometryCatalog catalog
	) {
		requireComplete(response);
		long eligibleEventCount = requireExactTotal(response);
		Map<String, Aggregate> root = response.aggregations();
		if (!root.keySet().equals(ROOT_AGGREGATIONS)) {
			throw invalidResponse("Elasticsearch snapshot has unexpected aggregations");
		}

		Set<String> regionIds = new LinkedHashSet<>();
		catalog.regions().forEach(region -> regionIds.add(region.regionId()));
		Map<String, FiltersBucket> regionBuckets = requireKeyedFilters(
				root.get(REGIONS_AGGREGATION), regionIds, REGIONS_AGGREGATION);
		Map<String, CountryMapSnapshotAssembler.RegionCounts> regions =
				new LinkedHashMap<>();
		long mappedEventCount = 0;
		for (CountryGeometryCatalog.Region region : catalog.regions()) {
			FiltersBucket bucket = regionBuckets.get(region.regionId());
			Map<String, Aggregate> nested = bucket.aggregations();
			if (!nested.keySet().equals(Set.of(TONES_AGGREGATION))) {
				throw invalidResponse("Elasticsearch region bucket has unexpected aggregations");
			}
			Map<String, FiltersBucket> tones = requireKeyedFilters(
					nested.get(TONES_AGGREGATION), TONE_BUCKETS, TONES_AGGREGATION);
			long eventCount = requireCount(bucket.docCount(), "region");
			mappedEventCount = addCount(mappedEventCount, eventCount);
			regions.put(
					region.regionId(),
					new CountryMapSnapshotAssembler.RegionCounts(
							eventCount,
							requireCount(
									tones.get(NEGATIVE_EXTREME_TONE_BUCKET).docCount(),
									"negative extreme tone"),
							requireCount(
									tones.get(NEGATIVE_STRONG_TONE_BUCKET).docCount(),
									"negative strong tone"),
							requireCount(
									tones.get(NEGATIVE_MILD_TONE_BUCKET).docCount(),
									"negative mild tone"),
							requireCount(tones.get(ZERO_TONE_BUCKET).docCount(), "zero tone"),
							requireCount(
									tones.get(POSITIVE_MILD_TONE_BUCKET).docCount(),
									"positive mild tone"),
							requireCount(
									tones.get(POSITIVE_STRONG_TONE_BUCKET).docCount(),
									"positive strong tone"),
							requireCount(
									tones.get(POSITIVE_EXTREME_TONE_BUCKET).docCount(),
									"positive extreme tone"),
							requireCount(tones.get(MISSING_TONE_BUCKET).docCount(), "missing tone")));
		}

		CategoryCounts<UnlocatedReason> unlocated = readCategory(
				root.get(UNLOCATED_AGGREGATION),
				UnlocatedReason.class,
				UNLOCATED_AGGREGATION);
		CategoryCounts<UnmappedReason> unmapped = readCategory(
				root.get(UNMAPPED_AGGREGATION),
				UnmappedReason.class,
				UNMAPPED_AGGREGATION);
		return new CountryMapSnapshotAssembler.Aggregation(
				eligibleEventCount,
				mappedEventCount,
				unlocated.total(),
				unmapped.total(),
				regions,
				unlocated.counts(),
				unmapped.counts());
	}

	private static <E extends Enum<E>> CategoryCounts<E> readCategory(
			Aggregate aggregate,
			Class<E> reasonType,
			String aggregationName
	) {
		if (aggregate == null || !aggregate.isFilter()) {
			throw invalidResponse("Elasticsearch " + aggregationName + " is not a filter");
		}
		var category = aggregate.filter();
		if (!category.aggregations().keySet().equals(Set.of(REASONS_AGGREGATION))) {
			throw invalidResponse("Elasticsearch category has unexpected aggregations");
		}
		Set<String> expected = new LinkedHashSet<>();
		for (E reason : reasonType.getEnumConstants()) {
			expected.add(reason.name());
		}
		Map<String, FiltersBucket> buckets = requireKeyedFilters(
				category.aggregations().get(REASONS_AGGREGATION),
				expected,
				aggregationName + " reasons");
		Map<E, Long> counts = new EnumMap<>(reasonType);
		for (E reason : reasonType.getEnumConstants()) {
			counts.put(
					reason,
					requireCount(buckets.get(reason.name()).docCount(), aggregationName));
		}
		return new CategoryCounts<>(
				requireCount(category.docCount(), aggregationName),
				counts);
	}

	private static Map<String, FiltersBucket> requireKeyedFilters(
			Aggregate aggregate,
			Set<String> expectedKeys,
			String aggregationName
	) {
		if (aggregate == null || !aggregate.isFilters()) {
			throw invalidResponse(
					"Elasticsearch " + aggregationName + " is not keyed filters");
		}
		var buckets = aggregate.filters().buckets();
		if (!buckets.isKeyed() || !buckets.keyed().keySet().equals(expectedKeys)) {
			throw invalidResponse(
					"Elasticsearch " + aggregationName + " has inconsistent buckets");
		}
		return buckets.keyed();
	}

	private static void requireComplete(SearchResponse<?> response) {
		if (response == null) {
			throw invalidResponse("Elasticsearch snapshot has no response");
		}
		ShardStatistics shards = response.shards();
		if (response.timedOut()
				|| Boolean.TRUE.equals(response.terminatedEarly())
				|| shards == null
				|| shards.failed().longValue() > 0) {
			throw invalidResponse("Elasticsearch snapshot response is partial");
		}
	}

	private static long requireExactTotal(SearchResponse<?> response) {
		TotalHits total = response.hits().total();
		if (total == null
				|| total.relation() != TotalHitsRelation.Eq
				|| !response.hits().hits().isEmpty()) {
			throw invalidResponse("Elasticsearch snapshot has inconsistent total hits");
		}
		return requireCount(total.value(), "eligible events");
	}

	private static long requireCount(long count, String label) {
		if (count < 0) {
			throw invalidResponse("Elasticsearch " + label + " count is negative");
		}
		return count;
	}

	private static long addCount(long left, long right) {
		try {
			return Math.addExact(left, right);
		} catch (ArithmeticException exception) {
			throw new SearchAccessException(exception);
		}
	}

	private static SearchSnapshotMetricOutcome metricOutcome(
			CountryMapSnapshot snapshot
	) {
		return switch (snapshot.coverage().status()) {
			case COMPLETE -> SearchSnapshotMetricOutcome.COMPLETE;
			case PARTIAL -> SearchSnapshotMetricOutcome.PARTIAL;
			case UNKNOWN -> SearchSnapshotMetricOutcome.UNKNOWN;
		};
	}

	private static SearchAccessException invalidResponse(String message) {
		return new SearchAccessException(new IllegalStateException(message));
	}

	private record CategoryCounts<E extends Enum<E>>(
			long total,
			Map<E, Long> counts
	) {

		private CategoryCounts {
			counts = Map.copyOf(counts);
		}
	}
}
