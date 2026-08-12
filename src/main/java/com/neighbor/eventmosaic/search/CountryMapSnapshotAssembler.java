package com.neighbor.eventmosaic.search;

import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageInterval;
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
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Собирает публичную поисковую модель из одного ответа Elasticsearch и уже
 * прочитанной оценки полноты. Любой пропущенный bucket, неизвестный регион или
 * нарушенное равенство отклоняет весь снимок вместо частичного результата.
 */
@Component
final class CountryMapSnapshotAssembler {

	private final CountryGeometryCatalog catalog;

	@Autowired
	CountryMapSnapshotAssembler(CountryGeometryCatalogLoader catalogLoader) {
		this(Objects.requireNonNull(
				catalogLoader,
				"catalogLoader must not be null").catalog());
	}

	private CountryMapSnapshotAssembler(CountryGeometryCatalog catalog) {
		this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
	}

	static CountryMapSnapshotAssembler forCatalog(CountryGeometryCatalog catalog) {
		return new CountryMapSnapshotAssembler(catalog);
	}

	CountryMapSnapshot assemble(
			CountrySnapshotWindowPolicy.Window window,
			IngestionCoverageEvidence coverageEvidence,
			Aggregation aggregation
	) {
		try {
			return assembleChecked(window, coverageEvidence, aggregation);
		}
		catch (IllegalArgumentException | IllegalStateException | NullPointerException exception) {
			throw new SearchAccessException(exception);
		}
	}

	private CountryMapSnapshot assembleChecked(
			CountrySnapshotWindowPolicy.Window window,
			IngestionCoverageEvidence coverageEvidence,
			Aggregation aggregation
	) {
		Objects.requireNonNull(window, "window must not be null");
		Objects.requireNonNull(coverageEvidence, "coverageEvidence must not be null");
		Objects.requireNonNull(aggregation, "aggregation must not be null");

		List<Region> regions = assembleRegions(aggregation.regions());
		Quality quality = new Quality(
				aggregation.eligible(),
				aggregation.mapped(),
				aggregation.unlocated(),
				aggregation.unmapped(),
				assembleUnlocatedReasons(aggregation.unlocatedReasons()),
				assembleUnmappedReasons(aggregation.unmappedReasons()));
		return new CountryMapSnapshot(
				window.from(),
				window.to(),
				catalog.geometryVersion(),
				CountryMapSnapshot.TONE_MODEL_VERSION,
				assembleCoverage(coverageEvidence),
				quality,
				regions);
	}

	private List<Region> assembleRegions(Map<String, RegionCounts> regionCounts) {
		Objects.requireNonNull(regionCounts, "regions must not be null");
		Set<String> expectedRegionIds = new HashSet<>();
		for (CountryGeometryCatalog.Region region : catalog.regions()) {
			expectedRegionIds.add(region.regionId());
		}
		if (!regionCounts.keySet().equals(expectedRegionIds)) {
			throw new IllegalStateException(
					"Elasticsearch response does not contain the exact region roster");
		}

		var assembled = new ArrayList<Region>(catalog.regions().size());
		for (CountryGeometryCatalog.Region catalogRegion : catalog.regions()) {
			RegionCounts counts = regionCounts.get(catalogRegion.regionId());
			if (counts == null) {
				throw new IllegalStateException(
						"Elasticsearch response has no counts for a catalog region");
			}
			ToneCounts toneCounts = new ToneCounts(
					counts.negativeExtreme(),
					counts.negativeStrong(),
					counts.negativeMild(),
					counts.zero(),
					counts.positiveMild(),
					counts.positiveStrong(),
					counts.positiveExtreme());
			long colored = addCounts(
					"colored region count",
					counts.negativeExtreme(),
					counts.negativeStrong(),
					counts.negativeMild(),
					counts.zero(),
					counts.positiveMild(),
					counts.positiveStrong(),
					counts.positiveExtreme());
			assembled.add(new Region(
					catalogRegion.regionId(),
					counts.event(),
					colored,
					counts.missingTone(),
					toneCounts));
		}
		return List.copyOf(assembled);
	}

	private static List<UnlocatedReasonCount> assembleUnlocatedReasons(
			Map<UnlocatedReason, Long> reasonCounts
	) {
		requireExactReasons(
				reasonCounts,
				EnumSet.allOf(UnlocatedReason.class),
				"unlocated");
		var assembled = new ArrayList<UnlocatedReasonCount>(
				UnlocatedReason.values().length);
		for (UnlocatedReason reason : UnlocatedReason.values()) {
			assembled.add(new UnlocatedReasonCount(
					reason,
					requireReasonCount(reasonCounts, reason, "unlocated")));
		}
		return List.copyOf(assembled);
	}

	private static List<UnmappedReasonCount> assembleUnmappedReasons(
			Map<UnmappedReason, Long> reasonCounts
	) {
		requireExactReasons(
				reasonCounts,
				EnumSet.allOf(UnmappedReason.class),
				"unmapped");
		var assembled = new ArrayList<UnmappedReasonCount>(
				UnmappedReason.values().length);
		for (UnmappedReason reason : UnmappedReason.values()) {
			assembled.add(new UnmappedReasonCount(
					reason,
					requireReasonCount(reasonCounts, reason, "unmapped")));
		}
		return List.copyOf(assembled);
	}

	private static <E extends Enum<E>> void requireExactReasons(
			Map<E, Long> reasonCounts,
			Set<E> expectedReasons,
			String category
	) {
		Objects.requireNonNull(reasonCounts, category + " reasons must not be null");
		if (!reasonCounts.keySet().equals(expectedReasons)) {
			throw new IllegalStateException(
					"Elasticsearch response does not contain every " + category + " reason");
		}
	}

	private static <E extends Enum<E>> long requireReasonCount(
			Map<E, Long> reasonCounts,
			E reason,
			String category
	) {
		Long count = reasonCounts.get(reason);
		if (count == null) {
			throw new IllegalStateException(
					"Elasticsearch response has no " + category + " reason count");
		}
		return count;
	}

	private static Coverage assembleCoverage(IngestionCoverageEvidence evidence) {
		CoverageStatus status = switch (evidence.status()) {
			case COMPLETE -> CoverageStatus.COMPLETE;
			case PARTIAL -> CoverageStatus.PARTIAL;
			case UNKNOWN -> CoverageStatus.UNKNOWN;
		};
		List<MissingInterval> missingIntervals = evidence.missingIntervals() == null
				? null
				: evidence.missingIntervals().stream()
						.map(CountryMapSnapshotAssembler::toMissingInterval)
						.toList();
		return new Coverage(status, missingIntervals);
	}

	private static MissingInterval toMissingInterval(IngestionCoverageInterval interval) {
		Objects.requireNonNull(interval, "coverage interval must not be null");
		return new MissingInterval(interval.from(), interval.to());
	}

	private static long addCounts(String label, long... counts) {
		long total = 0;
		try {
			for (long count : counts) {
				total = Math.addExact(total, count);
			}
			return total;
		}
		catch (ArithmeticException exception) {
			throw new IllegalStateException(label + " does not fit in a 64-bit count", exception);
		}
	}

	/** Один разобранный ответ всех агрегаций country snapshot. */
	record Aggregation(
			long eligible,
			long mapped,
			long unlocated,
			long unmapped,
			Map<String, RegionCounts> regions,
			Map<UnlocatedReason, Long> unlocatedReasons,
			Map<UnmappedReason, Long> unmappedReasons
	) {
	}

	/** Сырые счетчики одного region bucket и его tone subfilters. */
	record RegionCounts(
			long event,
			long negativeExtreme,
			long negativeStrong,
			long negativeMild,
			long zero,
			long positiveMild,
			long positiveStrong,
			long positiveExtreme,
			long missingTone
	) {
	}
}
