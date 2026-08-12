package com.neighbor.eventmosaic.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageEvidence;
import com.neighbor.eventmosaic.ingestion.api.IngestionCoverageInterval;
import com.neighbor.eventmosaic.search.CountryMapSnapshotAssembler.Aggregation;
import com.neighbor.eventmosaic.search.CountryMapSnapshotAssembler.RegionCounts;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Coverage;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.CoverageStatus;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.MissingInterval;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.Tone;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnlocatedReason;
import com.neighbor.eventmosaic.search.api.CountryMapSnapshot.UnmappedReason;
import com.neighbor.eventmosaic.search.api.SearchAccessException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Сборка плотной модели снимка карты стран")
class CountryMapSnapshotAssemblerTest {

	private static final Instant FROM = Instant.parse("2026-08-10T12:15:00Z");
	private static final Instant TO = FROM.plus(Duration.ofHours(24));
	private static final CountrySnapshotWindowPolicy.Window WINDOW =
			new CountrySnapshotWindowPolicy.Window(FROM, TO);

	private final CountryMapSnapshotAssembler assembler =
			CountryMapSnapshotAssembler.forCatalog(catalog());

	@Test
	@DisplayName("Сохраняет порядок каталога, нулевые регионы и все равенства счетчиков")
	void assemblesCanonicalDenseSnapshot() {
		IngestionCoverageInterval missing = new IngestionCoverageInterval(
				FROM.plus(Duration.ofMinutes(15)),
				FROM.plus(Duration.ofMinutes(45)));

		CountryMapSnapshot snapshot = assembler.assemble(
				WINDOW,
				IngestionCoverageEvidence.partial(List.of(missing)),
				populatedAggregation());

		assertThat(snapshot.from()).isEqualTo(FROM);
		assertThat(snapshot.to()).isEqualTo(TO);
		assertThat(snapshot.geometryVersion()).isEqualTo("country-v1");
		assertThat(snapshot.toneModelVersion())
				.isEqualTo(CountryMapSnapshot.TONE_MODEL_VERSION);
		assertThat(snapshot.regions())
				.extracting(CountryMapSnapshot.Region::regionId)
				.containsExactly("country:aaa", "country:bbb", "country:ccc");

		CountryMapSnapshot.Region first = snapshot.regions().getFirst();
		assertThat(first.eventCount()).isEqualTo(2);
		assertThat(first.coloredEventCount()).isEqualTo(2);
		assertThat(first.missingToneEventCount()).isZero();
		assertThat(first.toneCounts().count(Tone.NEGATIVE_EXTREME)).isEqualTo(1);
		assertThat(first.toneCounts().count(Tone.NEGATIVE_STRONG)).isZero();
		assertThat(first.toneCounts().count(Tone.NEGATIVE_MILD)).isZero();
		assertThat(first.toneCounts().count(Tone.ZERO)).isZero();
		assertThat(first.toneCounts().count(Tone.POSITIVE_MILD)).isZero();
		assertThat(first.toneCounts().count(Tone.POSITIVE_STRONG)).isZero();
		assertThat(first.toneCounts().count(Tone.POSITIVE_EXTREME)).isEqualTo(1);

		CountryMapSnapshot.Region second = snapshot.regions().get(1);
		assertThat(second.eventCount()).isEqualTo(1);
		assertThat(second.coloredEventCount()).isZero();
		assertThat(second.missingToneEventCount()).isEqualTo(1);
		assertThat(snapshot.regions().getLast().eventCount()).isZero();

		assertThat(snapshot.quality().eligibleEventCount()).isEqualTo(7);
		assertThat(snapshot.quality().mappedEventCount()).isEqualTo(3);
		assertThat(snapshot.quality().unlocatedEventCount()).isEqualTo(2);
		assertThat(snapshot.quality().unmappedEventCount()).isEqualTo(2);
		assertThat(snapshot.quality().unlocatedReasonCounts())
				.extracting(CountryMapSnapshot.UnlocatedReasonCount::reason)
				.containsExactly(UnlocatedReason.values());
		assertThat(snapshot.quality().unmappedReasonCounts())
				.extracting(CountryMapSnapshot.UnmappedReasonCount::reason)
				.containsExactly(UnmappedReason.values());
		assertThat(snapshot.coverage().status()).isEqualTo(CoverageStatus.PARTIAL);
		assertThat(snapshot.coverage().missingIntervals())
				.containsExactly(new MissingInterval(missing.from(), missing.to()));
	}

	@Test
	@DisplayName("Пустая поисковая модель сохраняет все регионы и различает COMPLETE от UNKNOWN")
	void keepsZeroRosterAndCoverageShape() {
		CountryMapSnapshot complete = assembler.assemble(
				WINDOW,
				IngestionCoverageEvidence.complete(),
				zeroAggregation());
		CountryMapSnapshot unknown = assembler.assemble(
				WINDOW,
				IngestionCoverageEvidence.unknown(),
				zeroAggregation());

		assertThat(complete.regions()).hasSize(3);
		assertThat(complete.regions())
				.allSatisfy(region -> {
					assertThat(region.eventCount()).isZero();
					assertThat(region.coloredEventCount()).isZero();
					assertThat(region.missingToneEventCount()).isZero();
					for (Tone tone : Tone.values()) {
						assertThat(region.toneCounts().count(tone)).isZero();
					}
				});
		assertThat(complete.coverage().status()).isEqualTo(CoverageStatus.COMPLETE);
		assertThat(complete.coverage().missingIntervals()).isEmpty();
		assertThat(unknown.coverage().status()).isEqualTo(CoverageStatus.UNKNOWN);
		assertThat(unknown.coverage().missingIntervals()).isNull();
	}

	@Test
	@DisplayName("Публичная модель копирует все переданные списки и не отдает изменяемые коллекции")
	void publicModelDefensivelyCopiesCollections() {
		CountryMapSnapshot source = assembler.assemble(
				WINDOW,
				IngestionCoverageEvidence.complete(),
				zeroAggregation());
		List<CountryMapSnapshot.Region> mutableRegions = new ArrayList<>(source.regions());
		List<MissingInterval> mutableIntervals = new ArrayList<>(List.of(
				new MissingInterval(FROM, FROM.plus(Duration.ofMinutes(15)))));
		Coverage copiedCoverage = new Coverage(CoverageStatus.PARTIAL, mutableIntervals);
		CountryMapSnapshot copied = new CountryMapSnapshot(
				source.from(),
				source.to(),
				source.geometryVersion(),
				source.toneModelVersion(),
				copiedCoverage,
				source.quality(),
				mutableRegions);

		mutableRegions.clear();
		mutableIntervals.clear();

		assertThat(copied.regions()).hasSize(3);
		assertThat(copied.coverage().missingIntervals()).hasSize(1);
		assertThatThrownBy(() -> copied.regions().clear())
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> copied.coverage().missingIntervals().clear())
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> copied.quality().unmappedReasonCounts().clear())
				.isInstanceOf(UnsupportedOperationException.class);
	}

	@Test
	@DisplayName("Отклоняет пропущенный или посторонний счетчик региона")
	void rejectsInexactRegionRoster() {
		Aggregation valid = populatedAggregation();
		Map<String, RegionCounts> missing = new HashMap<>(valid.regions());
		missing.remove("country:bbb");
		Map<String, RegionCounts> extra = new HashMap<>(valid.regions());
		extra.put("country:zzz", zeroRegion());

		assertSearchFailure(withRegions(valid, missing));
		assertSearchFailure(withRegions(valid, extra));
	}

	@Test
	@DisplayName("Отклоняет отрицательные, переполненные и несогласованные счетчики")
	void rejectsInvalidCounts() {
		Aggregation valid = populatedAggregation();

		Map<String, RegionCounts> inconsistentRegion = new HashMap<>(valid.regions());
		inconsistentRegion.put(
				"country:aaa",
				new RegionCounts(2, 1, 0, 0, 0, 0, 0, 0, 0));
		assertSearchFailure(withRegions(valid, inconsistentRegion));

		Map<String, RegionCounts> negativeRegion = new HashMap<>(valid.regions());
		negativeRegion.put(
				"country:aaa",
				new RegionCounts(0, -1, 0, 0, 0, 0, 0, 0, 1));
		assertSearchFailure(withRegions(valid, negativeRegion));

		Map<String, RegionCounts> overflowRegion = new HashMap<>(valid.regions());
		overflowRegion.put(
				"country:aaa",
				new RegionCounts(
						Long.MAX_VALUE,
						Long.MAX_VALUE,
						0,
						0,
						0,
						0,
						0,
						1,
						0));
		assertSearchFailure(withRegions(valid, overflowRegion));

		assertSearchFailure(new Aggregation(
				8,
				valid.mapped(),
				valid.unlocated(),
				valid.unmapped(),
				valid.regions(),
				valid.unlocatedReasons(),
				valid.unmappedReasons()));
	}

	@Test
	@DisplayName("Отклоняет неполный набор причин и пропуск полноты за границей окна")
	void rejectsStructuralReasonAndCoverageInconsistency() {
		Aggregation valid = populatedAggregation();
		Map<UnmappedReason, Long> missingReason = new EnumMap<>(
				valid.unmappedReasons());
		missingReason.remove(UnmappedReason.OTHER);
		assertSearchFailure(new Aggregation(
				valid.eligible(),
				valid.mapped(),
				valid.unlocated(),
				valid.unmapped(),
				valid.regions(),
				valid.unlocatedReasons(),
				missingReason));

		IngestionCoverageEvidence outsideWindow = IngestionCoverageEvidence.partial(
				List.of(new IngestionCoverageInterval(
						FROM.minus(Duration.ofMinutes(15)),
						FROM)));
		assertThatThrownBy(() -> assembler.assemble(
				WINDOW,
				outsideWindow,
				valid))
				.isInstanceOf(SearchAccessException.class);
	}

	private void assertSearchFailure(Aggregation aggregation) {
		assertThatThrownBy(() -> assembler.assemble(
				WINDOW,
				IngestionCoverageEvidence.complete(),
				aggregation))
				.isInstanceOf(SearchAccessException.class)
				.hasCauseInstanceOf(RuntimeException.class);
	}

	private static Aggregation populatedAggregation() {
		Map<String, RegionCounts> regions = new LinkedHashMap<>();
		regions.put("country:ccc", zeroRegion());
		regions.put("country:bbb", new RegionCounts(1, 0, 0, 0, 0, 0, 0, 0, 1));
		regions.put("country:aaa", new RegionCounts(2, 1, 0, 0, 0, 0, 0, 1, 0));
		return new Aggregation(
				7,
				3,
				2,
				2,
				regions,
				Map.of(
						UnlocatedReason.ACTION_GEO_MISSING_OR_INVALID, 1L,
						UnlocatedReason.ACTOR_FALLBACK, 1L,
						UnlocatedReason.OTHER, 0L),
				Map.of(
						UnmappedReason.COUNTRY_CODE_MISSING, 1L,
						UnmappedReason.UNKNOWN_COUNTRY_CODE, 1L,
						UnmappedReason.NO_REGION_GEOMETRY, 0L,
						UnmappedReason.AMBIGUOUS_REGION_MAPPING, 0L,
						UnmappedReason.UNSUPPORTED_COUNTRY_CODE, 0L,
						UnmappedReason.OTHER, 0L));
	}

	private static Aggregation zeroAggregation() {
		return new Aggregation(
				0,
				0,
				0,
				0,
				Map.of(
						"country:aaa", zeroRegion(),
						"country:bbb", zeroRegion(),
						"country:ccc", zeroRegion()),
				zeroUnlocatedReasons(),
				zeroUnmappedReasons());
	}

	private static Aggregation withRegions(
			Aggregation source,
			Map<String, RegionCounts> regions
	) {
		return new Aggregation(
				source.eligible(),
				source.mapped(),
				source.unlocated(),
				source.unmapped(),
				regions,
				source.unlocatedReasons(),
				source.unmappedReasons());
	}

	private static RegionCounts zeroRegion() {
		return new RegionCounts(0, 0, 0, 0, 0, 0, 0, 0, 0);
	}

	private static Map<UnlocatedReason, Long> zeroUnlocatedReasons() {
		Map<UnlocatedReason, Long> counts = new EnumMap<>(UnlocatedReason.class);
		for (UnlocatedReason reason : UnlocatedReason.values()) {
			counts.put(reason, 0L);
		}
		return counts;
	}

	private static Map<UnmappedReason, Long> zeroUnmappedReasons() {
		Map<UnmappedReason, Long> counts = new EnumMap<>(UnmappedReason.class);
		for (UnmappedReason reason : UnmappedReason.values()) {
			counts.put(reason, 0L);
		}
		return counts;
	}

	private static CountryGeometryCatalog catalog() {
		return new CountryGeometryCatalog(
				"country-v1",
				List.of(
						new CountryGeometryCatalog.Region("country:aaa", List.of("AA")),
						new CountryGeometryCatalog.Region("country:bbb", List.of("BB")),
						new CountryGeometryCatalog.Region("country:ccc", List.of())),
				Map.of(
						"AA", "country:aaa",
						"BB", "country:bbb"),
				Map.of(
						"XX",
						CountryGeometryCatalog.UnmappedReason.UNSUPPORTED_NON_COUNTRY_CODE));
	}
}
