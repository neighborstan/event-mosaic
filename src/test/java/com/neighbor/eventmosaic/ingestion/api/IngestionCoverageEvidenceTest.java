package com.neighbor.eventmosaic.ingestion.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Evidence полноты Event-данных")
class IngestionCoverageEvidenceTest {

	private static final Instant FROM = Instant.parse("2026-07-20T00:00:00Z");

	@Test
	@DisplayName("Каждый статус имеет только разрешенную форму списка пропусков")
	void statusRequiresMatchingMissingIntervalShape() {
		IngestionCoverageInterval interval = interval(0, 15);

		assertThat(IngestionCoverageEvidence.complete())
				.isEqualTo(new IngestionCoverageEvidence(
						IngestionCoverageStatus.COMPLETE,
						List.of()));
		assertThat(IngestionCoverageEvidence.partial(List.of(interval)))
				.isEqualTo(new IngestionCoverageEvidence(
						IngestionCoverageStatus.PARTIAL,
						List.of(interval)));
		assertThat(IngestionCoverageEvidence.unknown().missingIntervals()).isNull();

		assertThatThrownBy(() -> new IngestionCoverageEvidence(
				IngestionCoverageStatus.COMPLETE,
				List.of(interval)))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("COMPLETE");
		assertThatThrownBy(() -> new IngestionCoverageEvidence(
				IngestionCoverageStatus.PARTIAL,
				List.of()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("PARTIAL");
		assertThatThrownBy(() -> new IngestionCoverageEvidence(
				IngestionCoverageStatus.UNKNOWN,
				List.of()))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("UNKNOWN");
	}

	@Test
	@DisplayName("Список пропусков копируется и не допускает соседние или пересекающиеся интервалы")
	void missingIntervalsAreImmutableAndAlreadyMerged() {
		IngestionCoverageInterval first = interval(0, 15);
		IngestionCoverageInterval separate = interval(30, 45);
		List<IngestionCoverageInterval> source = new ArrayList<>(List.of(first, separate));

		IngestionCoverageEvidence evidence = IngestionCoverageEvidence.partial(source);
		source.clear();

		assertThat(evidence.missingIntervals()).containsExactly(first, separate);
		assertThatThrownBy(() -> evidence.missingIntervals().add(interval(60, 75)))
				.isInstanceOf(UnsupportedOperationException.class);
		assertThatThrownBy(() -> IngestionCoverageEvidence.partial(List.of(
				first,
				interval(15, 30))))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("non-adjacent");
		assertThatThrownBy(() -> IngestionCoverageEvidence.partial(List.of(
				interval(0, 30),
				interval(15, 45))))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("disjoint");
	}

	@Test
	@DisplayName("Полуоткрытый интервал требует возрастающие ненулевые границы")
	void intervalRequiresIncreasingBoundaries() {
		assertThatThrownBy(() -> new IngestionCoverageInterval(FROM, FROM))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("before");
		assertThatThrownBy(() -> new IngestionCoverageInterval(
				FROM.plusSeconds(1),
				FROM))
				.isInstanceOf(IllegalArgumentException.class)
				.hasMessageContaining("before");
	}

	private static IngestionCoverageInterval interval(long fromMinutes, long toMinutes) {
		return new IngestionCoverageInterval(
				FROM.plusSeconds(fromMinutes * 60),
				FROM.plusSeconds(toMinutes * 60));
	}
}
