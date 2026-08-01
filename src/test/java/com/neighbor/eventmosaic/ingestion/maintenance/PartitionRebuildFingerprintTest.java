package com.neighbor.eventmosaic.ingestion.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexRepairCause;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingStatus;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.ArchivePlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildPlan;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Fingerprint плана перестроения partition")
class PartitionRebuildFingerprintTest {

	@Test
	@DisplayName("Не зависит от порядка archive set и alias membership")
	void canonicalizesSetOrdering() {
		ArchivePlan first = archive("a", "2026-07-27T00:00:00Z");
		ArchivePlan second = archive("b", "2026-07-27T00:15:00Z");
		PartitionRebuildPlan left = plan(
				List.of(first, second),
				linkedSet("gdelt-events-v1-p20260727-g0001", "other-event"),
				linkedSet("gdelt-mentions-v1-p20260727-g0001", "other-mention"),
				"event-uuid");
		PartitionRebuildPlan right = plan(
				List.of(second, first),
				linkedSet("other-event", "gdelt-events-v1-p20260727-g0001"),
				linkedSet("other-mention", "gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid");

		assertThat(PartitionRebuildFingerprint.calculate(left))
				.isEqualTo(PartitionRebuildFingerprint.calculate(right));
	}

	@Test
	@DisplayName("Меняется при замене exact UUID исходного поколения")
	void changesWithExactIdentity() {
		PartitionRebuildPlan expected = plan(
				List.of(archive("a", "2026-07-27T00:00:00Z")),
				Set.of("gdelt-events-v1-p20260727-g0001"),
				Set.of("gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid");
		PartitionRebuildPlan replaced = plan(
				List.of(archive("a", "2026-07-27T00:00:00Z")),
				Set.of("gdelt-events-v1-p20260727-g0001"),
				Set.of("gdelt-mentions-v1-p20260727-g0001"),
				"replacement-uuid");

		assertThat(PartitionRebuildFingerprint.calculate(expected))
				.isNotEqualTo(PartitionRebuildFingerprint.calculate(replaced));
	}

	@Test
	@DisplayName("Не меняется от диагностического disk observation и write block")
	void ignoresVolatileObservations() {
		PartitionRebuildPlan first = plan(
				List.of(archive("a", "2026-07-27T00:00:00Z")),
				Set.of("gdelt-events-v1-p20260727-g0001"),
				Set.of("gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid");
		PartitionRebuildPlan second = new PartitionRebuildPlan(
				first.partitionKey(),
				first.partitionStartAt(),
				first.partitionEndAt(),
				first.repairCause(),
				first.partitionStateVersion(),
				first.baseGenerationId(),
				first.baseGenerationUuid(),
				first.baseNames(),
				first.expectedEventIndexUuid(),
				first.expectedMentionIndexUuid(),
				first.observedEventIndexUuid(),
				true,
				first.observedMentionIndexUuid(),
				true,
				first.eventAliasMembership(),
				first.mentionAliasMembership(),
				first.targetGenerationNumber(),
				first.targetNames(),
				first.archives(),
				10,
				20,
				first.expiresAt(),
				first.fingerprint());

		assertThat(PartitionRebuildFingerprint.calculate(first))
				.isEqualTo(PartitionRebuildFingerprint.calculate(second));
	}

	private static PartitionRebuildPlan plan(
			List<ArchivePlan> archives,
			Set<String> eventAliases,
			Set<String> mentionAliases,
			String observedEventUuid
	) {
		return new PartitionRebuildPlan(
				"p20260727",
				Instant.parse("2026-07-27T00:00:00Z"),
				Instant.parse("2026-08-03T00:00:00Z"),
				IndexRepairCause.SURPLUS,
				4,
				10,
				UUID.fromString("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0001",
						"gdelt-mentions-v1-p20260727-g0001"),
				"event-uuid",
				"mention-uuid",
				observedEventUuid,
				false,
				"mention-uuid",
				false,
				eventAliases,
				mentionAliases,
				2,
				new IndexGenerationNames(
						"gdelt-events-v1-p20260727-g0002",
						"gdelt-mentions-v1-p20260727-g0002"),
				archives,
				100,
				200,
				Instant.parse("2026-08-01T12:15:00Z"),
				"0".repeat(64));
	}

	private static ArchivePlan archive(String key, String sourceUpdateTime) {
		return new ArchivePlan(
				key,
				Instant.parse(sourceUpdateTime),
				GdeltArchiveKind.TRANSLATION_EVENTS,
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				"staging/archive.zip",
				"staging/archive.csv",
				"b".repeat(64),
				ArchiveProcessingStatus.INDEXED,
				3,
				1,
				10,
				"c".repeat(64));
	}

	private static Set<String> linkedSet(String first, String second) {
		LinkedHashSet<String> values = new LinkedHashSet<>();
		values.add(first);
		values.add(second);
		return values;
	}
}
