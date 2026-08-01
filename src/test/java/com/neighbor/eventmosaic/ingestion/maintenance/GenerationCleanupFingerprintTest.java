package com.neighbor.eventmosaic.ingestion.maintenance;

import static org.assertj.core.api.Assertions.assertThat;

import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationNames;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationWriteOutcome;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ProtectedActiveEvidence;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReceiptPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReplaySourcePlan;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Fingerprint плана очистки технического поколения")
class GenerationCleanupFingerprintTest {

	private static final IndexGenerationNames OLD_NAMES = new IndexGenerationNames(
			"gdelt-events-v1-p20260727-g0001",
			"gdelt-mentions-v1-p20260727-g0001");
	private static final IndexGenerationNames ACTIVE_NAMES = new IndexGenerationNames(
			"gdelt-events-v1-p20260727-g0002",
			"gdelt-mentions-v1-p20260727-g0002");

	@Test
	@DisplayName("Не зависит от порядка aliases, архивов и receipt")
	void canonicalizesUnorderedEvidence() {
		ReplaySourcePlan firstSource = source("archive-a", "2026-07-27T00:00:00Z");
		ReplaySourcePlan secondSource = source("archive-b", "2026-07-27T00:15:00Z");
		ReceiptPlan firstReceipt = receipt("archive-a", GdeltIndexKind.EVENT);
		ReceiptPlan secondReceipt = receipt("archive-b", GdeltIndexKind.MENTION);
		GenerationCleanupPlan left = plan(
				List.of(firstSource, secondSource),
				List.of(firstReceipt, secondReceipt),
				linkedSet("gdelt-events-read", "operator-hold"),
				linkedSet("gdelt-mentions-read", "operator-hold"),
				"old-event-uuid");
		GenerationCleanupPlan right = plan(
				List.of(secondSource, firstSource),
				List.of(secondReceipt, firstReceipt),
				linkedSet("operator-hold", "gdelt-events-read"),
				linkedSet("operator-hold", "gdelt-mentions-read"),
				"old-event-uuid");

		assertThat(GenerationCleanupFingerprint.calculate(left))
				.isEqualTo(GenerationCleanupFingerprint.calculate(right));
	}

	@Test
	@DisplayName("Меняется при замене ожидаемого UUID удаляемого индекса")
	void changesWithExactDeleteIdentity() {
		GenerationCleanupPlan expected = plan(
				List.of(source("archive-a", "2026-07-27T00:00:00Z")),
				List.of(receipt("archive-a", GdeltIndexKind.EVENT)),
				Set.of(),
				Set.of(),
				"old-event-uuid");
		GenerationCleanupPlan replaced = plan(
				List.of(source("archive-a", "2026-07-27T00:00:00Z")),
				List.of(receipt("archive-a", GdeltIndexKind.EVENT)),
				Set.of(),
				Set.of(),
				"replacement-event-uuid");

		assertThat(GenerationCleanupFingerprint.calculate(expected))
				.isNotEqualTo(GenerationCleanupFingerprint.calculate(replaced));
	}

	@Test
	@DisplayName("Не меняется от информационного размера индексов")
	void ignoresObservedStoreSize() {
		List<ReplaySourcePlan> sources = List.of(
				source("archive-a", "2026-07-27T00:00:00Z"));
		List<ReceiptPlan> receipts = List.of(
				receipt("archive-a", GdeltIndexKind.EVENT));
		GenerationCleanupPlan first = plan(
				sources,
				receipts,
				Set.of(),
				Set.of(),
				"old-event-uuid",
				100);
		GenerationCleanupPlan second = plan(
				sources,
				receipts,
				Set.of(),
				Set.of(),
				"old-event-uuid",
				10_000);

		assertThat(GenerationCleanupFingerprint.calculate(first))
				.isEqualTo(GenerationCleanupFingerprint.calculate(second));
	}

	private static GenerationCleanupPlan plan(
			List<ReplaySourcePlan> sources,
			List<ReceiptPlan> receipts,
			Set<String> eventAliases,
			Set<String> mentionAliases,
			String expectedEventUuid
	) {
		return plan(
				sources,
				receipts,
				eventAliases,
				mentionAliases,
				expectedEventUuid,
				0);
	}

	private static GenerationCleanupPlan plan(
			List<ReplaySourcePlan> sources,
			List<ReceiptPlan> receipts,
			Set<String> eventAliases,
			Set<String> mentionAliases,
			String expectedEventUuid,
			long observedStoreBytes
	) {
		return new GenerationCleanupPlan(
				"p20260727",
				Instant.parse("2026-07-27T00:00:00Z"),
				Instant.parse("2026-08-03T00:00:00Z"),
				4,
				10,
				UUID.fromString("11111111-1111-1111-1111-111111111111"),
				1,
				IndexGenerationStatus.SUPERSEDED,
				3,
				OLD_NAMES,
				expectedEventUuid,
				"old-mention-uuid",
				null,
				GenerationWriteOutcome.COMPLETED,
				false,
				false,
				Instant.parse("2026-07-27T12:00:00Z"),
				Instant.parse("2026-08-01T12:00:00Z"),
				null,
				"old-event-uuid",
				eventAliases,
				"old-mention-uuid",
				mentionAliases,
				observedStoreBytes,
				new ProtectedActiveEvidence(
						20,
						UUID.fromString("22222222-2222-2222-2222-222222222222"),
						7,
						ACTIVE_NAMES,
						"active-event-uuid",
						"active-mention-uuid"),
				sources,
				receipts,
				Instant.parse("2026-08-01T12:15:00Z"),
				"0".repeat(64));
	}

	private static ReplaySourcePlan source(String key, String updateTime) {
		return new ReplaySourcePlan(
				key,
				Instant.parse(updateTime),
				"20260727000000.translation.export.CSV.zip",
				100,
				"a".repeat(32),
				"staging/" + key + ".zip",
				"staging/" + key + ".csv");
	}

	private static ReceiptPlan receipt(String key, GdeltIndexKind kind) {
		return new ReceiptPlan(
				key,
				"b".repeat(64),
				4,
				2,
				kind,
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
