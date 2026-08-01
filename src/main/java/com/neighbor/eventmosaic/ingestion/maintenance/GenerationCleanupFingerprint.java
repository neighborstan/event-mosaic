package com.neighbor.eventmosaic.ingestion.maintenance;

import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.GenerationCleanupPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReceiptPlan;
import com.neighbor.eventmosaic.ingestion.api.GenerationCleanupService.ReplaySourcePlan;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;

/** Считает canonical SHA-256 immutable authorization полей cleanup plan. */
final class GenerationCleanupFingerprint {

	private GenerationCleanupFingerprint() {
	}

	static String calculate(GenerationCleanupPlan plan) {
		CanonicalDigest digest = new CanonicalDigest();
		digest.add("generation-cleanup-plan-v1");
		digest.add(plan.partitionKey());
		digest.add(plan.partitionStartAt().toString());
		digest.add(plan.partitionEndAt().toString());
		digest.add(plan.partitionStateVersion());
		digest.add(plan.generationId());
		digest.add(plan.generationUuid().toString());
		digest.add(plan.generationNumber());
		digest.add(plan.generationStatus().name());
		digest.add(plan.generationStateVersion());
		digest.add(plan.generationNames().eventIndexName());
		digest.add(plan.generationNames().mentionIndexName());
		digest.addNullable(plan.expectedEventIndexUuid());
		digest.addNullable(plan.expectedMentionIndexUuid());
		digest.addNullable(plan.failureOrigin());
		digest.add(plan.writeOutcome().name());
		digest.add(Boolean.toString(plan.repairOpen()));
		digest.add(Boolean.toString(plan.activeProcessing()));
		digest.add(plan.generationHeartbeatAt().toString());
		digest.add(plan.candidateSinceAt().toString());
		if (plan.ownerEvidence() == null) {
			digest.add("owner-absent");
		}
		else {
			digest.add(plan.ownerEvidence().operationId());
			digest.add(plan.ownerEvidence().type().name());
			digest.add(plan.ownerEvidence().operationToken().toString());
			digest.add(plan.ownerEvidence().phase().name());
			digest.add(plan.ownerEvidence().operationVersion());
			digest.add(plan.ownerEvidence().leaseExpiresAt().toString());
			digest.add(plan.ownerEvidence().heartbeatAt().toString());
		}
		digest.addNullable(plan.observedEventIndexUuid());
		plan.eventAliases().stream().sorted().forEach(digest::add);
		digest.add("event-alias-end");
		digest.addNullable(plan.observedMentionIndexUuid());
		plan.mentionAliases().stream().sorted().forEach(digest::add);
		digest.add("mention-alias-end");
		if (plan.protectedActive() == null) {
			digest.add("active-absent");
		}
		else {
			digest.add(plan.protectedActive().generationId());
			digest.add(plan.protectedActive().generationUuid().toString());
			digest.add(plan.protectedActive().generationStateVersion());
			digest.add(plan.protectedActive().names().eventIndexName());
			digest.add(plan.protectedActive().names().mentionIndexName());
			digest.add(plan.protectedActive().eventIndexUuid());
			digest.add(plan.protectedActive().mentionIndexUuid());
		}
		for (ReplaySourcePlan source : plan.replaySources().stream()
				.sorted(Comparator
						.comparing(ReplaySourcePlan::sourceUpdateTime)
						.thenComparing(ReplaySourcePlan::archiveKey))
				.toList()) {
			digest.add(source.archiveKey());
			digest.add(source.sourceUpdateTime().toString());
			digest.add(source.archiveName());
			digest.add(source.expectedArchiveSizeBytes());
			digest.add(source.expectedArchiveMd5());
			digest.add(source.stagedArchivePath());
			digest.add(source.stagedCsvPath());
		}
		for (ReceiptPlan receipt : plan.currentReceipts().stream()
				.sorted(Comparator
						.comparing(ReceiptPlan::archiveKey)
						.thenComparing(receipt -> receipt.kind().name()))
				.toList()) {
			digest.add(receipt.archiveKey());
			digest.add(receipt.processingFingerprint());
			digest.add(receipt.processingStateVersion());
			digest.add(receipt.attemptCount());
			digest.add(receipt.kind().name());
			digest.add(receipt.expectedDocumentCount());
			digest.add(receipt.expectedIdentityDigest());
		}
		digest.add(plan.expiresAt().toString());
		return digest.finish();
	}

	private static final class CanonicalDigest {

		private final MessageDigest digest;

		private CanonicalDigest() {
			try {
				this.digest = MessageDigest.getInstance("SHA-256");
			}
			catch (NoSuchAlgorithmException exception) {
				throw new IllegalStateException(
						"Required SHA-256 algorithm is unavailable", exception);
			}
		}

		private void add(long value) {
			add(Long.toString(value));
		}

		private void addNullable(String value) {
			add(value == null ? "<absent>" : value);
		}

		private void add(String value) {
			byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
			digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
			digest.update(bytes);
		}

		private String finish() {
			return HexFormat.of().formatHex(digest.digest());
		}
	}
}
