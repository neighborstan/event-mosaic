package com.neighbor.eventmosaic.ingestion.maintenance;

import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.ArchivePlan;
import com.neighbor.eventmosaic.ingestion.api.PartitionRebuildService.PartitionRebuildPlan;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.HexFormat;

/** Считает canonical SHA-256 immutable authorization полей rebuild plan. */
final class PartitionRebuildFingerprint {

	private PartitionRebuildFingerprint() {
	}

	static String calculate(PartitionRebuildPlan plan) {
		CanonicalDigest digest = new CanonicalDigest();
		digest.add("partition-rebuild-plan-v1");
		digest.add(plan.partitionKey());
		digest.add(plan.partitionStartAt().toString());
		digest.add(plan.partitionEndAt().toString());
		digest.add(plan.repairCause().name());
		digest.add(plan.partitionStateVersion());
		digest.add(plan.baseGenerationId());
		digest.add(plan.baseGenerationUuid().toString());
		digest.add(plan.baseNames().eventIndexName());
		digest.add(plan.baseNames().mentionIndexName());
		digest.add(plan.expectedEventIndexUuid());
		digest.add(plan.expectedMentionIndexUuid());
		digest.addNullable(plan.observedEventIndexUuid());
		digest.addNullable(plan.observedMentionIndexUuid());
		plan.eventAliasMembership().stream().sorted().forEach(digest::add);
		digest.add("event-alias-end");
		plan.mentionAliasMembership().stream().sorted().forEach(digest::add);
		digest.add("mention-alias-end");
		digest.add(plan.targetGenerationNumber());
		digest.add(plan.targetNames().eventIndexName());
		digest.add(plan.targetNames().mentionIndexName());
		digest.add(plan.expiresAt().toString());
		for (ArchivePlan archive : plan.archives().stream()
				.sorted(Comparator
						.comparing(ArchivePlan::sourceUpdateTime)
						.thenComparing(archive -> archive.kind().name())
						.thenComparing(ArchivePlan::archiveKey))
				.toList()) {
			digest.add(archive.archiveKey());
			digest.add(archive.sourceUpdateTime().toString());
			digest.add(archive.kind().name());
			digest.add(archive.archiveName());
			digest.add(archive.expectedArchiveSizeBytes());
			digest.add(archive.expectedArchiveMd5());
			digest.add(archive.stagedArchivePath());
			digest.add(archive.stagedCsvPath());
			digest.add(archive.processingFingerprint());
			digest.add(archive.processingStatus().name());
			digest.add(archive.processingStateVersion());
			digest.add(archive.attemptCount());
			digest.add(archive.expectedDocumentCount());
			digest.add(archive.expectedIdentityDigest());
		}
		return digest.finish();
	}

	private static final class CanonicalDigest {

		private final MessageDigest digest;

		private CanonicalDigest() {
			try {
				this.digest = MessageDigest.getInstance("SHA-256");
			}
			catch (NoSuchAlgorithmException exception) {
				throw new IllegalStateException("Required SHA-256 algorithm is unavailable", exception);
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
