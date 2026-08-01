package com.neighbor.eventmosaic.indexing.state;

import com.neighbor.eventmosaic.indexing.api.CleanupBuildWriteOutcome;
import com.neighbor.eventmosaic.indexing.api.CleanupCandidateSnapshot;
import com.neighbor.eventmosaic.indexing.api.CleanupClaim;
import com.neighbor.eventmosaic.indexing.api.CleanupCompletionEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupDeleteEvidence;
import com.neighbor.eventmosaic.indexing.api.CleanupOperation;
import com.neighbor.eventmosaic.indexing.api.CleanupOwnership;
import com.neighbor.eventmosaic.indexing.api.CleanupTransitionResult;
import com.neighbor.eventmosaic.indexing.api.IndexCleanupLedger;
import com.neighbor.eventmosaic.indexing.api.IndexGenerationStatus;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/** Транзакционный facade fenced cleanup для физических generation индексов. */
@Repository
public class JdbcIndexCleanupLedger implements IndexCleanupLedger {

	private static final Pattern PARTITION_KEY = Pattern.compile("^[a-z0-9][a-z0-9_-]{0,31}$");
	private static final Pattern FINGERPRINT = Pattern.compile("^[0-9a-f]{64}$");

	private final JdbcIndexCleanupRepository repository;
	private final Clock clock;

	/**
	 * Создает facade с единым application Clock.
	 *
	 * @param repository JDBC state machine cleanup
	 * @param clock UTC source текущего времени
	 */
	public JdbcIndexCleanupLedger(JdbcIndexCleanupRepository repository, Clock clock) {
		this.repository = repository;
		this.clock = clock;
	}

	@Override
	@Transactional(readOnly = true)
	public List<CleanupCandidateSnapshot> findCandidates(
			String partitionKey,
			Duration orphanBuildingAge,
			Duration supersededAge
	) {
		requirePartitionKey(partitionKey);
		requirePositive(orphanBuildingAge, "orphanBuildingAge");
		requirePositive(supersededAge, "supersededAge");
		Instant now = clock.instant();
		return repository.findCandidates(
				partitionKey,
				now.minus(orphanBuildingAge),
				now.minus(supersededAge),
				now);
	}

	@Override
	@Transactional
	public Optional<CleanupOperation> claim(CleanupClaim claim, Duration leaseDuration) {
		Objects.requireNonNull(claim, "claim");
		requirePositive(leaseDuration, "leaseDuration");
		validateFingerprint(claim.planFingerprint());
		Instant now = clock.instant();
		if (!claim.planExpiresAt().isAfter(now)
				|| !claim.aliasesConfirmedAbsent()
				|| !claim.replaySourcesConfirmed()
				|| claim.candidate().activeProcessing()
				|| claim.candidate().repairOpen()) {
			return Optional.empty();
		}
		if (claim.candidate().status() == IndexGenerationStatus.SUPERSEDED
				&& !claim.currentReceiptsConfirmed()) {
			return Optional.empty();
		}
		if ((claim.candidate().status() == IndexGenerationStatus.FAILED
				|| claim.candidate().status() == IndexGenerationStatus.BUILDING)
				&& (claim.candidate().buildWriteOutcome()
						!= CleanupBuildWriteOutcome.NONE
						|| "REBUILD_BASE_INVALID".equals(claim.candidate().failureOrigin()))) {
			return Optional.empty();
		}
		return repository.claim(claim, leaseDuration, now);
	}

	@Override
	@Transactional
	public Optional<CleanupOperation> resume(
			String partitionKey,
			String planFingerprint,
			Duration leaseDuration
	) {
		requirePartitionKey(partitionKey);
		validateFingerprint(planFingerprint);
		requirePositive(leaseDuration, "leaseDuration");
		return repository.resume(partitionKey, planFingerprint, leaseDuration, clock.instant());
	}

	@Override
	@Transactional
	public Optional<CleanupOperation> renew(
			CleanupOwnership ownership,
			Duration leaseDuration
	) {
		Objects.requireNonNull(ownership, "ownership");
		validateFingerprint(ownership.planFingerprint());
		requirePositive(leaseDuration, "leaseDuration");
		return repository.renew(ownership, leaseDuration, clock.instant());
	}

	@Override
	@Transactional
	public CleanupTransitionResult requestDelete(
			CleanupOwnership ownership,
			CleanupDeleteEvidence evidence
	) {
		Objects.requireNonNull(ownership, "ownership");
		Objects.requireNonNull(evidence, "evidence");
		validateFingerprint(ownership.planFingerprint());
		if (!evidence.aliasesConfirmedAbsent() || !evidence.replaySourcesConfirmed()) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}
		return repository.requestDelete(ownership, evidence, clock.instant());
	}

	@Override
	@Transactional
	public CleanupTransitionResult complete(
			CleanupOwnership ownership,
			CleanupCompletionEvidence evidence
	) {
		Objects.requireNonNull(ownership, "ownership");
		Objects.requireNonNull(evidence, "evidence");
		validateFingerprint(ownership.planFingerprint());
		if (!evidence.eventIndexAbsent() || !evidence.mentionIndexAbsent()) {
			return CleanupTransitionResult.OWNERSHIP_LOST;
		}
		return repository.complete(ownership, evidence, clock.instant());
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<CleanupOperation> findRecoverable(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.findRecoverable(partitionKey);
	}

	@Override
	@Transactional(readOnly = true)
	public boolean hasOpenMaintenance(String partitionKey) {
		requirePartitionKey(partitionKey);
		return repository.hasOpenMaintenance(partitionKey);
	}

	private static void requirePartitionKey(String partitionKey) {
		if (partitionKey == null || !PARTITION_KEY.matcher(partitionKey).matches()) {
			throw new IllegalArgumentException("partitionKey имеет неверный формат");
		}
	}

	private static void validateFingerprint(String fingerprint) {
		if (fingerprint == null || !FINGERPRINT.matcher(fingerprint).matches()) {
			throw new IllegalArgumentException("planFingerprint имеет неверный формат");
		}
	}

	private static void requirePositive(Duration duration, String name) {
		Objects.requireNonNull(duration, name);
		if (duration.isZero() || duration.isNegative()) {
			throw new IllegalArgumentException(name + " должен быть положительным");
		}
	}
}
