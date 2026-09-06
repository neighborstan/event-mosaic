package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionArchiveState;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.IngestionGap;
import com.neighbor.eventmosaic.ingestion.api.IngestionRunState;
import com.neighbor.eventmosaic.ingestion.api.StagedArchive;
import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Транзакционный facade PostgreSQL ledger для acquisition pipeline.
 *
 * <p>SQL обязанности делегируются узким package-private repositories, а facade
 * задает единый момент времени и границы транзакций use case.</p>
 */
@Repository
public class JdbcIngestionArchiveLedger implements IngestionArchiveLedger {

	private final JdbcIngestionRunRepository runRepository;
	private final JdbcIngestionArchiveRepository archiveRepository;
	private final JdbcIngestionContinuityRepository continuityRepository;
	private final Clock clock;

	/**
	 * Создает ledger из узких JDBC collaborators и application Clock.
	 *
	 * @param runRepository repository производного состояния run
	 * @param archiveRepository repository archive rows и transitions
	 * @param continuityRepository repository source progress и gaps
	 * @param clock единый UTC clock приложения
	 */
	public JdbcIngestionArchiveLedger(
			JdbcIngestionRunRepository runRepository,
			JdbcIngestionArchiveRepository archiveRepository,
			JdbcIngestionContinuityRepository continuityRepository,
			Clock clock
	) {
		this.runRepository = runRepository;
		this.archiveRepository = archiveRepository;
		this.continuityRepository = continuityRepository;
		this.clock = clock;
	}

	@Override
	@Transactional
	public int registerDiscoveredUpdate(
			DiscoveredUpdate update,
			FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt
	) {
		Objects.requireNonNull(update, "update must not be null");
		Objects.requireNonNull(firstRunPolicy, "firstRunPolicy must not be null");
		Instant now = clock.instant();
		long runId = runRepository.findOrCreate(update.sourceUpdateTime(), now);
		for (DiscoveredArchive archive : update.archives()) {
			archiveRepository.register(runId, archive, now);
		}
		return continuityRepository.update(
				update.sourceUpdateTime(),
				firstRunPolicy,
				firstRunStartAt,
				now
		);
	}

	@Override
	@Transactional
	public Optional<ArchiveAttempt> claimArchive(String idempotencyKey, Duration leaseDuration) {
		Objects.requireNonNull(idempotencyKey, "idempotencyKey must not be null");
		Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
		if (leaseDuration.isZero() || leaseDuration.isNegative()) {
			throw new IllegalArgumentException("leaseDuration must be positive");
		}
		Optional<ClaimedArchive> claimed = archiveRepository.claim(idempotencyKey, leaseDuration, clock.instant());
		claimed.ifPresent(value -> runRepository.recalculate(value.runId(), clock.instant()));
		return claimed.map(ClaimedArchive::attempt);
	}

	@Override
	@Transactional
	public AttemptTransitionResult markStaged(
			String idempotencyKey,
			UUID attemptToken,
			StagedArchive stagedArchive
	) {
		ArchiveTransition transition = archiveRepository.markStaged(
				idempotencyKey,
				attemptToken,
				stagedArchive,
				clock.instant()
		);
		recalculateApplied(transition);
		return transition.result();
	}

	@Override
	@Transactional
	public AttemptTransitionResult markFailed(
			String idempotencyKey,
			UUID attemptToken,
			IngestionFailure failure,
			Duration retryAfter
	) {
		Objects.requireNonNull(retryAfter, "retryAfter must not be null");
		if (retryAfter.isNegative()) {
			throw new IllegalArgumentException("retryAfter must not be negative");
		}
		ArchiveTransition transition = archiveRepository.markFailed(
				idempotencyKey,
				attemptToken,
				failure,
				retryAfter,
				clock.instant()
		);
		recalculateApplied(transition);
		return transition.result();
	}

	@Override
	public Optional<IngestionArchiveState> findByIdempotencyKey(String idempotencyKey) {
		return archiveRepository.findByIdempotencyKey(idempotencyKey);
	}

	@Override
	@Transactional(readOnly = true)
	public List<IngestionArchiveState> findStagedBetween(
			Instant startAt,
			Instant endAt
	) {
		Objects.requireNonNull(startAt, "startAt must not be null");
		Objects.requireNonNull(endAt, "endAt must not be null");
		if (!startAt.isBefore(endAt)) {
			throw new IllegalArgumentException("startAt must be before endAt");
		}
		return archiveRepository.findStagedBetween(startAt, endAt);
	}

	@Override
	@Transactional(readOnly = true)
	public List<IngestionArchiveState> findEligibleRecentWork(int limit, Set<String> excludedKeys) {
		if (limit < 1 || limit > 1024) {
			throw new IllegalArgumentException("limit must be between 1 and 1024");
		}
		Set<String> exclusions = Set.copyOf(excludedKeys);
		if (exclusions.size() > 1024 || exclusions.stream().anyMatch(String::isBlank)) {
			throw new IllegalArgumentException("excludedKeys must contain at most 1024 nonblank keys");
		}
		return archiveRepository.findEligibleRecentWork(limit, exclusions, clock.instant());
	}

	@Override
	@Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
	public Optional<IngestionRunState> findRunByUpdateTime(Instant sourceUpdateTime) {
		return runRepository.findByUpdateTime(sourceUpdateTime).map(this::loadRun);
	}

	@Override
	@Transactional(readOnly = true, isolation = Isolation.REPEATABLE_READ)
	public Optional<IngestionRunState> findLatestRun() {
		return runRepository.findLatest().map(this::loadRun);
	}

	@Override
	public Optional<Instant> findCompletionProgress(ArchiveType archiveType) {
		return archiveRepository.findCompletionProgress(archiveType);
	}

	@Override
	public List<IngestionGap> findOpenGaps() {
		return continuityRepository.findOpenGaps();
	}

	private void recalculateApplied(ArchiveTransition transition) {
		if (transition.result() == AttemptTransitionResult.APPLIED) {
			runRepository.recalculate(transition.runId(), clock.instant());
		}
	}

	private IngestionRunState loadRun(IngestionRunRow row) {
		return new IngestionRunState(
				row.id(),
				row.sourceUpdateTime(),
				row.status(),
				row.firstSeenAt(),
				row.completedAt(),
				row.lastFailure(),
				archiveRepository.findByRunId(row.id())
		);
	}
}
