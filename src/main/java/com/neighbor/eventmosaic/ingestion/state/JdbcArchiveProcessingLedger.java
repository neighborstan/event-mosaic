package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Транзакционный facade отдельного processing ledger с единым application Clock.
 */
@Repository
public class JdbcArchiveProcessingLedger implements ArchiveProcessingLedger {

	private static final String RECEIPT_MISMATCH_CODE = "INDEX_RECEIPT_MISMATCH";
	private static final String RECEIPT_SURPLUS_CODE = "INDEX_RECEIPT_SURPLUS";
	private static final String ATTEMPT_TOKEN_REQUIRED =
			"attemptToken must not be null";
	private static final String PROGRESS_REQUIRED = "progress must not be null";
	private static final String FAILURE_REQUIRED = "failure must not be null";
	private static final Pattern PROCESSING_FINGERPRINT = Pattern.compile("^[0-9a-f]{64}$");

	private final JdbcArchiveProcessingRepository repository;
	private final Clock clock;

	/**
	 * Создает facade из узкого JDBC repository и UTC Clock.
	 *
	 * @param repository repository processing rows
	 * @param clock единый production source времени
	 */
	public JdbcArchiveProcessingLedger(
			JdbcArchiveProcessingRepository repository,
			Clock clock
	) {
		this.repository = repository;
		this.clock = clock;
	}

	@Override
	@Transactional
	public ArchiveProcessingState register(
			String archiveIdempotencyKey,
			ArchiveProcessingFingerprint fingerprint
	) {
		requireArchiveKey(archiveIdempotencyKey);
		Objects.requireNonNull(fingerprint, "fingerprint must not be null");
		return repository.register(archiveIdempotencyKey, fingerprint, clock.instant());
	}

	@Override
	@Transactional
	public Optional<ArchiveProcessingAttempt> claim(
			String archiveIdempotencyKey,
			Duration leaseDuration
	) {
		requireArchiveKey(archiveIdempotencyKey);
		requirePositive(leaseDuration);
		return repository.claim(archiveIdempotencyKey, leaseDuration, clock.instant());
	}

	@Override
	@Transactional
	public AttemptTransitionResult checkpoint(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress,
			Duration leaseDuration
	) {
		requireArchiveKey(archiveIdempotencyKey);
		Objects.requireNonNull(attemptToken, ATTEMPT_TOKEN_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED);
		requirePositive(leaseDuration);
		Instant now = clock.instant();
		return repository.checkpoint(
				archiveIdempotencyKey,
				attemptToken,
				progress,
				now.plus(leaseDuration),
				now
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult markIndexed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingProgress progress
	) {
		requireArchiveKey(archiveIdempotencyKey);
		Objects.requireNonNull(attemptToken, ATTEMPT_TOKEN_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED)
				.requireIndexedCompletion();
		return repository.markIndexed(
				archiveIdempotencyKey,
				attemptToken,
				progress,
				clock.instant()
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult markFailed(
			String archiveIdempotencyKey,
			UUID attemptToken,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress
	) {
		requireArchiveKey(archiveIdempotencyKey);
		Objects.requireNonNull(attemptToken, ATTEMPT_TOKEN_REQUIRED);
		Objects.requireNonNull(failure, FAILURE_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED);
		return repository.markFailed(
				archiveIdempotencyKey,
				attemptToken,
				failure,
				progress,
				clock.instant()
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			ArchiveProcessingFailure failure
	) {
		requireArchiveKey(archiveIdempotencyKey);
		requireProcessingFingerprint(expectedProcessingFingerprint);
		if (expectedAttemptCount <= 0) {
			throw new IllegalArgumentException("expectedAttemptCount must be positive");
		}
		Objects.requireNonNull(failure, FAILURE_REQUIRED);
		boolean retryableShortage = failure.retryable()
				&& RECEIPT_MISMATCH_CODE.equals(failure.errorCode());
		boolean nonRetryableSurplus = !failure.retryable()
				&& RECEIPT_SURPLUS_CODE.equals(failure.errorCode());
		if (!retryableShortage && !nonRetryableSurplus) {
			throw new IllegalArgumentException(
					"Receipt failure must be retryable mismatch or non-retryable surplus");
		}
		return repository.recordReceiptMismatch(
				archiveIdempotencyKey,
				expectedProcessingFingerprint,
				expectedAttemptCount,
				failure,
				clock.instant()
		);
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<ArchiveProcessingState> findByArchiveIdempotencyKey(
			String archiveIdempotencyKey
	) {
		requireArchiveKey(archiveIdempotencyKey);
		return repository.findByArchiveIdempotencyKey(archiveIdempotencyKey);
	}

	private static void requireArchiveKey(String archiveIdempotencyKey) {
		Objects.requireNonNull(archiveIdempotencyKey, "archiveIdempotencyKey must not be null");
		if (archiveIdempotencyKey.isBlank()) {
			throw new IllegalArgumentException("archiveIdempotencyKey must not be blank");
		}
	}

	private static void requirePositive(Duration leaseDuration) {
		Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
		if (leaseDuration.isZero() || leaseDuration.isNegative()) {
			throw new IllegalArgumentException("leaseDuration must be positive");
		}
	}

	private static void requireProcessingFingerprint(String processingFingerprint) {
		Objects.requireNonNull(
				processingFingerprint,
				"expectedProcessingFingerprint must not be null");
		if (!PROCESSING_FINGERPRINT.matcher(processingFingerprint).matches()) {
			throw new IllegalArgumentException(
					"expectedProcessingFingerprint must be a lowercase SHA-256");
		}
	}
}
