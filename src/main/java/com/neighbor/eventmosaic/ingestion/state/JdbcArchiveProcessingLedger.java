package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingAttempt;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingClaimResult;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingFingerprint;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingLedger;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingState;
import com.neighbor.eventmosaic.ingestion.api.ArchiveProcessingTargetBinding;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
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
	private static final String ATTEMPT_REQUIRED = "attempt must not be null";
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
	public ArchiveProcessingClaimResult claim(
			String archiveIdempotencyKey,
			ArchiveProcessingTargetBinding targetBinding,
			Duration leaseDuration
	) {
		requireArchiveKey(archiveIdempotencyKey);
		Objects.requireNonNull(targetBinding, "targetBinding must not be null");
		requirePositive(leaseDuration);
		return repository.claim(
				archiveIdempotencyKey,
				targetBinding,
				leaseDuration,
				clock.instant());
	}

	@Override
	@Transactional
	public AttemptTransitionResult checkpoint(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			Duration leaseDuration
	) {
		Objects.requireNonNull(attempt, ATTEMPT_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED);
		requirePositive(leaseDuration);
		Instant now = clock.instant();
		return repository.checkpoint(
				attempt,
				progress,
				now.plus(leaseDuration),
				now
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult markIndexed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification
	) {
		Objects.requireNonNull(attempt, ATTEMPT_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED)
				.requireIndexedCompletion();
		Objects.requireNonNull(verification, "verification must not be null");
		requireVerificationMatchesAttempt(verification, attempt.targetBinding(), progress);
		if (!verification.matched()) {
			throw new IllegalArgumentException("INDEXED receipt verification must match");
		}
		return repository.markIndexed(
				attempt,
				progress,
				verification,
				clock.instant()
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult markFailed(
			ArchiveProcessingAttempt attempt,
			ArchiveProcessingFailure failure,
			ArchiveProcessingProgress progress,
			ArchiveReceiptVerification verification
	) {
		Objects.requireNonNull(attempt, ATTEMPT_REQUIRED);
		Objects.requireNonNull(failure, FAILURE_REQUIRED);
		Objects.requireNonNull(progress, PROGRESS_REQUIRED);
		if (verification != null) {
			requireVerificationMatchesAttempt(
					verification,
					attempt.targetBinding(),
					progress);
			requireReceiptFailureMatchesVerification(verification, failure);
		}
		else if (isReceiptFailure(failure)) {
			throw new IllegalArgumentException(
					"Receipt failure must contain verification evidence");
		}
		return repository.markFailed(
				attempt,
				failure,
				progress,
				verification,
				clock.instant()
		);
	}

	@Override
	@Transactional
	public AttemptTransitionResult recordReceiptMatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification
	) {
		requireReconciliationContext(
				archiveIdempotencyKey,
				expectedProcessingFingerprint,
				expectedAttemptCount,
				expectedStateVersion,
				expectedStoredTargetBinding,
				verifiedCurrentTargetBinding,
				verification);
		if (!verification.matched()) {
			throw new IllegalArgumentException(
					"Receipt match transition requires matched verification");
		}
		return repository.recordReceiptMatch(
				archiveIdempotencyKey,
				expectedProcessingFingerprint,
				expectedAttemptCount,
				expectedStateVersion,
				expectedStoredTargetBinding,
				verifiedCurrentTargetBinding,
				verification,
				clock.instant());
	}

	@Override
	@Transactional
	public AttemptTransitionResult recordReceiptMismatch(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification,
			ArchiveProcessingFailure failure
	) {
		requireReconciliationContext(
				archiveIdempotencyKey,
				expectedProcessingFingerprint,
				expectedAttemptCount,
				expectedStateVersion,
				expectedStoredTargetBinding,
				verifiedCurrentTargetBinding,
				verification);
		Objects.requireNonNull(failure, FAILURE_REQUIRED);
		requireReceiptFailureMatchesVerification(verification, failure);
		return repository.recordReceiptMismatch(
				archiveIdempotencyKey,
				expectedProcessingFingerprint,
				expectedAttemptCount,
				expectedStateVersion,
				expectedStoredTargetBinding,
				verifiedCurrentTargetBinding,
				verification,
				failure,
				clock.instant()
		);
	}

	private static void requireReconciliationContext(
			String archiveIdempotencyKey,
			String expectedProcessingFingerprint,
			int expectedAttemptCount,
			long expectedStateVersion,
			ArchiveProcessingTargetBinding expectedStoredTargetBinding,
			ArchiveProcessingTargetBinding verifiedCurrentTargetBinding,
			ArchiveReceiptVerification verification
	) {
		requireArchiveKey(archiveIdempotencyKey);
		requireProcessingFingerprint(expectedProcessingFingerprint);
		if (expectedAttemptCount <= 0) {
			throw new IllegalArgumentException("expectedAttemptCount must be positive");
		}
		if (expectedStateVersion < 0) {
			throw new IllegalArgumentException("expectedStateVersion must not be negative");
		}
		Objects.requireNonNull(
				expectedStoredTargetBinding,
				"expectedStoredTargetBinding must not be null");
		Objects.requireNonNull(
				verifiedCurrentTargetBinding,
				"verifiedCurrentTargetBinding must not be null");
		Objects.requireNonNull(verification, "verification must not be null");
		if (expectedStoredTargetBinding.indexKind()
				!= verifiedCurrentTargetBinding.indexKind()
				|| !expectedStoredTargetBinding.partitionKey().equals(
						verifiedCurrentTargetBinding.partitionKey())) {
			throw new IllegalArgumentException(
					"Stored and current receipt targets must belong to one partition kind");
		}
		requireVerificationTarget(verification, verifiedCurrentTargetBinding);
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

	private static void requireVerificationMatchesAttempt(
			ArchiveReceiptVerification verification,
			ArchiveProcessingTargetBinding targetBinding,
			ArchiveProcessingProgress progress
	) {
		requireVerificationTarget(verification, targetBinding);
		if (verification.expectedDocumentCount() != progress.succeededOperations()
				|| verification.actualDocumentCount() != progress.receiptDocuments()) {
			throw new IllegalArgumentException(
					"Receipt verification counts must match processing progress");
		}
	}

	private static void requireVerificationTarget(
			ArchiveReceiptVerification verification,
			ArchiveProcessingTargetBinding targetBinding
	) {
		if (verification.kind() != targetBinding.indexKind()) {
			throw new IllegalArgumentException(
					"Receipt verification kind must match captured target");
		}
	}

	private static void requireReceiptFailureMatchesVerification(
			ArchiveReceiptVerification verification,
			ArchiveProcessingFailure failure
	) {
		if (verification.matched()) {
			throw new IllegalArgumentException("Receipt failure requires mismatch evidence");
		}
		boolean surplus = verification.actualDocumentCount()
				> verification.expectedDocumentCount();
		String expectedCode = surplus ? RECEIPT_SURPLUS_CODE : RECEIPT_MISMATCH_CODE;
		boolean expectedRetryable = !surplus;
		if (!expectedCode.equals(failure.errorCode())
				|| expectedRetryable != failure.retryable()) {
			throw new IllegalArgumentException(
					"Receipt failure must match verification outcome");
		}
	}

	private static boolean isReceiptFailure(ArchiveProcessingFailure failure) {
		return RECEIPT_MISMATCH_CODE.equals(failure.errorCode())
				|| RECEIPT_SURPLUS_CODE.equals(failure.errorCode());
	}
}
