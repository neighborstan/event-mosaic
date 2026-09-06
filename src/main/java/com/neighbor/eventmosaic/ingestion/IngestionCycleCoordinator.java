package com.neighbor.eventmosaic.ingestion;

import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.ingestion.error.OperationDeadlineExceededException;
import com.neighbor.eventmosaic.ingestion.observability.IngestionCycleActivity;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationBudgetFactory;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionException;

/**
 * Ограждает любой ingestion trigger одним source-scoped PostgreSQL ownership.
 * Общий budget начинается до claim, а pipeline получает только guarded view.
 */
@Service
public class IngestionCycleCoordinator {

	/** Минимальный запас между общей deadline и окончанием cycle lease. */
	public static final Duration LEASE_SAFETY_MARGIN = Duration.ofMinutes(3);

	private final IngestionCycleLedger cycleLedger;
	private final GdeltPipelineService pipelineService;
	private final OperationBudgetFactory budgetFactory;
	private final Duration operationDeadline;
	private final Duration cycleLease;
	private final IngestionMetrics metrics;
	private final IngestionCycleActivity activity;

	/**
	 * Создает единственную trigger boundary полного ingestion cycle.
	 *
	 * @param cycleLedger долговечный global ownership
	 * @param pipelineService trigger-independent pipeline
	 * @param budgetFactory factory monotonic budget
	 * @param ingestionProperties настройки cycle lease
	 * @param backendDataProperties общая operation deadline
	 */
	@Autowired
	public IngestionCycleCoordinator(
			IngestionCycleLedger cycleLedger,
			GdeltPipelineService pipelineService,
			OperationBudgetFactory budgetFactory,
			GdeltIngestionProperties ingestionProperties,
			BackendDataProperties backendDataProperties,
			IngestionMetrics metrics,
			IngestionCycleActivity activity
	) {
		this(
				cycleLedger,
				pipelineService,
				budgetFactory,
				backendDataProperties.operationDeadline(),
				ingestionProperties.automatic().cycleLease(),
				metrics,
				activity);
	}

	IngestionCycleCoordinator(
			IngestionCycleLedger cycleLedger,
			GdeltPipelineService pipelineService,
			OperationBudgetFactory budgetFactory,
			Duration operationDeadline,
			Duration cycleLease,
			IngestionMetrics metrics,
			IngestionCycleActivity activity
	) {
		this.cycleLedger = Objects.requireNonNull(cycleLedger, "cycleLedger must not be null");
		this.pipelineService = Objects.requireNonNull(
				pipelineService, "pipelineService must not be null");
		this.budgetFactory = Objects.requireNonNull(
				budgetFactory, "budgetFactory must not be null");
		this.operationDeadline = requirePositive(
				operationDeadline, "operationDeadline");
		this.cycleLease = requirePositive(cycleLease, "cycleLease");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
		this.activity = Objects.requireNonNull(activity, "activity must not be null");
		if (cycleLease.compareTo(operationDeadline.plus(LEASE_SAFETY_MARGIN)) < 0) {
			throw new IllegalArgumentException(
					"cycleLease must cover operationDeadline and lease safety margin");
		}
	}

	/**
	 * Пытается выполнить один cycle без ожидания занятого owner и без sleep.
	 *
	 * @return bounded terminal outcome текущего trigger
	 */
	public IngestionCycleOutcome runCycle() {
		long startedAt = System.nanoTime();
		IngestionCycleOutcome outcome = IngestionCycleOutcome.INTERNAL_FAILURE;
		activity.cycleStarted();
		try {
			outcome = runOwnedCycle();
			return outcome;
		}
		catch (IngestionInterruptedException | IndexingInterruptedException exception) {
			outcome = IngestionCycleOutcome.INTERRUPTED;
			throw exception;
		}
		catch (OperationOwnershipLostException exception) {
			outcome = IngestionCycleOutcome.OWNERSHIP_LOST;
			throw exception;
		}
		catch (ApplicationException exception) {
			outcome = IngestionCycleOutcome.EXPECTED_FAILURE;
			throw exception;
		}
		finally {
			activity.cycleFinished();
			metrics.cycleDuration(System.nanoTime() - startedAt, outcome);
		}
	}

	private IngestionCycleOutcome runOwnedCycle() {
		OperationBudget budget = budgetFactory.start(operationDeadline);
		Optional<IngestionCycleOwnership> claimed;
		try {
			claimed = cycleLedger.claim(
					GdeltSourceContract.SOURCE_NAME,
					cycleLease,
					budget);
		}
		catch (OperationDeadlineReachedException exception) {
			return IngestionCycleOutcome.DEADLINE;
		}
		if (claimed.isEmpty()) {
			return IngestionCycleOutcome.SKIPPED_ACTIVE_CYCLE;
		}

		IngestionCycleOwnership ownership = claimed.orElseThrow();
		OperationBudget guardedBudget = budget.withLeaseGuard(
				() -> readLeaseSnapshot(ownership, budget),
				LEASE_SAFETY_MARGIN);
		IngestionCycleOutcome outcome;
		try {
			guardedBudget.requireAvailable();
			outcome = map(pipelineService.runCycle(guardedBudget));
		}
		catch (OperationDeadlineReachedException exception) {
			completeAfterFailure(ownership, IngestionCycleOutcome.DEADLINE, exception);
			return IngestionCycleOutcome.DEADLINE;
		}
		catch (OperationDeadlineExceededException exception) {
			completeAfterFailure(ownership, IngestionCycleOutcome.DEADLINE, exception);
			return IngestionCycleOutcome.DEADLINE;
		}
		catch (OperationOwnershipLostException exception) {
			completeOwnershipLossIfCurrent(ownership, exception);
			throw exception;
		}
		catch (IngestionInterruptedException | IndexingInterruptedException exception) {
			completeAfterFailure(ownership, IngestionCycleOutcome.INTERRUPTED, exception);
			throw exception;
		}
		catch (ApplicationException exception) {
			completeAfterFailure(
					ownership,
					IngestionCycleOutcome.EXPECTED_FAILURE,
					exception);
			throw exception;
		}
		catch (RuntimeException exception) {
			completeAfterFailure(
					ownership,
					IngestionCycleOutcome.INTERNAL_FAILURE,
					exception);
			throw exception;
		}
		complete(ownership, outcome);
		return outcome;
	}

	private OperationLeaseSnapshot readLeaseSnapshot(
			IngestionCycleOwnership ownership,
			OperationBudget baseBudget
	) {
		try {
			return cycleLedger.remainingLease(ownership, baseBudget)
					.map(OperationLeaseSnapshot::current)
					.orElseGet(OperationLeaseSnapshot::lost);
		}
		catch (OperationOwnershipLostException exception) {
			throw exception;
		}
		catch (DataAccessException | TransactionException persistenceFailure) {
			OperationOwnershipLostException ownershipLost =
					new OperationOwnershipLostException();
			ownershipLost.addSuppressed(persistenceFailure);
			throw ownershipLost;
		}
	}

	private void complete(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome
	) {
		if (cycleLedger.complete(ownership, outcome) != AttemptTransitionResult.APPLIED) {
			throw new OperationOwnershipLostException();
		}
	}

	private void completeAfterFailure(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome,
			RuntimeException original
	) {
		try {
			complete(ownership, outcome);
		}
		catch (OperationOwnershipLostException ownershipLost) {
			ownershipLost.addSuppressed(original);
			throw ownershipLost;
		}
		catch (RuntimeException persistenceFailure) {
			if (persistenceFailure != original) {
				original.addSuppressed(persistenceFailure);
			}
		}
	}

	private void completeOwnershipLossIfCurrent(
			IngestionCycleOwnership ownership,
			OperationOwnershipLostException original
	) {
		try {
			cycleLedger.complete(ownership, IngestionCycleOutcome.OWNERSHIP_LOST);
		}
		catch (RuntimeException persistenceFailure) {
			addSuppressedOnce(original, persistenceFailure);
		}
	}

	private static void addSuppressedOnce(
			RuntimeException original,
			RuntimeException additional
	) {
		if (additional == original) {
			return;
		}
		for (Throwable suppressed : original.getSuppressed()) {
			if (suppressed == additional) {
				return;
			}
		}
		original.addSuppressed(additional);
	}

	private static IngestionCycleOutcome map(IngestionOneShotOutcome outcome) {
		return switch (Objects.requireNonNull(outcome, "pipeline outcome must not be null")) {
			case COMPLETED -> IngestionCycleOutcome.COMPLETED;
			case UNCHANGED -> IngestionCycleOutcome.UNCHANGED;
			case EXPECTED_FAILURE -> IngestionCycleOutcome.EXPECTED_FAILURE;
			case RETRY_DEFERRED -> IngestionCycleOutcome.RETRY_DEFERRED;
			case STORAGE_PRESSURE -> IngestionCycleOutcome.STORAGE_PRESSURE;
			case OPERATION_DEADLINE_EXCEEDED -> IngestionCycleOutcome.DEADLINE;
			case OWNERSHIP_LOST -> IngestionCycleOutcome.OWNERSHIP_LOST;
		};
	}

	private static Duration requirePositive(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isZero() || value.isNegative()) {
			throw new IllegalArgumentException(name + " must be positive");
		}
		return value;
	}
}
