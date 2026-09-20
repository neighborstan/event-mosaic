package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleLedger;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOutcome;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleOwnership;
import com.neighbor.eventmosaic.ingestion.api.IngestionCycleState;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationDeadlineReachedException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.autoconfigure.JdbcProperties;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Управляет правом на выполнение загрузки через транзакции PostgreSQL.
 * При захвате отсчитывает срок по времени базы после получения блокировки
 * строки, чтобы ожидание другого процесса не сокращало выданный срок.
 */
@Repository
public class JdbcIngestionCycleLedger implements IngestionCycleLedger {

	private final JdbcIngestionCycleRepository repository;
	private final PlatformTransactionManager transactionManager;
	private final Duration budgetedTransactionTimeout;

	/**
	 * Соединяет операции хранения состояния с транзакциями и лимитом времени запросов.
	 *
	 * @param repository операции чтения и изменения состояния цикла
	 * @param transactionManager механизм транзакций базы данных
	 * @param jdbcProperties общий лимит времени запросов приложения
	 */
	@Autowired
	public JdbcIngestionCycleLedger(
			JdbcIngestionCycleRepository repository,
			PlatformTransactionManager transactionManager,
			JdbcProperties jdbcProperties
	) {
		this(
				repository,
				transactionManager,
				Objects.requireNonNull(jdbcProperties, "jdbcProperties must not be null")
						.getTemplate()
						.getQueryTimeout());
	}

	JdbcIngestionCycleLedger(
			JdbcIngestionCycleRepository repository,
			PlatformTransactionManager transactionManager,
			Duration budgetedTransactionTimeout
	) {
		this.repository = repository;
		this.transactionManager = Objects.requireNonNull(
				transactionManager, "transactionManager must not be null");
		this.budgetedTransactionTimeout = requireAtLeastOneSecond(
				budgetedTransactionTimeout, "budgetedTransactionTimeout");
	}

	@Override
	@Transactional
	public Optional<IngestionCycleOwnership> claim(
			String sourceName,
			Duration leaseDuration
	) {
		requireSourceName(sourceName);
		requirePositive(leaseDuration);
		return repository.claim(sourceName, leaseDuration);
	}

	@Override
	public Optional<IngestionCycleOwnership> claim(
			String sourceName,
			Duration leaseDuration,
			OperationBudget budget
	) {
		requireSourceName(sourceName);
		requirePositive(leaseDuration);
		Objects.requireNonNull(budget, "budget must not be null");
		Optional<IngestionCycleOwnership> claimed = executeWithinBudget(
				budget,
				false,
				() -> repository.claim(sourceName, leaseDuration));
		try {
			budget.requireAvailable();
		}
		catch (OperationDeadlineReachedException exception) {
			// Ownership нужно вернуть coordinator даже после позднего
			// commit, чтобы он успел условно сохранить DEADLINE.
			if (claimed.isEmpty()) {
				throw exception;
			}
		}
		return claimed;
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<Duration> remainingLease(IngestionCycleOwnership ownership) {
		Objects.requireNonNull(ownership, "ownership must not be null");
		return repository.remainingLease(ownership);
	}

	@Override
	public Optional<Duration> remainingLease(
			IngestionCycleOwnership ownership,
			OperationBudget budget
	) {
		Objects.requireNonNull(ownership, "ownership must not be null");
		Objects.requireNonNull(budget, "budget must not be null");
		Optional<Duration> remaining = executeWithinBudget(
				budget,
				true,
				() -> repository.remainingLease(ownership));
		budget.requireAvailable();
		return remaining;
	}

	@Override
	@Transactional
	public AttemptTransitionResult complete(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome
	) {
		Objects.requireNonNull(ownership, "ownership must not be null");
		Objects.requireNonNull(outcome, "outcome must not be null");
		return repository.complete(ownership, outcome);
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<IngestionCycleState> findBySourceName(String sourceName) {
		requireSourceName(sourceName);
		return repository.findBySourceName(sourceName);
	}

	private static void requireSourceName(String sourceName) {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		if (sourceName.isBlank() || sourceName.length() > 64) {
			throw new IllegalArgumentException(
					"sourceName must contain between 1 and 64 characters");
		}
	}

	private static void requirePositive(Duration leaseDuration) {
		Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
		if (leaseDuration.isZero() || leaseDuration.isNegative()) {
			throw new IllegalArgumentException("leaseDuration must be positive");
		}
	}

	private <T> T executeWithinBudget(
			OperationBudget budget,
			boolean readOnly,
			Supplier<T> action
	) {
		Duration remaining = budget.requireAvailable();
		Duration effectiveTimeout = minimum(
				budgetedTransactionTimeout,
				remaining);
		long timeoutSeconds = effectiveTimeout.getSeconds();
		if (timeoutSeconds < 1) {
			throw new OperationDeadlineReachedException();
		}

		TransactionTemplate transaction = new TransactionTemplate(transactionManager);
		transaction.setReadOnly(readOnly);
		transaction.setTimeout(Math.toIntExact(timeoutSeconds));
		return transaction.execute(_ -> action.get());
	}

	private static Duration requireAtLeastOneSecond(Duration value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.compareTo(Duration.ofSeconds(1)) < 0) {
			throw new IllegalArgumentException(name + " must be at least one second");
		}
		return value;
	}

	private static Duration minimum(Duration first, Duration second) {
		return first.compareTo(second) < 0 ? first : second;
	}
}
