package com.neighbor.eventmosaic.ingestion.state;

import com.neighbor.eventmosaic.ingestion.api.AttemptTransitionResult;
import com.neighbor.eventmosaic.ingestion.api.IngestionFailure;
import com.neighbor.eventmosaic.ingestion.api.SourcePollAttempt;
import com.neighbor.eventmosaic.ingestion.api.SourcePollLedger;
import com.neighbor.eventmosaic.ingestion.api.SourcePollState;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * Транзакционный facade current source-poll state с единым application Clock.
 */
@Repository
public class JdbcSourcePollLedger implements SourcePollLedger {

	private final JdbcSourcePollRepository repository;
	private final BackendDataProperties properties;
	private final Clock clock;

	/**
	 * Создает facade из JDBC repository, retry policy и UTC Clock.
	 *
	 * @param repository repository current poll state
	 * @param properties проверенные Backend Data defaults
	 * @param clock единый production source времени
	 */
	public JdbcSourcePollLedger(
			JdbcSourcePollRepository repository,
			BackendDataProperties properties,
			Clock clock
	) {
		this.repository = repository;
		this.properties = properties;
		this.clock = clock;
	}

	@Override
	@Transactional
	public SourcePollState register(String sourceName) {
		requireSourceName(sourceName);
		return repository.register(
				sourceName,
				properties.retry().automaticRetryLimit(),
				clock.instant());
	}

	@Override
	@Transactional
	public Optional<SourcePollAttempt> claim(String sourceName, Duration leaseDuration) {
		requireSourceName(sourceName);
		requirePositive(leaseDuration);
		return repository.claim(sourceName, leaseDuration, clock.instant());
	}

	@Override
	@Transactional
	public AttemptTransitionResult markSucceeded(String sourceName, UUID attemptToken) {
		requireSourceName(sourceName);
		Objects.requireNonNull(attemptToken, "attemptToken must not be null");
		return repository.markSucceeded(sourceName, attemptToken, clock.instant());
	}

	@Override
	@Transactional
	public AttemptTransitionResult markFailed(
			String sourceName,
			UUID attemptToken,
			IngestionFailure failure,
			Duration retryAfter
	) {
		requireSourceName(sourceName);
		Objects.requireNonNull(attemptToken, "attemptToken must not be null");
		Objects.requireNonNull(failure, "failure must not be null");
		Objects.requireNonNull(retryAfter, "retryAfter must not be null");
		if (retryAfter.isNegative()) {
			throw new IllegalArgumentException("retryAfter must not be negative");
		}
		return repository.markFailed(
				sourceName,
				attemptToken,
				failure,
				retryAfter,
				clock.instant());
	}

	@Override
	@Transactional(readOnly = true)
	public Optional<SourcePollState> findBySourceName(String sourceName) {
		requireSourceName(sourceName);
		return repository.findBySourceName(sourceName);
	}

	private static void requireSourceName(String sourceName) {
		Objects.requireNonNull(sourceName, "sourceName must not be null");
		if (sourceName.isBlank() || sourceName.length() > 64) {
			throw new IllegalArgumentException("sourceName must contain between 1 and 64 characters");
		}
	}

	private static void requirePositive(Duration leaseDuration) {
		Objects.requireNonNull(leaseDuration, "leaseDuration must not be null");
		if (leaseDuration.isZero() || leaseDuration.isNegative()) {
			throw new IllegalArgumentException("leaseDuration must be positive");
		}
	}
}
