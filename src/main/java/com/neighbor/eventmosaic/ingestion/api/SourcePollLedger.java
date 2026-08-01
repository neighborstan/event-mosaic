package com.neighbor.eventmosaic.ingestion.api;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/**
 * Транзакционная граница current source-poll ownership и delayed retry state.
 */
public interface SourcePollLedger {

	/**
	 * Идемпотентно создает current state source с configured retry limit.
	 *
	 * @param sourceName стабильное имя source
	 * @return созданное или существующее state
	 */
	SourcePollState register(String sourceName);

	/**
	 * Пытается атомарно получить poll ownership с учетом due time и budget.
	 *
	 * @param sourceName стабильное имя source
	 * @param leaseDuration длительность ownership
	 * @return attempt либо empty для недоступного current state
	 */
	Optional<SourcePollAttempt> claim(String sourceName, Duration leaseDuration);

	/**
	 * Завершает successful poll и сбрасывает текущую retry sequence.
	 *
	 * @param sourceName стабильное имя source
	 * @param attemptToken token текущего owner
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markSucceeded(String sourceName, UUID attemptToken);

	/**
	 * Сохраняет безопасную failure projection текущего owner.
	 *
	 * @param sourceName стабильное имя source
	 * @param attemptToken token текущего owner
	 * @param failure безопасная failure projection
	 * @return результат conditional transition
	 */
	default AttemptTransitionResult markFailed(
			String sourceName,
			UUID attemptToken,
			IngestionFailure failure
	) {
		return markFailed(sourceName, attemptToken, failure, Duration.ZERO);
	}

	/**
	 * Сохраняет failure и учитывает корректный HTTP Retry-After hint.
	 *
	 * @param sourceName стабильное имя source
	 * @param attemptToken token текущего owner
	 * @param failure безопасная failure projection
	 * @param retryAfter серверная нижняя граница задержки либо zero
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markFailed(
			String sourceName,
			UUID attemptToken,
			IngestionFailure failure,
			Duration retryAfter
	);

	/**
	 * Возвращает current source-poll state.
	 *
	 * @param sourceName стабильное имя source
	 * @return state либо empty до регистрации
	 */
	Optional<SourcePollState> findBySourceName(String sourceName);
}
