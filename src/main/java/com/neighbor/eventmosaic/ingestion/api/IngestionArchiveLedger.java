package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.ingestion.config.FirstRunPolicy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Транзакционная граница durable state acquisition pipeline.
 */
public interface IngestionArchiveLedger {

	/**
	 * Идемпотентно регистрирует update, его архивы и обнаруженные continuity gaps.
	 *
	 * @param update проверенный source update
	 * @param firstRunPolicy правило инициализации continuity
	 * @param firstRunStartAt фиксированная исходная точка или {@code null}
	 * @return число впервые сохраненных continuity gaps
	 */
	int registerDiscoveredUpdate(
			DiscoveredUpdate update,
			FirstRunPolicy firstRunPolicy,
			Instant firstRunStartAt
	);

	/**
	 * Пытается получить lease на обработку архива.
	 *
	 * @param idempotencyKey стабильный ключ архива
	 * @param leaseDuration срок владения попыткой
	 * @return выданная попытка или empty, если архив сейчас нельзя claim
	 */
	Optional<ArchiveAttempt> claimArchive(String idempotencyKey, Duration leaseDuration);

	/**
	 * Завершает staging только для текущего owner token.
	 *
	 * @param idempotencyKey стабильный ключ архива
	 * @param attemptToken token владельца
	 * @param stagedArchive опубликованные artifacts
	 * @return результат conditional transition
	 */
	AttemptTransitionResult markStaged(String idempotencyKey, UUID attemptToken, StagedArchive stagedArchive);

	/**
	 * Сохраняет каталогизированную ошибку только для текущего owner token.
	 *
	 * @param idempotencyKey стабильный ключ архива
	 * @param attemptToken token владельца
	 * @param failure безопасная проекция ошибки
	 * @return результат conditional transition
	 */
	default AttemptTransitionResult markFailed(
			String idempotencyKey,
			UUID attemptToken,
			IngestionFailure failure
	) {
		return markFailed(idempotencyKey, attemptToken, failure, Duration.ZERO);
	}

	/** Сохраняет failure с bounded HTTP Retry-After hint. */
	AttemptTransitionResult markFailed(
			String idempotencyKey,
			UUID attemptToken,
			IngestionFailure failure,
			Duration retryAfter
	);

	/**
	 * Находит полное durable state архива.
	 *
	 * @param idempotencyKey стабильный ключ архива
	 * @return состояние либо empty для неизвестного ключа
	 */
	Optional<IngestionArchiveState> findByIdempotencyKey(String idempotencyKey);

	/** Возвращает полный STAGED archive set half-open UTC interval. */
	List<IngestionArchiveState> findStagedBetween(Instant startAt, Instant endAt);

	/**
	 * Выбирает доступные для загрузки или индексирования архивы текущего суточного плана: сначала события, затем упоминания, от новых к старым.
	 *
	 * @param limit максимум возвращаемых архивов, от 1 до 1024
	 * @param excludedKeys ключи уже посещенных в этом цикле архивов, не больше 1024
	 * @return ограниченная выборка без завершенных архивов, будущих повторов и постоянных ошибок
	 */
	List<IngestionArchiveState> findEligibleRecentWork(int limit, Set<String> excludedKeys);

	/**
	 * Находит производное состояние run по времени source update.
	 *
	 * @param sourceUpdateTime UTC timestamp публикации
	 * @return состояние run либо empty
	 */
	Optional<IngestionRunState> findRunByUpdateTime(Instant sourceUpdateTime);

	/**
	 * Возвращает newest known update для downstream прохода без нового source poll.
	 *
	 * @return последний зарегистрированный run либо empty
	 */
	Optional<IngestionRunState> findLatestRun();

	/**
	 * Возвращает последний успешно staged update указанного типа.
	 *
	 * @param archiveType роль архива в update
	 * @return UTC timestamp progress либо empty до первого успеха
	 */
	Optional<Instant> findCompletionProgress(ArchiveType archiveType);

	/**
	 * Возвращает незакрытые разрывы наблюдаемой 15-минутной последовательности.
	 *
	 * @return gaps в хронологическом порядке
	 */
	List<IngestionGap> findOpenGaps();
}
