package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.time.Duration;
import java.util.Optional;

/**
 * Транзакционная граница единственного source-scoped ingestion cycle owner.
 * Все изменяющие операции проверяют одновременно token и fencing epoch.
 */
public interface IngestionCycleLedger {

	/**
	 * Пытается получить cycle ownership на заданный срок.
	 *
	 * @param sourceName стабильное имя source
	 * @param leaseDuration длительность ownership
	 * @return новый owner либо empty, пока действующий lease занят
	 */
	Optional<IngestionCycleOwnership> claim(String sourceName, Duration leaseDuration);

	/**
	 * Пытается получить cycle ownership, не начиная запрос к PostgreSQL без
	 * целой секунды в общем лимите операции.
	 *
	 * @param sourceName стабильное имя source
	 * @param leaseDuration длительность ownership
	 * @param budget общий лимит времени, начатый до попытки claim
	 * @return новый owner либо empty, пока действующий lease занят
	 */
	Optional<IngestionCycleOwnership> claim(
			String sourceName,
			Duration leaseDuration,
			OperationBudget budget
	);

	/**
	 * Продлевает lease только для подтвержденного текущего owner.
	 *
	 * @param ownership token и fencing epoch текущего owner
	 * @param leaseDuration новый срок от текущего времени PostgreSQL
	 * @return обновленный owner либо empty после потери ownership
	 */
	Optional<IngestionCycleOwnership> renew(
			IngestionCycleOwnership ownership,
			Duration leaseDuration
	);

	/**
	 * Проверяет, что ownership еще принадлежит caller и не истек.
	 *
	 * @param ownership проверяемые token и fencing epoch
	 * @return {@code true} только для текущего непросроченного owner
	 */
	boolean isCurrent(IngestionCycleOwnership ownership);

	/**
	 * Возвращает остаток lease по часам PostgreSQL только для текущего owner.
	 *
	 * @param ownership проверяемые token и fencing epoch
	 * @return положительный остаток либо empty после expiry или takeover
	 */
	Optional<Duration> remainingLease(IngestionCycleOwnership ownership);

	/**
	 * Возвращает свежий остаток lease с учетом общего лимита времени текущего
	 * cycle. Реализация не должна начинать запрос, когда для него осталось меньше
	 * целой секунды.
	 *
	 * @param ownership проверяемые token и fencing epoch
	 * @param budget общий лимит времени, созданный до получения ownership
	 * @return положительный остаток либо empty после expiry или takeover
	 */
	Optional<Duration> remainingLease(
			IngestionCycleOwnership ownership,
			OperationBudget budget
	);

	/**
	 * Сохраняет progress текущего owner без продления lease.
	 *
	 * @param ownership token и fencing epoch текущего owner
	 * @return результат условного изменения
	 */
	AttemptTransitionResult markProgress(IngestionCycleOwnership ownership);

	/**
	 * Сохраняет bounded terminal outcome и освобождает cycle.
	 *
	 * @param ownership token и fencing epoch текущего owner
	 * @param outcome ограниченный итог cycle
	 * @return результат условного изменения
	 */
	AttemptTransitionResult complete(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome
	);

	/**
	 * Освобождает cycle без записи нового terminal outcome.
	 *
	 * @param ownership token и fencing epoch текущего owner
	 * @return результат условного изменения
	 */
	AttemptTransitionResult release(IngestionCycleOwnership ownership);

	/**
	 * Возвращает current state source.
	 *
	 * @param sourceName стабильное имя source
	 * @return state либо empty до первого claim
	 */
	Optional<IngestionCycleState> findBySourceName(String sourceName);
}
