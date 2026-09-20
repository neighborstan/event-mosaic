package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.time.Duration;
import java.util.Optional;

/**
 * Хранит право выполнять один цикл загрузки для каждого источника данных.
 * Захват и завершение выполняются атомарно в базе, чтобы два процесса не могли
 * одновременно управлять одним циклом. Владельца определяют случайный ключ
 * и возрастающий номер захвата; завершить цикл может только действующий владелец.
 */
public interface IngestionCycleLedger {

	/**
	 * Пытается получить право выполнять загрузку на заданный срок.
	 *
	 * @param sourceName постоянное имя источника данных
	 * @param leaseDuration срок, на который процесс получает цикл
	 * @return данные нового владельца либо пустой результат, пока цикл занят
	 */
	Optional<IngestionCycleOwnership> claim(String sourceName, Duration leaseDuration);

	/**
	 * Пытается получить право на загрузку с учетом общего лимита времени.
	 * Не начинает запрос к PostgreSQL, если осталось меньше целой секунды.
	 *
	 * @param sourceName постоянное имя источника данных
	 * @param leaseDuration срок, на который процесс получает цикл
	 * @param budget общий лимит времени, отсчитываемый до попытки захвата
	 * @return данные нового владельца либо пустой результат, пока цикл занят
	 */
	Optional<IngestionCycleOwnership> claim(
			String sourceName,
			Duration leaseDuration,
			OperationBudget budget
	);

	/**
	 * Проверяет владельца и возвращает оставшийся срок его права на загрузку
	 * по часам PostgreSQL. Ключ и номер захвата должны совпадать с действующими.
	 *
	 * @param ownership данные проверяемого владельца
	 * @return положительный остаток времени либо пустой результат, если права уже нет
	 */
	Optional<Duration> remainingLease(IngestionCycleOwnership ownership);

	/**
	 * Проверяет владельца и оставшийся срок его права на загрузку с учетом общего
	 * лимита времени. Не начинает запрос, если осталось меньше целой секунды.
	 *
	 * @param ownership данные проверяемого владельца
	 * @param budget общий лимит времени, отсчитываемый до попытки захвата
	 * @return положительный остаток времени либо пустой результат, если права уже нет
	 */
	Optional<Duration> remainingLease(
			IngestionCycleOwnership ownership,
			OperationBudget budget
	);

	/**
	 * Одной транзакцией сохраняет итог и освобождает цикл для следующей загрузки.
	 * Просроченный или чужой владелец, а также повторное завершение не меняют запись.
	 *
	 * @param ownership данные владельца, который завершает загрузку
	 * @param outcome итог загрузки из поддерживаемого набора результатов
	 * @return примененное изменение либо отказ из-за потери права на цикл
	 */
	AttemptTransitionResult complete(
			IngestionCycleOwnership ownership,
			IngestionCycleOutcome outcome
	);

	/**
	 * Возвращает сохраненное состояние цикла и последний итог загрузки источника.
	 *
	 * @param sourceName постоянное имя источника данных
	 * @return состояние либо пустой результат до первого захвата
	 */
	Optional<IngestionCycleState> findBySourceName(String sourceName);
}
