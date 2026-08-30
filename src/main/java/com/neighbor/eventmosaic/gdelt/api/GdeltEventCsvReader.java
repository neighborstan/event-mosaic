package com.neighbor.eventmosaic.gdelt.api;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Потоково читает GDELT Event CSV и синхронно передает Event records consumer.
 */
public interface GdeltEventCsvReader {

	/**
	 * Открывает и закрывает файл внутри вызова, сохраняя source order.
	 *
	 * @param csvPath путь к Event CSV
	 * @param consumer синхронный обработчик одной валидной Event record
	 * @return итог успешного чтения файла
	 * @throws GdeltCsvSchemaException при нарушении schema или encoding
	 * @throws GdeltCsvAccessException при временном filesystem failure
	 * @throws GdeltCsvInterruptedException при cooperative interruption
	 */
	GdeltCsvReadSummary read(Path csvPath, GdeltRecordConsumer<GdeltEvent> consumer);

	/**
	 * Читает Event CSV в пределах общего ограничения времени и проверяет
	 * владение циклом до открытия файла. Основная реализация повторяет проверку
	 * во время потокового чтения.
	 *
	 * @param csvPath путь к Event CSV
	 * @param consumer синхронный обработчик одной валидной Event record
	 * @param budget общая граница времени и проверка владения циклом
	 * @return итог успешного чтения файла
	 */
	default GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltEvent> consumer,
			OperationBudget budget
	) {
		Objects.requireNonNull(budget, "budget must not be null").requireAvailable();
		return read(csvPath, consumer);
	}

	/**
	 * Читает Event CSV и потоково сообщает source rejections.
	 *
	 * <p>Default сохраняет совместимость с существующими implementations и
	 * сообщает только terminal counter. Production reader переопределяет этот
	 * метод и сообщает каждое отклонение сразу.</p>
	 *
	 * @param csvPath путь к Event CSV
	 * @param consumer синхронный обработчик одной валидной Event record
	 * @param progressListener синхронный listener source rejection progress
	 * @return итог успешного чтения файла
	 */
	default GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltEvent> consumer,
			GdeltCsvProgressListener progressListener
	) {
		Objects.requireNonNull(progressListener, "progressListener must not be null");
		GdeltCsvReadSummary summary = read(csvPath, consumer);
		progressListener.onInvalidRecords(summary.invalidRecords());
		return summary;
	}

	/**
	 * Читает Event CSV, сообщая об отклоненных строках и проверяя владение циклом.
	 *
	 * @param csvPath путь к Event CSV
	 * @param consumer синхронный обработчик одной валидной Event record
	 * @param progressListener синхронный обработчик числа отклоненных строк
	 * @param budget общая граница времени и проверка владения циклом
	 * @return итог успешного чтения файла
	 */
	default GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltEvent> consumer,
			GdeltCsvProgressListener progressListener,
			OperationBudget budget
	) {
		Objects.requireNonNull(progressListener, "progressListener must not be null");
		OperationBudget checkedBudget = Objects.requireNonNull(
				budget, "budget must not be null");
		checkedBudget.requireAvailable();
		GdeltCsvReadSummary summary = read(csvPath, consumer, progressListener);
		checkedBudget.requireAvailable();
		return summary;
	}
}
