package com.neighbor.eventmosaic.gdelt.api;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Потоково читает GDELT Mention CSV и синхронно передает Mention records consumer.
 */
public interface GdeltMentionCsvReader {

	/**
	 * Открывает и закрывает файл внутри вызова, сохраняя source order.
	 *
	 * @param csvPath путь к Mention CSV
	 * @param consumer синхронный обработчик одной валидной Mention record
	 * @return итог успешного чтения файла
	 * @throws GdeltCsvSchemaException при нарушении schema или encoding
	 * @throws GdeltCsvAccessException при временном filesystem failure
	 * @throws GdeltCsvInterruptedException при cooperative interruption
	 */
	GdeltCsvReadSummary read(Path csvPath, GdeltRecordConsumer<GdeltMention> consumer);

	/**
	 * Читает Mention CSV и потоково сообщает source rejections.
	 *
	 * <p>Default сохраняет совместимость с существующими implementations и
	 * сообщает только terminal counter. Production reader переопределяет этот
	 * метод и сообщает каждое отклонение сразу.</p>
	 *
	 * @param csvPath путь к Mention CSV
	 * @param consumer синхронный обработчик одной валидной Mention record
	 * @param progressListener синхронный listener source rejection progress
	 * @return итог успешного чтения файла
	 */
	default GdeltCsvReadSummary read(
			Path csvPath,
			GdeltRecordConsumer<GdeltMention> consumer,
			GdeltCsvProgressListener progressListener
	) {
		Objects.requireNonNull(progressListener, "progressListener must not be null");
		GdeltCsvReadSummary summary = read(csvPath, consumer);
		progressListener.onInvalidRecords(summary.invalidRecords());
		return summary;
	}
}
