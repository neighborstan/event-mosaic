package com.neighbor.eventmosaic.gdelt.api;

import java.nio.file.Path;

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
}
