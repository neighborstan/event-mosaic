package com.neighbor.eventmosaic.gdelt.api;

import java.nio.file.Path;

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
}
