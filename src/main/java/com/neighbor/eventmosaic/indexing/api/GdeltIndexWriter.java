package com.neighbor.eventmosaic.indexing.api;

/**
 * Синхронная граница записи и проверки Elasticsearch read model GDELT.
 */
public interface GdeltIndexWriter {

	/**
	 * Возвращает проверенный максимальный размер одной порции.
	 *
	 * @return положительный размер bulk
	 */
	int bulkSize();

	/**
	 * Возвращает проверенный максимальный размер одной bulk-порции в байтах.
	 *
	 * @return положительная верхняя граница консервативной NDJSON-оценки
	 */
	long maxBulkBytes();

	/**
	 * Консервативно оценивает размер индексной NDJSON-операции.
	 *
	 * <p>Реализация обязана учитывать фактически сериализованный document,
	 * metadata action и оба разделителя строк.</p>
	 *
	 * @param target exact physical target, имя которого попадет в metadata action
	 * @param document индексируемый документ
	 * @return положительная оценка в байтах
	 * @throws IndexingProtocolException если document нельзя безопасно сериализовать
	 */
	long estimateBulkOperationBytes(
			ExactIndexTarget target,
			GdeltIndexedDocument document
	);

	/**
	 * Идемпотентно устанавливает версионированные шаблоны read model.
	 */
	void prepareReadModel();

	/**
	 * Индексирует одну ограниченную порцию и анализирует каждый bulk item.
	 *
	 * @param command непустая порция одного вида
	 * @return подтвержденный итог элементов
	 */
	BulkIndexResult write(BulkIndexCommand<? extends GdeltIndexedDocument> command);

	/**
	 * Делает все подтвержденные записи целевого индекса видимыми для receipt.
	 *
	 * @param kind вид целевого индекса
	 * @param target exact physical target ожидаемой generation
	 */
	void refresh(GdeltIndexKind kind, ExactIndexTarget target);

	/**
	 * Сверяет count и identity digest exact target по ключу архива и fingerprint.
	 *
	 * @param query параметры приемочной проверки
	 * @return различимый результат совпадения, расхождения или отсутствия индекса
	 */
	ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query);

}
