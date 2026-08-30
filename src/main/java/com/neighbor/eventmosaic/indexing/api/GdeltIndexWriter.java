package com.neighbor.eventmosaic.indexing.api;

import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.util.Objects;

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
	 * Идемпотентно устанавливает шаблоны в пределах текущего cycle budget.
	 *
	 * @param budget общий deadline и ownership guard ingestion cycle
	 */
	default void prepareReadModel(OperationBudget budget) {
		Objects.requireNonNull(budget, "budget must not be null");
		prepareReadModel();
	}

	/**
	 * Индексирует одну ограниченную порцию и анализирует каждый bulk item.
	 *
	 * @param command непустая порция одного вида
	 * @return подтвержденный итог элементов
	 */
	BulkIndexResult write(BulkIndexCommand<? extends GdeltIndexedDocument> command);

	/**
	 * Индексирует порцию только пока общий cycle разрешает внешние запросы.
	 *
	 * @param command непустая порция одного вида
	 * @param budget общий deadline и ownership guard ingestion cycle
	 * @return подтвержденный итог элементов
	 */
	default BulkIndexResult write(
			BulkIndexCommand<? extends GdeltIndexedDocument> command,
			OperationBudget budget
	) {
		Objects.requireNonNull(budget, "budget must not be null");
		return write(command);
	}

	/**
	 * Делает все подтвержденные записи целевого индекса видимыми для receipt.
	 *
	 * @param kind вид целевого индекса
	 * @param target exact physical target ожидаемой generation
	 */
	void refresh(GdeltIndexKind kind, ExactIndexTarget target);

	/**
	 * Делает записи видимыми, если текущий cycle все еще владеет lease.
	 *
	 * @param kind вид целевого индекса
	 * @param target exact physical target ожидаемой generation
	 * @param budget общий deadline и ownership guard ingestion cycle
	 */
	default void refresh(
			GdeltIndexKind kind,
			ExactIndexTarget target,
			OperationBudget budget
	) {
		Objects.requireNonNull(budget, "budget must not be null");
		refresh(kind, target);
	}

	/**
	 * Сверяет count и identity digest exact target по ключу архива и fingerprint.
	 *
	 * @param query параметры приемочной проверки
	 * @return различимый результат совпадения, расхождения или отсутствия индекса
	 */
	ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query);

	/**
	 * Сверяет receipt через bounded PIT-страницы в пределах текущего cycle.
	 *
	 * @param query параметры приемочной проверки
	 * @param budget общий deadline и ownership guard ingestion cycle
	 * @return различимый результат проверки
	 */
	default ArchiveReceiptVerification verifyReceipt(
			ArchiveReceiptQuery query,
			OperationBudget budget
	) {
		Objects.requireNonNull(budget, "budget must not be null");
		return verifyReceipt(query);
	}

}
