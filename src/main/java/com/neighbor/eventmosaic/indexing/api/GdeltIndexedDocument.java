package com.neighbor.eventmosaic.indexing.api;

/**
 * Общий контракт неизменяемого документа read model GDELT.
 */
public sealed interface GdeltIndexedDocument
		permits IndexedEventDocument, IndexedMentionDocument {

	/**
	 * Возвращает вид документа.
	 *
	 * @return ограниченный вид индекса
	 */
	GdeltIndexKind kind();

	/**
	 * Возвращает детерминированный Elasticsearch {@code _id}.
	 *
	 * @return идентификатор документа
	 */
	String documentId();

	/**
	 * Возвращает точный ключ исходного архива.
	 *
	 * @return ключ архива без нормализации
	 */
	String sourceArchiveKey();

	/**
	 * Возвращает физический номер строки в исходном CSV.
	 *
	 * @return положительный номер строки
	 */
	long sourceLineNumber();

}
