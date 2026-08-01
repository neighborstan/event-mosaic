package com.neighbor.eventmosaic.indexing.api;

import java.util.List;
import java.util.Objects;

/**
 * Ограниченная непустая порция документов одного вида.
 *
 * @param kind вид документов
 * @param target exact physical target подтвержденной generation
 * @param writeMode обычная запись либо заполнение теневой rebuild generation
 * @param documents документы порции
 * @param <T> конкретный тип индексируемого документа
 */
public record BulkIndexCommand<T extends GdeltIndexedDocument>(
		GdeltIndexKind kind,
		ExactIndexTarget target,
		IndexWriteMode writeMode,
		List<T> documents
) {

	/**
	 * Создает защищенную копию порции и проверяет единый вид документов.
	 */
	public BulkIndexCommand {
		Objects.requireNonNull(kind, "kind must not be null");
		Objects.requireNonNull(target, "target must not be null");
		Objects.requireNonNull(writeMode, "writeMode must not be null");
		if (!kind.accepts(target)) {
			throw new IllegalArgumentException("target must match command kind");
		}
		documents = List.copyOf(Objects.requireNonNull(documents, "documents must not be null"));
		if (documents.isEmpty()) {
			throw new IllegalArgumentException("documents must not be empty");
		}
		for (T document : documents) {
			if (document.kind() != kind) {
				throw new IllegalArgumentException("all documents must match command kind");
			}
		}
	}

	/** Создает обычную порцию для текущей ACTIVE generation. */
	public BulkIndexCommand(
			GdeltIndexKind kind,
			ExactIndexTarget target,
			List<T> documents
	) {
		this(kind, target, IndexWriteMode.ACTIVE, documents);
	}

}
