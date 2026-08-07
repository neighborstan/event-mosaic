package com.neighbor.eventmosaic.ingestion.observability;

/** Читает bounded operational state без изменения lifecycle данных. */
public interface BackendDataOperationalState {

	/** Выполняет свежую проверку PostgreSQL и stable aliases. */
	BackendDataOperationalSnapshot observe();

	/** Возвращает коротко кешированную проекцию для серии Micrometer gauges. */
	BackendDataOperationalSnapshot current();
}
