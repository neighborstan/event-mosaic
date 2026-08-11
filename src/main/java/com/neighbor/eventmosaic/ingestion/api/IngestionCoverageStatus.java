package com.neighbor.eventmosaic.ingestion.api;

/** Описывает, насколько надежно подтверждена полнота Event-данных выбранного окна. */
public enum IngestionCoverageStatus {

	/** Все ожидаемые Event-слоты подтверждены текущими проверенными receipts. */
	COMPLETE,

	/** Evidence доступен, но хотя бы один ожидаемый Event-слот не подтвержден. */
	PARTIAL,

	/** Полноту нельзя определить по доступному состоянию ingestion. */
	UNKNOWN
}
