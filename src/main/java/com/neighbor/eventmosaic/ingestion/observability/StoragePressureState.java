package com.neighbor.eventmosaic.ingestion.observability;

/** Результат сравнения доступного места с настроенным резервом. */
public enum StoragePressureState {

	/** Доступное место не меньше настроенного резерва. */
	AVAILABLE,

	/** Доступное место меньше настроенного резерва. */
	PRESSURE,

	/** Безопасно определить доступное место не удалось. */
	UNAVAILABLE
}
