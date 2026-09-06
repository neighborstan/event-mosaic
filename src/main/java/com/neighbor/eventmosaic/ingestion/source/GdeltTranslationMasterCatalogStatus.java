package com.neighbor.eventmosaic.ingestion.source;

/**
 * Показывает, достаточно ли проверенного фрагмента полного каталога GDELT, чтобы считать текущий план полностью подтвержденным.
 */
public enum GdeltTranslationMasterCatalogStatus {
	PENDING,
	CATALOG_COMPLETE
}
