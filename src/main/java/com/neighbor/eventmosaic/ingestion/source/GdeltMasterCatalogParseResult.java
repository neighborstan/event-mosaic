package com.neighbor.eventmosaic.ingestion.source;

import java.util.Objects;

/**
 * Сообщает, достаточно ли прочитанного хвостового фрагмента полного каталога GDELT. Если нужные данные не поместились,
 * вызывающий код прочитает больший фрагмент той же версии файла.
 */
record GdeltMasterCatalogParseResult(
		Status status,
		GdeltTranslationMasterCatalog catalog
) {

	enum Status {
		NEEDS_EXPANSION,
		READY
	}

	GdeltMasterCatalogParseResult {
		Objects.requireNonNull(status, "status must not be null");
		if ((status == Status.READY) != (catalog != null)) {
			throw new IllegalArgumentException("ready result must contain catalog only");
		}
	}

	static GdeltMasterCatalogParseResult needsExpansion() {
		return new GdeltMasterCatalogParseResult(Status.NEEDS_EXPANSION, null);
	}

	static GdeltMasterCatalogParseResult ready(GdeltTranslationMasterCatalog catalog) {
		return new GdeltMasterCatalogParseResult(
				Status.READY,
				Objects.requireNonNull(catalog, "catalog must not be null"));
	}

	boolean requiresExpansion() {
		return status == Status.NEEDS_EXPANSION;
	}

	GdeltTranslationMasterCatalog requireCatalog() {
		return Objects.requireNonNull(catalog, "catalog is not ready");
	}
}
