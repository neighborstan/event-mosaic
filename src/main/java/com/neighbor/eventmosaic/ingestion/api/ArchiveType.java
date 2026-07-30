package com.neighbor.eventmosaic.ingestion.api;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import java.util.Objects;

/**
 * Поддерживаемые роли archive внутри ingestion update.
 */
public enum ArchiveType {
	TRANSLATION_EVENTS,
	TRANSLATION_MENTIONS;

	/**
	 * Отображает source-specific тип GDELT во внутренний ingestion contract.
	 *
	 * @param kind тип archive источника
	 * @return соответствующий тип ingestion
	 */
	public static ArchiveType fromGdeltKind(GdeltArchiveKind kind) {
		return switch (Objects.requireNonNull(kind, "kind must not be null")) {
			case TRANSLATION_EVENTS -> TRANSLATION_EVENTS;
			case TRANSLATION_MENTIONS -> TRANSLATION_MENTIONS;
		};
	}
}
