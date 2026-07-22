package com.neighbor.eventmosaic.ingestion.api;

import java.util.Objects;

/**
 * Информационная диагностика одной проигнорированной строки manifest.
 *
 * @param code стабильный informational code
 * @param lineNumber номер строки manifest, начиная с единицы
 */
public record DiscoveryDiagnostic(IngestionEventCode code, int lineNumber) {

	/**
	 * Проверяет наличие кода и положительный номер строки manifest.
	 */
	public DiscoveryDiagnostic {
		Objects.requireNonNull(code, "code must not be null");
		if (lineNumber <= 0) {
			throw new IllegalArgumentException("lineNumber must be positive");
		}
	}
}
