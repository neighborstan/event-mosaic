package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Хранит результат проверки хвостового фрагмента полного каталога GDELT. Результат показывает, какие пары архивов
 * найдены для текущего плана и достаточно ли прочитанного фрагмента для окончательного вывода.
 *
 * @param updates найденные полные пары архивов Event и Mention
 * @param genuineMissingTargets моменты, для которых в проверенной версии каталога действительно нет пары архивов
 * @param status достаточно ли прочитанной части каталога для всего плана
 * @param verifiedFrom самый ранний момент, попавший в проверенный фрагмент
 * @param generation версия файла в Google Cloud Storage
 * @param etag дополнительный идентификатор той же версии файла
 * @param diagnostics информационные события о пропущенных неподдерживаемых строках
 */
public record GdeltTranslationMasterCatalog(
		List<DiscoveredUpdate> updates,
		Set<Instant> genuineMissingTargets,
		GdeltTranslationMasterCatalogStatus status,
		Instant verifiedFrom,
		String generation,
		String etag,
		List<DiscoveryDiagnostic> diagnostics
) {

	/** Копирует коллекции и проверяет обязательные сведения о версии каталога. */
	public GdeltTranslationMasterCatalog {
		updates = List.copyOf(updates);
		genuineMissingTargets = Collections.unmodifiableSet(
				new TreeSet<>(genuineMissingTargets));
		status = Objects.requireNonNull(status, "status must not be null");
		verifiedFrom = Objects.requireNonNull(verifiedFrom, "verifiedFrom must not be null");
		generation = requireText(generation, "generation");
		etag = requireText(etag, "etag");
		diagnostics = List.copyOf(diagnostics);
	}

	private static String requireText(String value, String name) {
		Objects.requireNonNull(value, name + " must not be null");
		if (value.isBlank()) {
			throw new IllegalArgumentException(name + " must not be blank");
		}
		return value;
	}
}
