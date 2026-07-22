package com.neighbor.eventmosaic.ingestion.api;

import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Целостный GDELT update с обязательной парой Event/Mention и diagnostics.
 *
 * @param sourceUpdateTime общий UTC timestamp публикации
 * @param archives ровно по одному Event и Mention archive
 * @param diagnostics безопасные сведения о проигнорированных строках manifest
 */
public record DiscoveredUpdate(
		Instant sourceUpdateTime,
		List<DiscoveredArchive> archives,
		List<DiscoveryDiagnostic> diagnostics
) {

	/**
	 * Создает immutable snapshot и проверяет полноту пары архивов.
	 */
	public DiscoveredUpdate {
		Objects.requireNonNull(sourceUpdateTime, "sourceUpdateTime must not be null");
		archives = List.copyOf(archives);
		diagnostics = List.copyOf(diagnostics);
		Set<ArchiveType> types = EnumSet.noneOf(ArchiveType.class);
		for (DiscoveredArchive archive : archives) {
			if (!sourceUpdateTime.equals(archive.sourceUpdateTime())) {
				throw new IllegalArgumentException("all archives must share sourceUpdateTime");
			}
			types.add(archive.archiveType());
		}
		if (archives.size() != 2 || !types.equals(Set.of(
				ArchiveType.TRANSLATION_EVENTS,
				ArchiveType.TRANSLATION_MENTIONS))) {
			throw new IllegalArgumentException("update must contain exactly one Event and one Mention archive");
		}
	}
}
