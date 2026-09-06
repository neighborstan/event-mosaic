package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Хранит результат проверки одной строки из файла GDELT со списком архивов. Результат позже используется,
 * чтобы собрать полную пару архивов событий Event и упоминаний Mention.
 *
 * @param sourceUpdateTime время публикации, если его удалось распознать в имени файла
 * @param supportedArchive описание архива Event или Mention; пустое значение для остальных видов файлов
 */
record GdeltManifestEntry(
		Optional<Instant> sourceUpdateTime,
		Optional<DiscoveredArchive> supportedArchive
) {

	GdeltManifestEntry {
		sourceUpdateTime = Objects.requireNonNull(
				sourceUpdateTime, "sourceUpdateTime must not be null");
		supportedArchive = Objects.requireNonNull(
				supportedArchive, "supportedArchive must not be null");
		if (supportedArchive.isPresent()) {
			DiscoveredArchive archive = supportedArchive.orElseThrow();
			if (sourceUpdateTime.isEmpty()
					|| !sourceUpdateTime.orElseThrow().equals(archive.sourceUpdateTime())) {
				throw new IllegalArgumentException(
						"supported archive must have the same source update time");
			}
		}
	}
}
