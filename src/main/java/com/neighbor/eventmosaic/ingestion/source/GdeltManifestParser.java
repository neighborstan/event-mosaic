package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.stereotype.Component;

/**
 * Разбирает файл GDELT со списком архивов последнего обновления. Результатом является одна проверенная
 * пара архивов событий Event и упоминаний Mention за один момент публикации.
 */
@Component
public class GdeltManifestParser {

	private final GdeltManifestLineParser lineParser;

	/**
	 * Создает разборщик, который применяет общие правила каждой строки GDELT.
	 *
	 * @param lineParser разборщик и проверка одной строки списка
	 */
	public GdeltManifestParser(GdeltManifestLineParser lineParser) {
		this.lineParser = lineParser;
	}

	/**
	 * Разбирает список и требует ровно один архив Event и один архив Mention за один момент публикации.
	 *
	 * @param manifest содержимое файла {@code lastupdate-translation.txt}
	 * @return проверенное обновление и сведения о пропущенных неподдерживаемых строках
	 */
	public DiscoveredUpdate parse(String manifest) {
		if (manifest == null) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(1));
		}
		Map<ArchiveType, DiscoveredArchive> supported = new EnumMap<>(ArchiveType.class);
		List<DiscoveryDiagnostic> diagnostics = new ArrayList<>();
		String[] lines = manifest.split("\\R", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index].trim();
			if (line.isEmpty()) {
				continue;
			}
			int lineNumber = index + 1;
			GdeltManifestEntry entry = lineParser.parse(line, lineNumber);
			if (entry.supportedArchive().isEmpty()) {
				diagnostics.add(new DiscoveryDiagnostic(
						IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE,
						lineNumber));
				continue;
			}
			DiscoveredArchive archive = entry.supportedArchive().orElseThrow();
			ArchiveType archiveType = archive.archiveType();
			if (supported.containsKey(archiveType)) {
				throw new SourceDataViolationException(IngestionErrorCode.MANIFEST_DUPLICATE_ARCHIVE);
			}
			supported.put(archiveType, archive);
		}

		DiscoveredArchive events = supported.get(ArchiveType.TRANSLATION_EVENTS);
		DiscoveredArchive mentions = supported.get(ArchiveType.TRANSLATION_MENTIONS);
		if (events == null || mentions == null) {
			throw new RemoteSourceAccessException(IngestionErrorCode.MANIFEST_REQUIRED_ARCHIVE_MISSING);
		}
		if (!events.sourceUpdateTime().equals(mentions.sourceUpdateTime())) {
			throw new SourceDataViolationException(IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH);
		}
		return new DiscoveredUpdate(
				events.sourceUpdateTime(),
				List.of(events, mentions),
				diagnostics
		);
	}

}
