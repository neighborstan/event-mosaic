package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.stereotype.Component;

/**
 * Разбирает GDELT Translation manifest в один целостный Event/Mention update.
 */
@Component
public class GdeltManifestParser {

	/**
	 * Разбирает manifest и требует ровно один Event и один Mention archive одного timestamp.
	 *
	 * @param manifest содержимое {@code lastupdate-translation.txt}
	 * @return валидированный source update и диагностика неподдерживаемых строк
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
			ParsedLine parsedLine = parseLine(line, lineNumber);
			Optional<GdeltArchiveName> classified = classify(parsedLine.uri(), lineNumber);
			if (classified.isEmpty()) {
				diagnostics.add(new DiscoveryDiagnostic(
						IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE,
						lineNumber));
				continue;
			}
			GdeltArchiveName archiveName = classified.orElseThrow();
			ArchiveType archiveType = ArchiveType.fromGdeltKind(archiveName.kind());
			if (supported.containsKey(archiveType)) {
				throw new SourceDataViolationException(IngestionErrorCode.MANIFEST_DUPLICATE_ARCHIVE);
			}
			DiscoveredArchive archive = new DiscoveredArchive(
					archiveName.updateTime(),
					archiveName.value(),
					parsedLine.uri(),
					parsedLine.md5(),
					archiveType,
					parsedLine.sizeBytes()
			);
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

	private static ParsedLine parseLine(String line, int lineNumber) {
		String[] fields = line.split("\\s+", -1);
		if (fields.length != 3) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		long sizeBytes;
		try {
			sizeBytes = Long.parseLong(fields[0]);
		} catch (NumberFormatException _) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		if (sizeBytes <= 0) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		String md5;
		try {
			md5 = GdeltSourceContract.normalizeMd5(fields[1]);
		} catch (IllegalArgumentException _) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		URI uri;
		try {
			uri = new URI(fields[2]);
		} catch (URISyntaxException _) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		if (!uri.isAbsolute()) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
		return new ParsedLine(sizeBytes, md5, uri);
	}

	private static Optional<GdeltArchiveName> classify(URI uri, int lineNumber) {
		String fileName;
		try {
			fileName = GdeltSourceContract.requireMetadataFileName(uri);
		} catch (IllegalArgumentException _) {
			throw new SourceDataViolationException(IngestionErrorCode.MANIFEST_SOURCE_URI_REJECTED);
		}
		try {
			return GdeltArchiveName.parseSupported(fileName);
		} catch (IllegalArgumentException _) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_MALFORMED_LINE,
					IngestionErrorContext.atLine(lineNumber));
		}
	}

	private record ParsedLine(long sizeBytes, String md5, URI uri) {
	}
}
