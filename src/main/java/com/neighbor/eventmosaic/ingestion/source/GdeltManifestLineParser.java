package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveName;
import com.neighbor.eventmosaic.gdelt.api.GdeltSourceContract;
import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.stereotype.Component;

/**
 * Разбирает одну строку списка архивов GDELT. Одинаково проверяет размер, контрольную сумму MD5,
 * URI и имя файла как для последнего обновления, так и для полного каталога.
 */
@Component
public final class GdeltManifestLineParser {

	private static final Pattern TIMESTAMPED_TRANSLATION_NAME = Pattern.compile(
			"^(\\d{14})\\.translation\\..+$");

	GdeltManifestEntry parse(String line, int lineNumber) {
		String[] fields = line.split("\\s+", -1);
		if (fields.length != 3) {
			throw malformed(lineNumber);
		}
		long sizeBytes;
		try {
			sizeBytes = Long.parseLong(fields[0]);
		}
		catch (NumberFormatException _) {
			throw malformed(lineNumber);
		}
		if (sizeBytes <= 0) {
			throw malformed(lineNumber);
		}

		String md5;
		try {
			md5 = GdeltSourceContract.normalizeMd5(fields[1]);
		}
		catch (IllegalArgumentException _) {
			throw malformed(lineNumber);
		}

		URI uri;
		try {
			uri = new URI(fields[2]);
		}
		catch (URISyntaxException _) {
			throw malformed(lineNumber);
		}
		if (!uri.isAbsolute()) {
			throw malformed(lineNumber);
		}

		String fileName;
		try {
			fileName = GdeltSourceContract.requireMetadataFileName(uri);
		}
		catch (IllegalArgumentException _) {
			throw new SourceDataViolationException(
					IngestionErrorCode.MANIFEST_SOURCE_URI_REJECTED);
		}

		Optional<GdeltArchiveName> archiveName;
		try {
			archiveName = GdeltArchiveName.parseSupported(fileName);
		}
		catch (IllegalArgumentException _) {
			throw malformed(lineNumber);
		}
		if (archiveName.isPresent()) {
			GdeltArchiveName supportedName = archiveName.orElseThrow();
			DiscoveredArchive archive = new DiscoveredArchive(
					supportedName.updateTime(),
					supportedName.value(),
					uri,
					md5,
					ArchiveType.fromGdeltKind(supportedName.kind()),
					sizeBytes);
			return new GdeltManifestEntry(
					Optional.of(supportedName.updateTime()),
					Optional.of(archive));
		}

		return new GdeltManifestEntry(
				parseUnsupportedUpdateTime(fileName, lineNumber),
				Optional.empty());
	}

	private static Optional<Instant> parseUnsupportedUpdateTime(
			String fileName,
			int lineNumber
	) {
		Matcher matcher = TIMESTAMPED_TRANSLATION_NAME.matcher(fileName);
		if (!matcher.matches()) {
			return Optional.empty();
		}
		try {
			return Optional.of(GdeltSourceContract.parseUpdateTimestamp(matcher.group(1)));
		}
		catch (IllegalArgumentException _) {
			throw malformed(lineNumber);
		}
	}

	private static SourceDataViolationException malformed(int lineNumber) {
		return new SourceDataViolationException(
				IngestionErrorCode.MANIFEST_MALFORMED_LINE,
				IngestionErrorContext.atLine(lineNumber));
	}
}
