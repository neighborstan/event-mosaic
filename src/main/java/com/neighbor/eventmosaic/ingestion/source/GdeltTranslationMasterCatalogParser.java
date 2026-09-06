package com.neighbor.eventmosaic.ingestion.source;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorContext;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.stereotype.Component;

/**
 * Разбирает хвостовой фрагмент полного каталога GDELT и ищет в нем пары архивов для текущего плана. Также сверяет каталог
 * с последним обновлением, которое было зафиксировано в начале цикла.
 */
@Component
public final class GdeltTranslationMasterCatalogParser {

	private final GdeltManifestLineParser lineParser;

	/**
	 * Создает разборщик, который применяет общие правила каждой строки GDELT.
	 *
	 * @param lineParser разборщик и проверка одной строки каталога
	 */
	public GdeltTranslationMasterCatalogParser(GdeltManifestLineParser lineParser) {
		this.lineParser = Objects.requireNonNull(lineParser, "lineParser must not be null");
	}

	GdeltMasterCatalogParseResult parse(
			GdeltMasterTailSnapshot snapshot,
			Set<Instant> targetTimestamps,
			DiscoveredUpdate capturedLatest
	) {
		Objects.requireNonNull(snapshot, "snapshot must not be null");
		Objects.requireNonNull(capturedLatest, "capturedLatest must not be null");
		TreeSet<Instant> targets = requireTargets(targetTimestamps, capturedLatest.sourceUpdateTime());
		Instant earliestTarget = targets.first();
		Instant frontier = capturedLatest.sourceUpdateTime();
		boolean hasTargetsAfterFrontier = targets.higher(frontier) != null;
		Map<Instant, EnumMap<ArchiveType, DiscoveredArchive>> supported = new java.util.HashMap<>();
		EnumMap<IngestionEventCode, DiscoveryDiagnostic> diagnostics =
				new EnumMap<>(IngestionEventCode.class);
		Instant earliestObserved = null;

		String[] lines = decode(snapshot.completeLines()).split("\n", -1);
		for (int index = 0; index < lines.length; index++) {
			String line = lines[index].trim();
			if (line.isEmpty()) {
				continue;
			}
			int lineNumber = index + 1;
			GdeltManifestEntry entry = parseEntry(line, lineNumber);
			if (entry.sourceUpdateTime().isPresent()) {
				Instant updateTime = entry.sourceUpdateTime().orElseThrow();
				if (earliestObserved == null || updateTime.isBefore(earliestObserved)) {
					earliestObserved = updateTime;
				}
			}
			if (entry.supportedArchive().isEmpty()) {
				diagnostics.putIfAbsent(
						IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE,
						new DiscoveryDiagnostic(
								IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE,
								lineNumber));
				continue;
			}

			DiscoveredArchive archive = entry.supportedArchive().orElseThrow();
			Instant updateTime = archive.sourceUpdateTime();
			if (updateTime.isAfter(frontier)) {
				diagnostics.putIfAbsent(
						IngestionEventCode.MASTER_AHEAD_OF_LATEST,
						new DiscoveryDiagnostic(
								IngestionEventCode.MASTER_AHEAD_OF_LATEST,
								lineNumber));
				continue;
			}
			if (!targets.contains(updateTime)) {
				continue;
			}
			EnumMap<ArchiveType, DiscoveredArchive> members = supported.computeIfAbsent(
					updateTime,
					_ -> new EnumMap<>(ArchiveType.class));
			DiscoveredArchive previous = members.putIfAbsent(archive.archiveType(), archive);
			if (previous != null) {
				IngestionErrorCode code = previous.equals(archive)
						? IngestionErrorCode.MASTER_CATALOG_DUPLICATE_ARCHIVE
						: IngestionErrorCode.MASTER_CATALOG_METADATA_CONFLICT;
				throw new GdeltMasterCatalogValidationException(
						code,
						IngestionErrorContext.atLine(lineNumber));
			}
		}

		boolean lowerBoundaryProven = snapshot.startsAtObjectBeginning()
				|| earliestObserved != null && earliestObserved.isBefore(earliestTarget);
		if (!lowerBoundaryProven) {
			return GdeltMasterCatalogParseResult.needsExpansion();
		}

		List<DiscoveredUpdate> updates = new ArrayList<>();
		Set<Instant> genuineMissing = new TreeSet<>();
		boolean frontierMatched = false;
		for (Instant target : targets) {
			Map<ArchiveType, DiscoveredArchive> members = supported.getOrDefault(
					target,
					new EnumMap<>(ArchiveType.class));
			if (members.size() == 1) {
				throw new GdeltMasterCatalogValidationException(
						IngestionErrorCode.MASTER_CATALOG_INCOMPLETE_PAIR);
			}
			if (members.isEmpty()) {
				if (target.isBefore(frontier)) {
					genuineMissing.add(target);
				}
				continue;
			}

			DiscoveredUpdate update = toUpdate(target, members);
			if (target.equals(frontier)) {
				requireLatestMatch(capturedLatest, update);
				frontierMatched = true;
			}
			updates.add(update);
		}
		updates.sort(Comparator.comparing(DiscoveredUpdate::sourceUpdateTime));
		Instant verifiedFrom = earliestObserved != null && earliestObserved.isBefore(earliestTarget)
				? earliestObserved
				: earliestTarget;
		return GdeltMasterCatalogParseResult.ready(new GdeltTranslationMasterCatalog(
				updates,
				genuineMissing,
				frontierMatched && !hasTargetsAfterFrontier
						? GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE
						: GdeltTranslationMasterCatalogStatus.PENDING,
				verifiedFrom,
				snapshot.generation(),
				snapshot.etag(),
				List.copyOf(diagnostics.values())));
	}

	private GdeltManifestEntry parseEntry(String line, int lineNumber) {
		try {
			return lineParser.parse(line, lineNumber);
		}
		catch (SourceDataViolationException exception) {
			IngestionErrorCode code = exception.errorCode()
					== IngestionErrorCode.MANIFEST_SOURCE_URI_REJECTED
					? IngestionErrorCode.MASTER_CATALOG_SOURCE_URI_REJECTED
					: IngestionErrorCode.MASTER_CATALOG_MALFORMED_LINE;
			throw new GdeltMasterCatalogValidationException(code, exception.context());
		}
	}

	private static TreeSet<Instant> requireTargets(
			Set<Instant> targetTimestamps,
			Instant frontier
	) {
		Objects.requireNonNull(targetTimestamps, "targetTimestamps must not be null");
		TreeSet<Instant> targets = new TreeSet<>(targetTimestamps);
		if (targets.isEmpty()) {
			throw new IllegalArgumentException("targetTimestamps must not be empty");
		}
		if (!targets.contains(frontier)) {
			throw new IllegalArgumentException("targetTimestamps must contain captured latest frontier");
		}
		return targets;
	}

	private static DiscoveredUpdate toUpdate(
			Instant updateTime,
			Map<ArchiveType, DiscoveredArchive> members
	) {
		DiscoveredArchive events = members.get(ArchiveType.TRANSLATION_EVENTS);
		DiscoveredArchive mentions = members.get(ArchiveType.TRANSLATION_MENTIONS);
		if (events == null || mentions == null) {
			throw new GdeltMasterCatalogValidationException(
					IngestionErrorCode.MASTER_CATALOG_INCOMPLETE_PAIR);
		}
		return new DiscoveredUpdate(updateTime, List.of(events, mentions), List.of());
	}

	private static void requireLatestMatch(
			DiscoveredUpdate capturedLatest,
			DiscoveredUpdate catalogLatest
	) {
		Map<ArchiveType, DiscoveredArchive> expected = archivesByType(capturedLatest);
		Map<ArchiveType, DiscoveredArchive> actual = archivesByType(catalogLatest);
		for (ArchiveType type : ArchiveType.values()) {
			DiscoveredArchive left = expected.get(type);
			DiscoveredArchive right = actual.get(type);
			if (!left.archiveName().equals(right.archiveName())
					|| left.expectedSizeBytes() != right.expectedSizeBytes()
					|| !left.expectedMd5().equals(right.expectedMd5())) {
				throw new GdeltMasterCatalogValidationException(
						IngestionErrorCode.MASTER_CATALOG_METADATA_CONFLICT);
			}
		}
	}

	private static Map<ArchiveType, DiscoveredArchive> archivesByType(DiscoveredUpdate update) {
		EnumMap<ArchiveType, DiscoveredArchive> result = new EnumMap<>(ArchiveType.class);
		for (DiscoveredArchive archive : update.archives()) {
			result.put(archive.archiveType(), archive);
		}
		return result;
	}

	private static String decode(byte[] bytes) {
		try {
			return StandardCharsets.UTF_8.newDecoder()
					.onMalformedInput(CodingErrorAction.REPORT)
					.onUnmappableCharacter(CodingErrorAction.REPORT)
					.decode(ByteBuffer.wrap(bytes))
					.toString();
		}
		catch (CharacterCodingException _) {
			throw new GdeltMasterCatalogValidationException(
					IngestionErrorCode.MASTER_CATALOG_MALFORMED_LINE,
					IngestionErrorContext.atLine(1));
		}
	}
}
