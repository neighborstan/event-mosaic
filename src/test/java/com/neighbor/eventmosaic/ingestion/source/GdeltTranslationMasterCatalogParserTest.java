package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredUpdate;
import com.neighbor.eventmosaic.ingestion.api.DiscoveryDiagnostic;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Разбор ограниченного хвоста GDELT master catalog")
class GdeltTranslationMasterCatalogParserTest {

	private static final DateTimeFormatter SOURCE_TIME = DateTimeFormatter
			.ofPattern("yyyyMMddHHmmss")
			.withZone(ZoneOffset.UTC);
	private static final String EVENT_MD5 = "0123456789abcdef0123456789abcdef";
	private static final String MENTION_MD5 = "fedcba9876543210fedcba9876543210";
	private static final Instant OLDER = Instant.parse("2026-08-30T11:30:00Z");
	private static final Instant HISTORICAL_MISSING = Instant.parse("2026-08-30T11:45:00Z");
	private static final Instant HISTORICAL_PRESENT = Instant.parse("2026-08-30T12:00:00Z");
	private static final Instant FRONTIER = Instant.parse("2026-08-30T12:15:00Z");
	private static final Instant AFTER_FRONTIER = Instant.parse("2026-08-30T12:30:00Z");

	private GdeltTranslationMasterCatalogParser parser;

	@BeforeEach
	void setUp() {
		parser = new GdeltTranslationMasterCatalogParser(new GdeltManifestLineParser());
	}

	@Test
	@DisplayName("Целевые пары проходят, реальный пропуск сохраняется, а GKG и более новый master ограниченно диагностируются")
	void buildsExactCatalogWithGenuineMissingAndBoundedDiagnostics() {
		String body = gkgLine(OLDER)
				+ gkgLine(HISTORICAL_MISSING)
				+ pair(HISTORICAL_PRESENT, 100, 200)
				+ pair(FRONTIER, 100, 200)
				+ pair(AFTER_FRONTIER, 100, 200);

		GdeltTranslationMasterCatalog catalog = parseReady(
				body,
				Set.of(HISTORICAL_MISSING, HISTORICAL_PRESENT, FRONTIER),
				latest());

		assertThat(catalog.status())
				.isEqualTo(GdeltTranslationMasterCatalogStatus.CATALOG_COMPLETE);
		assertThat(catalog.updates())
				.extracting(DiscoveredUpdate::sourceUpdateTime)
				.containsExactly(HISTORICAL_PRESENT, FRONTIER);
		assertThat(catalog.genuineMissingTargets()).containsExactly(HISTORICAL_MISSING);
		assertThat(catalog.diagnostics())
				.extracting(DiscoveryDiagnostic::code)
				.containsExactlyInAnyOrder(
						IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE,
						IngestionEventCode.MASTER_AHEAD_OF_LATEST);
	}

	@Test
	@DisplayName("Хвост без доказанной строки раньше первого target требует полной замены более широким диапазоном")
	void requestsExpansionUntilLowerBoundaryIsProven() {
		GdeltMasterCatalogParseResult result = parser.parse(
				snapshot(pair(HISTORICAL_PRESENT, 100, 200) + pair(FRONTIER, 100, 200), 10),
				Set.of(HISTORICAL_PRESENT, FRONTIER),
				latest());

		assertThat(result.requiresExpansion()).isTrue();
	}

	@Test
	@DisplayName("Отсутствие обоих архивов captured frontier означает отставание master и оставляет catalog ожидающим")
	void keepsCatalogPendingWhenFrontierPairIsMissing() {
		GdeltTranslationMasterCatalog catalog = parseReady(
				gkgLine(OLDER) + pair(HISTORICAL_PRESENT, 100, 200),
				Set.of(HISTORICAL_PRESENT, FRONTIER),
				latest());

		assertThat(catalog.status()).isEqualTo(GdeltTranslationMasterCatalogStatus.PENDING);
		assertThat(catalog.genuineMissingTargets()).isEmpty();
		assertThat(catalog.updates())
				.extracting(DiscoveredUpdate::sourceUpdateTime)
				.containsExactly(HISTORICAL_PRESENT);
	}

	@Test
	@DisplayName("Target позже stale captured frontier не считается пропуском и удерживает catalog ожидающим")
	void keepsCatalogPendingForTargetsAfterStaleFrontier() {
		GdeltTranslationMasterCatalog catalog = parseReady(
				gkgLine(OLDER) + pair(FRONTIER, 100, 200),
				Set.of(HISTORICAL_PRESENT, FRONTIER, AFTER_FRONTIER),
				latest());

		assertThat(catalog.status()).isEqualTo(GdeltTranslationMasterCatalogStatus.PENDING);
		assertThat(catalog.genuineMissingTargets()).containsExactly(HISTORICAL_PRESENT);
		assertThat(catalog.genuineMissingTargets()).doesNotContain(AFTER_FRONTIER);
		assertThat(catalog.updates())
				.extracting(DiscoveredUpdate::sourceUpdateTime)
				.containsExactly(FRONTIER);
	}

	@Test
	@DisplayName("Один архив целевого времени отклоняет весь catalog как неполную пару")
	void rejectsIncompleteTargetPair() {
		String body = gkgLine(OLDER)
				+ archiveLine(HISTORICAL_PRESENT, ArchiveType.TRANSLATION_EVENTS, 100, EVENT_MD5)
				+ pair(FRONTIER, 100, 200);

		assertFailure(
				body,
				Set.of(HISTORICAL_PRESENT, FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_INCOMPLETE_PAIR);
	}

	@Test
	@DisplayName("Один архив captured frontier отклоняет весь catalog как неполную пару")
	void rejectsIncompleteFrontierPair() {
		String body = gkgLine(OLDER)
				+ archiveLine(FRONTIER, ArchiveType.TRANSLATION_EVENTS, 100, EVENT_MD5);

		assertFailure(
				body,
				Set.of(FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_INCOMPLETE_PAIR);
	}

	@Test
	@DisplayName("Повтор одинакового поддерживаемого архива отклоняется отдельным кодом")
	void rejectsDuplicateSupportedArchive() {
		String event = archiveLine(
				HISTORICAL_PRESENT,
				ArchiveType.TRANSLATION_EVENTS,
				100,
				EVENT_MD5);
		String body = gkgLine(OLDER)
				+ event
				+ event
				+ archiveLine(
						HISTORICAL_PRESENT,
						ArchiveType.TRANSLATION_MENTIONS,
						200,
						MENTION_MD5)
				+ pair(FRONTIER, 100, 200);

		assertFailure(
				body,
				Set.of(HISTORICAL_PRESENT, FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_DUPLICATE_ARCHIVE);
	}

	@Test
	@DisplayName("Две разные метаданные одного архива отклоняются как отдельный конфликт")
	void rejectsConflictingDuplicateMetadata() {
		String body = gkgLine(OLDER)
				+ archiveLine(HISTORICAL_PRESENT, ArchiveType.TRANSLATION_EVENTS, 100, EVENT_MD5)
				+ archiveLine(HISTORICAL_PRESENT, ArchiveType.TRANSLATION_EVENTS, 101, EVENT_MD5)
				+ archiveLine(
						HISTORICAL_PRESENT,
						ArchiveType.TRANSLATION_MENTIONS,
						200,
						MENTION_MD5)
				+ pair(FRONTIER, 100, 200);

		assertFailure(
				body,
				Set.of(HISTORICAL_PRESENT, FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_METADATA_CONFLICT);
	}

	@Test
	@DisplayName("Отличие master metadata от captured latest отклоняется отдельным конфликтом")
	void rejectsMetadataThatDiffersFromCapturedLatest() {
		String body = gkgLine(OLDER) + pair(FRONTIER, 101, 200);

		assertFailure(
				body,
				Set.of(FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_METADATA_CONFLICT);
	}

	@Test
	@DisplayName("Некорректная строка получает типизированный master catalog код без раскрытия содержимого")
	void rejectsMalformedLineWithoutEchoingIt() {
		assertThatExceptionOfType(GdeltMasterCatalogValidationException.class)
				.isThrownBy(() -> parser.parse(
						snapshot("secret malformed line\n", 0),
						Set.of(FRONTIER),
						latest()))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IngestionErrorCode.MASTER_CATALOG_MALFORMED_LINE);
					assertThat(exception.context().lineNumber()).isEqualTo(1);
				})
				.withMessage(IngestionErrorCode.MASTER_CATALOG_MALFORMED_LINE.safeMessage())
				.withMessageNotContaining("secret");
	}

	@Test
	@DisplayName("URI master metadata вне разрешенного источника отклоняется отдельным кодом")
	void rejectsMetadataUriOutsideAllowlist() {
		String body = "100 " + EVENT_MD5
				+ " http://attacker.test/gdeltv2/20260830121500.translation.export.CSV.zip\n";

		assertFailure(
				body,
				Set.of(FRONTIER),
				latest(),
				IngestionErrorCode.MASTER_CATALOG_SOURCE_URI_REJECTED);
	}

	private GdeltTranslationMasterCatalog parseReady(
			String body,
			Set<Instant> targets,
			DiscoveredUpdate capturedLatest
	) {
		return parser.parse(snapshot(body, 10), targets, capturedLatest).requireCatalog();
	}

	private static void assertFailure(
			String body,
			Set<Instant> targets,
			DiscoveredUpdate capturedLatest,
			IngestionErrorCode expectedCode
	) {
		GdeltTranslationMasterCatalogParser catalogParser =
				new GdeltTranslationMasterCatalogParser(new GdeltManifestLineParser());
		assertThatExceptionOfType(GdeltMasterCatalogValidationException.class)
				.isThrownBy(() -> catalogParser.parse(
						snapshot(body, 10),
						targets,
						capturedLatest))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(expectedCode));
	}

	private static DiscoveredUpdate latest() {
		return new GdeltManifestParser(new GdeltManifestLineParser())
				.parse(pair(FRONTIER, 100, 200));
	}

	private static GdeltMasterTailSnapshot snapshot(String body, long rangeStart) {
		byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
		long rangeEnd = rangeStart + bytes.length - 1;
		return new GdeltMasterTailSnapshot(
				bytes,
				rangeStart,
				rangeEnd,
				rangeEnd + 1,
				"123",
				"\"catalog-etag\"");
	}

	private static String pair(Instant updateTime, long eventSize, long mentionSize) {
		return archiveLine(
				updateTime,
				ArchiveType.TRANSLATION_EVENTS,
				eventSize,
				EVENT_MD5)
				+ archiveLine(
						updateTime,
						ArchiveType.TRANSLATION_MENTIONS,
						mentionSize,
						MENTION_MD5);
	}

	private static String archiveLine(
			Instant updateTime,
			ArchiveType archiveType,
			long size,
			String md5
	) {
		String suffix = archiveType == ArchiveType.TRANSLATION_EVENTS
				? "translation.export.CSV.zip"
				: "translation.mentions.CSV.zip";
		return size + " " + md5 + " http://data.gdeltproject.org/gdeltv2/"
				+ SOURCE_TIME.format(updateTime) + "." + suffix + "\n";
	}

	private static String gkgLine(Instant updateTime) {
		return "300 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa "
				+ "http://data.gdeltproject.org/gdeltv2/"
				+ SOURCE_TIME.format(updateTime) + ".translation.gkg.csv.zip\n";
	}
}
