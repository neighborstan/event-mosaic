package com.neighbor.eventmosaic.ingestion.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.neighbor.eventmosaic.ingestion.api.ArchiveType;
import com.neighbor.eventmosaic.ingestion.api.DiscoveredArchive;
import com.neighbor.eventmosaic.ingestion.api.IngestionErrorCode;
import com.neighbor.eventmosaic.ingestion.api.IngestionEventCode;
import com.neighbor.eventmosaic.ingestion.error.RemoteSourceAccessException;
import com.neighbor.eventmosaic.ingestion.error.SourceDataViolationException;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Разбор GDELT manifest")
class GdeltManifestParserTest {

	private GdeltManifestParser parser;

	@BeforeEach
	void setUp() {
		parser = new GdeltManifestParser();
	}

	@Test
	@DisplayName("Полное обновление разбирается, а неподдерживаемый GKG регистрируется")
	void parsesCompleteUpdateAndReportsUnsupportedGkg() {
		String manifest = """
				100 0123456789ABCDEF0123456789ABCDEF http://data.gdeltproject.org/gdeltv2/20260720120000.translation.export.CSV.zip
				200 fedcba9876543210fedcba9876543210 http://data.gdeltproject.org/gdeltv2/20260720120000.translation.mentions.CSV.zip
				300 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa http://data.gdeltproject.org/gdeltv2/20260720120000.translation.gkg.csv.zip
				""";

		var update = parser.parse(manifest);

		assertThat(update.sourceUpdateTime()).isEqualTo(Instant.parse("2026-07-20T12:00:00Z"));
		assertThat(update.archives()).extracting(DiscoveredArchive::archiveType)
				.containsExactly(ArchiveType.TRANSLATION_EVENTS, ArchiveType.TRANSLATION_MENTIONS);
		assertThat(update.archives().getFirst().expectedMd5())
				.isEqualTo("0123456789abcdef0123456789abcdef");
		assertThat(update.diagnostics()).singleElement().satisfies(diagnostic -> {
			assertThat(diagnostic.code()).isEqualTo(IngestionEventCode.MANIFEST_UNSUPPORTED_ARCHIVE);
			assertThat(diagnostic.lineNumber()).isEqualTo(3);
		});
	}

	@Test
	@DisplayName("Некорректная непустая строка отклоняется без раскрытия содержимого")
	void rejectsMalformedNonblankLineWithoutEchoingIt() {
		assertThatExceptionOfType(SourceDataViolationException.class)
				.isThrownBy(() -> parser.parse("\nsecret malformed line"))
				.satisfies(exception -> {
					assertThat(exception.errorCode())
							.isEqualTo(IngestionErrorCode.MANIFEST_MALFORMED_LINE);
					assertThat(exception.context().lineNumber()).isEqualTo(2);
					assertThat(exception.context().httpStatus()).isNull();
				})
				.withMessage(IngestionErrorCode.MANIFEST_MALFORMED_LINE.safeMessage())
				.withMessageNotContaining("secret");
	}

	@Test
	@DisplayName("URI метаданных вне разрешенного источника отклоняется")
	void rejectsMetadataUriOutsideAllowlist() {
		String manifest = """
				100 0123456789abcdef0123456789abcdef http://attacker.test/gdeltv2/20260720120000.translation.export.CSV.zip
				200 fedcba9876543210fedcba9876543210 http://data.gdeltproject.org/gdeltv2/20260720120000.translation.mentions.CSV.zip
				""";

		assertFailure(
				manifest,
				IngestionErrorCode.MANIFEST_SOURCE_URI_REJECTED,
				SourceDataViolationException.class);
	}

	@Test
	@DisplayName("Для обновления обязательны оба поддерживаемых типа архивов")
	void requiresBothSupportedArchiveTypes() {
		String manifest = """
				100 0123456789abcdef0123456789abcdef http://data.gdeltproject.org/gdeltv2/20260720120000.translation.export.CSV.zip
				""";

		assertFailure(
				manifest,
				IngestionErrorCode.MANIFEST_REQUIRED_ARCHIVE_MISSING,
				RemoteSourceAccessException.class);
	}

	@Test
	@DisplayName("Повтор поддерживаемого типа архива отклоняется")
	void rejectsDuplicateSupportedType() {
		String manifest = """
				100 0123456789abcdef0123456789abcdef http://data.gdeltproject.org/gdeltv2/20260720120000.translation.export.CSV.zip
				101 11111111111111111111111111111111 http://data.gdeltproject.org/gdeltv2/20260720121500.translation.export.CSV.zip
				200 fedcba9876543210fedcba9876543210 http://data.gdeltproject.org/gdeltv2/20260720120000.translation.mentions.CSV.zip
				""";

		assertFailure(
				manifest,
				IngestionErrorCode.MANIFEST_DUPLICATE_ARCHIVE,
				SourceDataViolationException.class);
	}

	@Test
	@DisplayName("Разные временные метки поддерживаемых архивов отклоняются")
	void rejectsMismatchedSupportedTimestamps() {
		String manifest = """
				100 0123456789abcdef0123456789abcdef http://data.gdeltproject.org/gdeltv2/20260720120000.translation.export.CSV.zip
				200 fedcba9876543210fedcba9876543210 http://data.gdeltproject.org/gdeltv2/20260720121500.translation.mentions.CSV.zip
				""";

		assertFailure(
				manifest,
				IngestionErrorCode.MANIFEST_TIMESTAMP_MISMATCH,
				SourceDataViolationException.class);
	}

	private void assertFailure(
			String manifest,
			IngestionErrorCode code,
			Class<? extends ApplicationException> exceptionType
	) {
		assertThatExceptionOfType(exceptionType)
				.isThrownBy(() -> parser.parse(manifest))
				.satisfies(exception -> assertThat(exception.errorCode()).isEqualTo(code))
				.withMessage(code.safeMessage());
	}
}
