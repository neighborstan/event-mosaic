package com.neighbor.eventmosaic.gdelt.csv;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvAccessException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvSchemaException;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("Потоковое чтение GDELT CSV")
class GdeltCsvReaderTest {

	@TempDir
	Path tempDirectory;

	private SimpleMeterRegistry meterRegistry;
	private DefaultGdeltEventCsvReader eventReader;
	private DefaultGdeltMentionCsvReader mentionReader;

	@BeforeEach
	void setUp() {
		meterRegistry = new SimpleMeterRegistry();
		GdeltCsvMetrics metrics = new GdeltCsvMetrics(meterRegistry);
		GdeltCsvProperties properties = new GdeltCsvProperties(1_048_576);
		eventReader = new DefaultGdeltEventCsvReader(properties, metrics);
		mentionReader = new DefaultGdeltMentionCsvReader(properties, metrics);
	}

	@AfterEach
	void closeRegistry() {
		meterRegistry.close();
	}

	@Test
	@DisplayName("Маппит все 61 поле Event в точном provider order")
	void mapsAllEventFieldsInProviderOrder() throws URISyntaxException {
		List<GdeltCsvRecord<GdeltEvent>> records = new ArrayList<>();

		GdeltCsvReadSummary summary = eventReader.read(resource("event-valid.csv"), records::add);

		assertThat(summary).isEqualTo(new GdeltCsvReadSummary(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				2,
				2,
				0,
				Map.of(),
				Map.of()));
		assertThat(records).hasSize(2);
		GdeltEvent event = records.getFirst().value();
		assertThat(records.getFirst().lineNumber()).isEqualTo(1);
		assertThat(List.of(
				event.globalEventId(),
				event.day(),
				event.monthYear(),
				event.year(),
				event.fractionDate(),
				event.actor1Code(),
				event.actor1Name(),
				event.actor1CountryCode(),
				event.actor1KnownGroupCode(),
				event.actor1EthnicCode(),
				event.actor1Religion1Code(),
				event.actor1Religion2Code(),
				event.actor1Type1Code(),
				event.actor1Type2Code(),
				event.actor1Type3Code(),
				event.actor2Code(),
				event.actor2Name(),
				event.actor2CountryCode(),
				event.actor2KnownGroupCode(),
				event.actor2EthnicCode(),
				event.actor2Religion1Code(),
				event.actor2Religion2Code(),
				event.actor2Type1Code(),
				event.actor2Type2Code(),
				event.actor2Type3Code(),
				event.isRootEvent(),
				event.eventCode(),
				event.eventBaseCode(),
				event.eventRootCode(),
				event.quadClass(),
				event.goldsteinScale(),
				event.numMentions(),
				event.numSources(),
				event.numArticles(),
				event.avgTone(),
				event.actor1GeoType(),
				event.actor1GeoFullName(),
				event.actor1GeoCountryCode(),
				event.actor1GeoAdm1Code(),
				event.actor1GeoAdm2Code(),
				event.actor1GeoLat(),
				event.actor1GeoLong(),
				event.actor1GeoFeatureId(),
				event.actor2GeoType(),
				event.actor2GeoFullName(),
				event.actor2GeoCountryCode(),
				event.actor2GeoAdm1Code(),
				event.actor2GeoAdm2Code(),
				event.actor2GeoLat(),
				event.actor2GeoLong(),
				event.actor2GeoFeatureId(),
				event.actionGeoType(),
				event.actionGeoFullName(),
				event.actionGeoCountryCode(),
				event.actionGeoAdm1Code(),
				event.actionGeoAdm2Code(),
				event.actionGeoLat(),
				event.actionGeoLong(),
				event.actionGeoFeatureId(),
				event.dateAdded(),
				event.sourceUrl()
		)).containsExactly(
				1_314_602_221L,
				LocalDate.of(2025, 7, 21),
				YearMonth.of(2025, 7),
				2025,
				Double.valueOf("2025.5507"),
				"AFR",
				"AFRICA",
				"AFR",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"UAF",
				"MILITANT",
				"",
				"",
				"",
				"",
				"",
				"UAF",
				"",
				"",
				0,
				"062",
				"062",
				"06",
				2,
				Double.valueOf("7.4"),
				10,
				1,
				10,
				Double.valueOf("-1.97869101978691"),
				4,
				"Havana, Ciudad de La Habana, Cuba",
				"CU",
				"CU02",
				"14613",
				Double.valueOf("23.1319"),
				Double.valueOf("-82.3642"),
				"-1628751",
				4,
				"Havana, Ciudad de La Habana, Cuba",
				"CU",
				"CU02",
				"14613",
				Double.valueOf("23.1319"),
				Double.valueOf("-82.3642"),
				"-1628751",
				4,
				"Havana, Ciudad de La Habana, Cuba",
				"CU",
				"CU02",
				"14613",
				Double.valueOf("23.1319"),
				Double.valueOf("-82.3642"),
				"-1628751",
				Instant.parse("2026-07-21T14:45:00Z"),
				"https://eldiariony.com/2026/07/21/reporte-del-departamento-de-estado-de-ee-uu-sobre-comunismo-en-cuba-menciona-a-independentistas-de-p-r-como-filiberto-ojeda/"
		);
	}

	@Test
	@DisplayName("Маппит все 16 полей Mention и сохраняет пустой или непустой Extras")
	void mapsAllMentionFieldsAndPreservesExtras() throws URISyntaxException {
		List<GdeltCsvRecord<GdeltMention>> records = new ArrayList<>();

		GdeltCsvReadSummary summary = mentionReader.read(resource("mention-valid.csv"), records::add);

		assertThat(summary.physicalRecords()).isEqualTo(2);
		assertThat(summary.validRecords()).isEqualTo(2);
		assertThat(records).hasSize(2);
		GdeltMention first = records.getFirst().value();
		assertThat(List.of(
				first.globalEventId(),
				first.eventTimeDate(),
				first.mentionTimeDate(),
				first.mentionType(),
				first.mentionSourceName(),
				first.mentionIdentifier(),
				first.sentenceId(),
				first.actor1CharOffset(),
				first.actor2CharOffset(),
				first.actionCharOffset(),
				first.inRawText(),
				first.confidence(),
				first.mentionDocLen(),
				first.mentionDocTone(),
				first.mentionDocTranslationInfo(),
				first.extras()
		)).containsExactly(
				1_314_602_221L,
				Instant.parse("2026-07-21T14:45:00Z"),
				Instant.parse("2026-07-21T14:45:00Z"),
				1,
				"eldiariony.com",
				"https://eldiariony.com/2026/07/21/reporte-del-departamento-de-estado-de-ee-uu-sobre-comunismo-en-cuba-menciona-a-independentistas-de-p-r-como-filiberto-ojeda/",
				7,
				2533,
				2503,
				2464,
				1,
				100,
				8597,
				Double.valueOf("-1.97869101978691"),
				"srclc:spa;eng:GT-SPA 1.0",
				"");
		assertThat(records.get(1).value().actor2CharOffset()).isEqualTo(-1);
		assertThat(records.get(1).value().extras()).isEqualTo("future:opaque value");
	}

	@Test
	@DisplayName("Сохраняет различимые значения во всех 61 позициях Event")
	void mapsDistinctValuesAcrossEveryEventPosition() throws IOException {
		String[] fields = {
				"700000001",
				"20240131",
				"202402",
				"2023",
				"2024.0847",
				"A1-CODE",
				"A1-NAME",
				"A1-COUNTRY",
				"A1-GROUP",
				"A1-ETHNIC",
				"A1-REL1",
				"A1-REL2",
				"A1-TYPE1",
				"A1-TYPE2",
				"A1-TYPE3",
				"A2-CODE",
				"A2-NAME",
				"A2-COUNTRY",
				"A2-GROUP",
				"A2-ETHNIC",
				"A2-REL1",
				"A2-REL2",
				"A2-TYPE1",
				"A2-TYPE2",
				"A2-TYPE3",
				"101",
				"EVENT-CODE",
				"EVENT-BASE",
				"EVENT-ROOT",
				"102",
				"-3.25",
				"103",
				"104",
				"105",
				"-6.75",
				"106",
				"A1-GEO-NAME",
				"A1-GEO-COUNTRY",
				"A1-GEO-ADM1",
				"A1-GEO-ADM2",
				"10.125",
				"11.25",
				"A1-FEATURE",
				"107",
				"A2-GEO-NAME",
				"A2-GEO-COUNTRY",
				"A2-GEO-ADM1",
				"A2-GEO-ADM2",
				"20.125",
				"21.25",
				"A2-FEATURE",
				"108",
				"ACTION-GEO-NAME",
				"ACTION-GEO-COUNTRY",
				"ACTION-GEO-ADM1",
				"ACTION-GEO-ADM2",
				"30.125",
				"31.25",
				"ACTION-FEATURE",
				"20240304050607",
				"https://source.example/event-position-60"
		};
		List<GdeltEvent> events = new ArrayList<>();

		eventReader.read(
				write("event-distinct-fields.csv", line(fields)),
				csvRecord -> events.add(csvRecord.value()));

		assertThat(events).containsExactly(new GdeltEvent(
				700_000_001L,
				LocalDate.of(2024, 1, 31),
				YearMonth.of(2024, 2),
				2023,
				Double.valueOf("2024.0847"),
				"A1-CODE",
				"A1-NAME",
				"A1-COUNTRY",
				"A1-GROUP",
				"A1-ETHNIC",
				"A1-REL1",
				"A1-REL2",
				"A1-TYPE1",
				"A1-TYPE2",
				"A1-TYPE3",
				"A2-CODE",
				"A2-NAME",
				"A2-COUNTRY",
				"A2-GROUP",
				"A2-ETHNIC",
				"A2-REL1",
				"A2-REL2",
				"A2-TYPE1",
				"A2-TYPE2",
				"A2-TYPE3",
				101,
				"EVENT-CODE",
				"EVENT-BASE",
				"EVENT-ROOT",
				102,
				Double.valueOf("-3.25"),
				103,
				104,
				105,
				Double.valueOf("-6.75"),
				106,
				"A1-GEO-NAME",
				"A1-GEO-COUNTRY",
				"A1-GEO-ADM1",
				"A1-GEO-ADM2",
				Double.valueOf("10.125"),
				Double.valueOf("11.25"),
				"A1-FEATURE",
				107,
				"A2-GEO-NAME",
				"A2-GEO-COUNTRY",
				"A2-GEO-ADM1",
				"A2-GEO-ADM2",
				Double.valueOf("20.125"),
				Double.valueOf("21.25"),
				"A2-FEATURE",
				108,
				"ACTION-GEO-NAME",
				"ACTION-GEO-COUNTRY",
				"ACTION-GEO-ADM1",
				"ACTION-GEO-ADM2",
				Double.valueOf("30.125"),
				Double.valueOf("31.25"),
				"ACTION-FEATURE",
				Instant.parse("2024-03-04T05:06:07Z"),
				"https://source.example/event-position-60"
		));
	}

	@Test
	@DisplayName("Сохраняет различимые значения во всех 16 позициях Mention")
	void mapsDistinctValuesAcrossEveryMentionPosition() throws IOException {
		String[] fields = {
				"800000001",
				"20240102030405",
				"20240203040506",
				"201",
				"source-position-4",
				"identifier-position-5",
				"206",
				"-207",
				"208",
				"-209",
				"210",
				"211",
				"212",
				"-13.25",
				"translation-position-14",
				"extras-position-15"
		};
		List<GdeltMention> mentions = new ArrayList<>();

		mentionReader.read(
				write("mention-distinct-fields.csv", line(fields)),
				csvRecord -> mentions.add(csvRecord.value()));

		assertThat(mentions).containsExactly(new GdeltMention(
				800_000_001L,
				Instant.parse("2024-01-02T03:04:05Z"),
				Instant.parse("2024-02-03T04:05:06Z"),
				201,
				"source-position-4",
				"identifier-position-5",
				206,
				-207,
				208,
				-209,
				210,
				211,
				212,
				Double.valueOf("-13.25"),
				"translation-position-14",
				"extras-position-15"
		));
	}

	@Test
	@DisplayName("Оставляет optional typed fields null и verbatim строки без trim")
	void preservesNullableValuesAndVerbatimStrings() throws IOException {
		String[] fields = minimalMentionFields("41", "  citation value  ");
		fields[4] = " source ";
		fields[15] = "  opaque  ";
		Path path = write("nullable-mention.csv", line(fields));
		List<GdeltMention> mentions = new ArrayList<>();

		GdeltCsvReadSummary summary = mentionReader.read(
				path,
				csvRecord -> mentions.add(csvRecord.value()));

		assertThat(summary.validRecords()).isEqualTo(1);
		GdeltMention mention = mentions.getFirst();
		assertThat(mention.eventTimeDate()).isNull();
		assertThat(mention.mentionType()).isNull();
		assertThat(mention.mentionSourceName()).isEqualTo(" source ");
		assertThat(mention.mentionIdentifier()).isEqualTo("  citation value  ");
		assertThat(mention.extras()).isEqualTo("  opaque  ");
	}

	@Test
	@DisplayName("Продолжает после трех разных локальных отклонений")
	void continuesAfterLocalRejections() throws IOException {
		String validFirst = minimalEventLine("1");
		String wrongWidth = String.join("\t", Arrays.copyOf(minimalEventFields("2"), 60));
		String[] missingIdFields = minimalEventFields("");
		String[] invalidFields = minimalEventFields("3");
		invalidFields[3] = "not-a-year";
		String validLast = minimalEventLine("4");
		Path path = write(
				"mixed-events.csv",
				String.join("\n", validFirst, wrongWidth, line(missingIdFields), line(invalidFields), validLast));
		List<Long> ids = new ArrayList<>();

		GdeltCsvReadSummary summary = eventReader.read(
				path,
				csvRecord -> ids.add(csvRecord.value().globalEventId()));

		assertThat(ids).containsExactly(1L, 4L);
		assertThat(summary.physicalRecords()).isEqualTo(5);
		assertThat(summary.validRecords()).isEqualTo(2);
		assertThat(summary.invalidRecords()).isEqualTo(3);
		assertThat(summary.rejectionCounts()).containsExactlyInAnyOrderEntriesOf(Map.of(
				GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH, 1L,
				GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING, 1L,
				GdeltCsvRecordErrorCode.INVALID_VALUE, 1L));
		assertThat(summary.firstRejectedLineNumbers()).containsExactlyInAnyOrderEntriesOf(Map.of(
				GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH, 2L,
				GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING, 3L,
				GdeltCsvRecordErrorCode.INVALID_VALUE, 4L));
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
				.tags("kind", "translation_events", "outcome", "valid")
				.counter()
				.count()).isEqualTo(2);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
				.tags("kind", "translation_events", "outcome", "invalid")
				.counter()
				.count()).isEqualTo(3);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.rejections")
				.tags("kind", "translation_events", "reason", "field_count_mismatch")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.rejections")
				.tags("kind", "translation_events", "reason", "required_value_missing")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.rejections")
				.tags("kind", "translation_events", "reason", "invalid_value")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "completed")
				.counter()
				.count()).isEqualTo(1);
	}

	@Test
	@DisplayName("Отклоняет overflow, non-finite decimal и невозможный timestamp")
	void rejectsStrictTypedValues() throws IOException {
		String[] integerOverflow = minimalMentionFields("1", "doc-1");
		integerOverflow[6] = "999999999999999999999";
		String[] notFinite = minimalMentionFields("2", "doc-2");
		notFinite[13] = "NaN";
		String[] infinity = minimalMentionFields("3", "doc-3");
		infinity[13] = "Infinity";
		String[] badTimestamp = minimalMentionFields("4", "doc-4");
		badTimestamp[2] = "20261340129999";
		String[] shortTimestamp = minimalMentionFields("5", "doc-5");
		shortTimestamp[2] = "2024010203040";
		String[] longOverflow = minimalMentionFields(
				"999999999999999999999",
				"doc-6");
		Path path = write(
				"invalid-values.csv",
				String.join(
						"\n",
						line(integerOverflow),
						line(notFinite),
						line(infinity),
						line(badTimestamp),
						line(shortTimestamp),
						line(longOverflow)));

		GdeltCsvReadSummary summary = mentionReader.read(path, _ -> {
			throw new AssertionError("Invalid record must not reach consumer");
		});

		assertThat(summary.validRecords()).isZero();
		assertThat(summary.invalidRecords()).isEqualTo(6);
		assertThat(summary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.INVALID_VALUE, 6L);
	}

	@Test
	@DisplayName("Строго проверяет календарный день и месяц Event")
	void rejectsImpossibleEventDayAndMonth() throws IOException {
		String[] impossibleDay = minimalEventFields("1");
		impossibleDay[1] = "20250230";
		String[] impossibleMonth = minimalEventFields("2");
		impossibleMonth[2] = "202513";
		Path path = write(
				"invalid-event-dates.csv",
				String.join("\n", line(impossibleDay), line(impossibleMonth)));

		GdeltCsvReadSummary summary = eventReader.read(path, _ -> {
			throw new AssertionError("Invalid record must not reach consumer");
		});

		assertThat(summary.validRecords()).isZero();
		assertThat(summary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.INVALID_VALUE, 2L);
	}

	@Test
	@DisplayName("Отклоняет все отсутствующие обязательные идентификаторы")
	void rejectsMissingRequiredIdentifiers() throws IOException {
		String[] eventWithoutId = minimalEventFields("");
		String[] mentionWithoutEventId = minimalMentionFields("", "doc-1");
		String[] mentionWithBlankIdentifier = minimalMentionFields("2", " \t-safe ");
		mentionWithBlankIdentifier[5] = "   ";

		GdeltCsvReadSummary eventSummary = eventReader.read(
				write("event-without-id.csv", line(eventWithoutId)),
				_ -> {
					throw new AssertionError("Invalid record must not reach consumer");
				});
		GdeltCsvReadSummary mentionSummary = mentionReader.read(
				write(
						"mention-without-identifiers.csv",
						String.join("\n", line(mentionWithoutEventId), line(mentionWithBlankIdentifier))),
				_ -> {
					throw new AssertionError("Invalid record must not reach consumer");
				});

		assertThat(eventSummary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING, 1L);
		assertThat(mentionSummary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.REQUIRED_VALUE_MISSING, 2L);
	}

	@Test
	@DisplayName("Локально отклоняет Event 60/62 и Mention 15/17 полей")
	void rejectsEveryAdjacentWrongWidthLocally() throws IOException {
		String event60 = line(Arrays.copyOf(minimalEventFields("2"), 60));
		String[] event62Fields = Arrays.copyOf(minimalEventFields("3"), 62);
		event62Fields[61] = "unexpected";
		String mention15 = line(Arrays.copyOf(minimalMentionFields("2", "doc-2"), 15));
		String[] mention17Fields = Arrays.copyOf(minimalMentionFields("3", "doc-3"), 17);
		mention17Fields[16] = "unexpected";

		GdeltCsvReadSummary eventSummary = eventReader.read(
				write(
						"event-widths.csv",
						String.join(
								"\n",
								minimalEventLine("1"),
								event60,
								line(event62Fields),
								minimalEventLine("4"))),
				_ -> {
				});
		GdeltCsvReadSummary mentionSummary = mentionReader.read(
				write(
						"mention-widths.csv",
						String.join(
								"\n",
								line(minimalMentionFields("1", "doc-1")),
								mention15,
								line(mention17Fields),
								line(minimalMentionFields("4", "doc-4")))),
				_ -> {
				});

		assertThat(eventSummary.validRecords()).isEqualTo(2);
		assertThat(eventSummary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH, 2L);
		assertThat(mentionSummary.validRecords()).isEqualTo(2);
		assertThat(mentionSummary.rejectionCounts())
				.containsEntry(GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH, 2L);
	}

	@Test
	@DisplayName("Сохраняет CRLF, отсутствие final newline, source order и duplicate records")
	void preservesPhysicalLineSemanticsAndDuplicates() throws IOException {
		String duplicate = line(minimalMentionFields("10", "same-document"));
		String third = line(minimalMentionFields("11", "third-document"));
		Path path = write("line-semantics.csv", duplicate + "\r\n" + duplicate + "\r\n" + third);
		List<GdeltCsvRecord<GdeltMention>> records = new ArrayList<>();

		GdeltCsvReadSummary summary = mentionReader.read(path, records::add);

		assertThat(summary.validRecords()).isEqualTo(3);
		assertThat(records).extracting(GdeltCsvRecord::lineNumber).containsExactly(1L, 2L, 3L);
		assertThat(records).extracting(csvRecord -> csvRecord.value().globalEventId())
				.containsExactly(10L, 10L, 11L);
	}

	@Test
	@DisplayName("Считает blank line локальной wrong-width record")
	void countsBlankLineAsPhysicalInvalidRecord() throws IOException {
		Path path = write(
				"blank-line.csv",
				minimalEventLine("1") + "\n\n" + minimalEventLine("2") + "\n");

		GdeltCsvReadSummary summary = eventReader.read(path, _ -> {
		});

		assertThat(summary.physicalRecords()).isEqualTo(3);
		assertThat(summary.validRecords()).isEqualTo(2);
		assertThat(summary.invalidRecords()).isEqualTo(1);
		assertThat(summary.firstRejectedLineNumbers())
				.containsEntry(GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH, 2L);
	}

	@Test
	@DisplayName("Завершает empty и all-wrong-shape файлы schema failure")
	void rejectsFilesWithoutShapeCompatibleRecords() throws IOException {
		Path empty = write("empty.csv", "");
		Path fifteenColumns = write(
				"mention-15.csv",
				String.join("\t", Arrays.copyOf(minimalMentionFields("1", "doc"), 15)));

		assertSchemaCode(empty, eventReader, GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH);
		assertThatExceptionOfType(GdeltCsvSchemaException.class)
				.isThrownBy(() -> mentionReader.read(fifteenColumns, _ -> {
				}))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH));
	}

	@Test
	@DisplayName("Отклоняет malformed UTF-8 и BOM без replacement character")
	void rejectsMalformedUtf8AndBom() throws IOException {
		Path malformed = tempDirectory.resolve("malformed.csv");
		Files.write(malformed, new byte[]{'1', '\t', (byte) 0xC3, 0x28});
		Path bom = tempDirectory.resolve("bom.csv");
		byte[] content = minimalEventLine("1").getBytes(StandardCharsets.UTF_8);
		byte[] withBom = new byte[content.length + 3];
		withBom[0] = (byte) 0xEF;
		withBom[1] = (byte) 0xBB;
		withBom[2] = (byte) 0xBF;
		System.arraycopy(content, 0, withBom, 3, content.length);
		Files.write(bom, withBom);

		assertSchemaCode(malformed, eventReader, GdeltCsvErrorCode.CSV_ENCODING_INVALID);
		assertSchemaCode(bom, eventReader, GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH);
	}

	@Test
	@DisplayName("Останавливает oversized unterminated record на configured limit")
	void rejectsOversizedRecordWithoutReadingWholeLine() throws IOException {
		GdeltCsvMetrics metrics = new GdeltCsvMetrics(meterRegistry);
		DefaultGdeltEventCsvReader boundedReader = new DefaultGdeltEventCsvReader(
				new GdeltCsvProperties(10),
				metrics);
		Path path = write("oversized.csv", "12345678901");

		assertSchemaCode(path, boundedReader, GdeltCsvErrorCode.CSV_RECORD_LIMIT_EXCEEDED);
	}

	@Test
	@DisplayName("Прерывает oversized record до второго source read")
	void enforcesRecordBoundBeforeReadingUnterminatedTail() {
		AtomicInteger sourceReads = new AtomicInteger();
		Reader source = new Reader() {
			@Override
			public int read(char[] buffer, int offset, int length) {
				if (sourceReads.incrementAndGet() > 1) {
					throw new AssertionError("Reader must fail before reading the unbounded tail");
				}
				Arrays.fill(buffer, offset, offset + length, 'x');
				return length;
			}

			@Override
			public void close() {
				// Test double не владеет внешним ресурсом.
			}
		};
		GdeltTsvRecordReader tsvReader = new GdeltTsvRecordReader(source, 10);

		assertThatExceptionOfType(GdeltCsvSchemaException.class)
				.isThrownBy(tsvReader::readRecord)
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_RECORD_LIMIT_EXCEEDED));
		assertThat(sourceReads).hasValue(1);
	}

	@Test
	@DisplayName("Не читает следующий source chunk до возврата consumer")
	void doesNotReadAheadBeforeConsumerReturns() {
		AtomicInteger sourceReads = new AtomicInteger();
		String firstRecord = minimalEventLine("1") + "\n";
		Reader source = new Reader() {
			private final StringReader prefix = new StringReader(firstRecord);

			@Override
			public int read(char[] buffer, int offset, int length) throws IOException {
				sourceReads.incrementAndGet();
				int read = prefix.read(buffer, offset, length);
				if (read >= 0) {
					return read;
				}
				throw new AssertionError("Reader accessed the tail before consumer returned");
			}

			@Override
			public void close() {
				// Test double не владеет внешним ресурсом.
			}
		};
		IllegalStateException expected = new IllegalStateException("consumer stopped");
		GdeltRecordConsumer<GdeltEvent> failingConsumer = _ -> {
			throw expected;
		};

		assertThatThrownBy(() -> eventReader.readCsv(source, failingConsumer))
				.isSameAs(expected);

		assertThat(sourceReads).hasValue(1);
	}

	@Test
	@DisplayName("Публикует partial counts для schema, I/O и interruption failures")
	void publishesPartialMetricsForFileFailures() throws IOException {
		GdeltCsvMetrics metrics = new GdeltCsvMetrics(meterRegistry);
		DefaultGdeltEventCsvReader boundedReader = new DefaultGdeltEventCsvReader(
				new GdeltCsvProperties(61),
				metrics);
		Path partial = write(
				"partial-schema.csv",
				minimalEventLine("1") + "\n" + "x".repeat(62));

		assertSchemaCode(
				partial,
				boundedReader,
				GdeltCsvErrorCode.CSV_RECORD_LIMIT_EXCEEDED);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
				.tags("kind", "translation_events", "outcome", "valid")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "schema_failed")
				.counter()
				.count()).isEqualTo(1);

		Path missing = tempDirectory.resolve("missing.csv");
		assertThatExceptionOfType(GdeltCsvAccessException.class)
				.isThrownBy(() -> boundedReader.read(missing, _ -> {
				}))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE));
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "io_failed")
				.counter()
				.count()).isEqualTo(1);

		Thread.currentThread().interrupt();
		try {
			assertThatExceptionOfType(GdeltCsvInterruptedException.class)
					.isThrownBy(() -> boundedReader.read(partial, _ -> {
					}));
			assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
					.tags("kind", "translation_events", "outcome", "interrupted")
					.counter()
					.count()).isEqualTo(1);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Классифицирует типизированные исключения consumer как downstream failure")
	void classifiesTypedConsumerExceptionsAsConsumerFailures() throws IOException {
		Path path = write("typed-consumer-failures.csv", minimalEventLine("1"));
		GdeltCsvSchemaException schemaFailure =
				new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH);
		GdeltCsvInterruptedException interruptionFailure = new GdeltCsvInterruptedException();

		assertThatThrownBy(() -> eventReader.read(path, _ -> {
			throw schemaFailure;
		})).isSameAs(schemaFailure);
		assertThatThrownBy(() -> eventReader.read(path, _ -> {
			throw interruptionFailure;
		})).isSameAs(interruptionFailure);

		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "consumer_failed")
				.counter()
				.count()).isEqualTo(2);
		assertThat(meterRegistry.find("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "schema_failed")
				.counter()).isNull();
		assertThat(meterRegistry.find("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "interrupted")
				.counter()).isNull();
	}

	@Test
	@DisplayName("Сохраняет close failure как suppressed у исходного consumer exception")
	void preservesCloseFailureOnConsumerException() {
		StringReader content = new StringReader(minimalEventLine("1"));
		IOException closeFailure = new IOException("synthetic close failure");
		Reader source = new Reader() {
			@Override
			public int read(char[] buffer, int offset, int length) throws IOException {
				return content.read(buffer, offset, length);
			}

			@Override
			public void close() throws IOException {
				throw closeFailure;
			}
		};
		IllegalStateException consumerFailure = new IllegalStateException("consumer failed");

		assertThatThrownBy(() -> eventReader.readCsv(source, _ -> {
			throw consumerFailure;
		})).isSameAs(consumerFailure);

		assertThat(consumerFailure.getSuppressed()).containsExactly(closeFailure);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "consumer_failed")
				.counter()
				.count()).isEqualTo(1);
	}

	@Test
	@DisplayName("Сохраняет close failure у исключения source progress listener")
	void preservesCloseFailureOnProgressListenerException() {
		StringReader content = new StringReader("wrong-width\n");
		IOException closeFailure = new IOException("synthetic close failure");
		Reader source = new Reader() {
			@Override
			public int read(char[] buffer, int offset, int length) throws IOException {
				return content.read(buffer, offset, length);
			}

			@Override
			public void close() throws IOException {
				throw closeFailure;
			}
		};
		IllegalStateException progressFailure =
				new IllegalStateException("progress listener failed");

		assertThatThrownBy(() -> eventReader.readCsv(
				source,
				_ -> {
				},
				_ -> {
					throw progressFailure;
				})).isSameAs(progressFailure);

		assertThat(progressFailure.getSuppressed()).containsExactly(closeFailure);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "consumer_failed")
				.counter()
				.count()).isEqualTo(1);
	}

	@Test
	@DisplayName("Не классифицирует внутренний mapper defect как consumer failure")
	void doesNotClassifyMapperDefectAsConsumerFailure() {
		IllegalStateException mapperFailure = new IllegalStateException("mapper defect");
		GdeltCsvReaderSupport<GdeltEvent> failingReader = new GdeltCsvReaderSupport<>(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				61,
				new GdeltCsvProperties(1_048_576),
				_ -> {
					throw mapperFailure;
				},
				new GdeltCsvMetrics(meterRegistry)
		) {
		};
		Reader source = new StringReader(minimalEventLine("1"));

		assertThatThrownBy(() -> failingReader.readCsv(source, _ -> {
		})).isSameAs(mapperFailure);

		assertThat(meterRegistry.find("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "consumer_failed")
				.counter()).isNull();
	}

	@Test
	@DisplayName("Публикует ненулевой parsed prefix при I/O failure")
	void publishesNonzeroPartialMetricsForIoFailure() {
		StringReader prefix = new StringReader(minimalEventLine("1") + "\n");
		Reader source = new Reader() {
			@Override
			public int read(char[] buffer, int offset, int length) throws IOException {
				int read = prefix.read(buffer, offset, length);
				if (read >= 0) {
					return read;
				}
				throw new IOException("synthetic read failure");
			}

			@Override
			public void close() {
				// Test double не владеет внешним ресурсом.
			}
		};
		List<Long> delivered = new ArrayList<>();

		assertThatExceptionOfType(GdeltCsvAccessException.class)
				.isThrownBy(() -> eventReader.readCsv(
						source,
						csvRecord -> delivered.add(csvRecord.value().globalEventId())))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE));

		assertThat(delivered).containsExactly(1L);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
				.tags("kind", "translation_events", "outcome", "valid")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tags("kind", "translation_events", "outcome", "io_failed")
				.counter()
				.count()).isEqualTo(1);
	}

	@Test
	@DisplayName("Потоково сообщает каждое отклонение до invalid-only schema failure")
	void reportsInvalidOnlyProgressBeforeSchemaFailure() throws IOException {
		Path path = write(
				"invalid-only-progress.csv",
				String.join("\n", "wrong-width", "still-wrong-width"));
		List<Long> invalidProgress = new ArrayList<>();

		assertThatExceptionOfType(GdeltCsvSchemaException.class)
				.isThrownBy(() -> eventReader.read(
						path,
						_ -> {
						},
						invalidProgress::add))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH));

		assertThat(invalidProgress).containsExactly(1L, 2L);
	}

	@Test
	@DisplayName("Сохраняет source rejection progress перед последующим I/O failure")
	void reportsInvalidProgressBeforeIoFailure() {
		StringReader prefix = new StringReader("wrong-width\n");
		Reader source = new Reader() {
			@Override
			public int read(char[] buffer, int offset, int length) throws IOException {
				int read = prefix.read(buffer, offset, length);
				if (read >= 0) {
					return read;
				}
				throw new IOException("synthetic read failure");
			}

			@Override
			public void close() {
				// Test double не владеет внешним ресурсом.
			}
		};
		List<Long> invalidProgress = new ArrayList<>();

		assertThatExceptionOfType(GdeltCsvAccessException.class)
				.isThrownBy(() -> eventReader.readCsv(
						source,
						_ -> {
						},
						invalidProgress::add))
				.satisfies(exception -> assertThat(exception.errorCode())
						.isEqualTo(GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE));

		assertThat(invalidProgress).containsExactly(1L);
	}

	@Test
	@DisplayName("Сохраняет downstream exception и не вызывает consumer после отказа")
	void propagatesSameConsumerFailureAndStopsDelivery() throws IOException {
		Path path = write(
				"consumer-failure.csv",
				String.join("\n", minimalEventLine("1"), minimalEventLine("2"), minimalEventLine("3")));
		IllegalStateException expected = new IllegalStateException("downstream failed");
		AtomicInteger calls = new AtomicInteger();

		assertThatThrownBy(() -> eventReader.read(path, _ -> {
			if (calls.incrementAndGet() == 2) {
				throw expected;
			}
		})).isSameAs(expected);

		assertThat(calls).hasValue(2);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
				.tag("kind", "translation_events")
				.tag("outcome", "consumer_failed")
				.counter()
				.count()).isEqualTo(1);
		assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
				.tags("kind", "translation_events", "outcome", "valid")
				.counter()
				.count()).isEqualTo(2);
	}

	@Test
	@DisplayName("Повтор после consumer failure снова доставляет prefix с начала")
	void redeliversPrefixOnRetry() throws IOException {
		Path path = write(
				"retry.csv",
				String.join("\n", minimalEventLine("1"), minimalEventLine("2"), minimalEventLine("3")));
		List<Long> firstAttempt = new ArrayList<>();

		assertThatThrownBy(() -> eventReader.read(path, csvRecord -> {
			firstAttempt.add(csvRecord.value().globalEventId());
			if (firstAttempt.size() == 2) {
				throw new IllegalStateException("stop");
			}
		})).isInstanceOf(IllegalStateException.class);
		List<Long> retry = new ArrayList<>();
		eventReader.read(path, csvRecord -> retry.add(csvRecord.value().globalEventId()));

		assertThat(firstAttempt).containsExactly(1L, 2L);
		assertThat(retry).containsExactly(1L, 2L, 3L);
	}

	@Test
	@DisplayName("Cooperative checkpoint сохраняет interrupt flag")
	void stopsOnCooperativeInterruption() throws IOException {
		Path path = write("interrupted.csv", minimalEventLine("1"));
		Thread.currentThread().interrupt();
		try {
			assertThatExceptionOfType(GdeltCsvInterruptedException.class)
					.isThrownBy(() -> eventReader.read(path, _ -> {
					}))
					.satisfies(exception -> assertThat(exception.errorCode())
							.isEqualTo(GdeltCsvErrorCode.CSV_OPERATION_INTERRUPTED));
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Останавливается перед buffered record после interruption в consumer")
	void stopsBeforeBufferedRecordWhenConsumerInterrupts() throws IOException {
		Path path = write(
				"consumer-interrupted.csv",
				String.join("\n", minimalEventLine("1"), minimalEventLine("2")));
		List<Long> delivered = new ArrayList<>();

		try {
			assertThatExceptionOfType(GdeltCsvInterruptedException.class)
					.isThrownBy(() -> eventReader.read(path, csvRecord -> {
						delivered.add(csvRecord.value().globalEventId());
						Thread.currentThread().interrupt();
					}));

			assertThat(delivered).containsExactly(1L);
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
			assertThat(meterRegistry.get("event_mosaic.gdelt.csv.files")
					.tags("kind", "translation_events", "outcome", "interrupted")
					.counter()
					.count()).isEqualTo(1);
			assertThat(meterRegistry.get("event_mosaic.gdelt.csv.records")
					.tags("kind", "translation_events", "outcome", "valid")
					.counter()
					.count()).isEqualTo(1);
		} finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Большой generated input обрабатывается без retained records и reentrant callback")
	void streamsLargeGeneratedInputOneRecordAtATime() throws IOException {
		int recordCount = 20_000;
		Path path = tempDirectory.resolve("large.csv");
		try (BufferedWriter writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
			for (int index = 1; index <= recordCount; index++) {
				if (index > 1) {
					writer.newLine();
				}
				writer.write(minimalEventLine(Integer.toString(index)));
			}
		}
		AtomicInteger consumed = new AtomicInteger();
		AtomicBoolean inCallback = new AtomicBoolean();

		GdeltCsvReadSummary summary = eventReader.read(path, _ -> {
			assertThat(inCallback.compareAndSet(false, true)).isTrue();
			consumed.incrementAndGet();
			inCallback.set(false);
		});

		assertThat(summary.validRecords()).isEqualTo(recordCount);
		assertThat(consumed).hasValue(recordCount);
		assertThat(inCallback).isFalse();
	}

	private Path resource(String name) throws URISyntaxException {
		return Path.of(Objects.requireNonNull(
				getClass().getResource("/gdelt/csv/" + name),
				"Missing test resource " + name).toURI());
	}

	private Path write(String name, String content) throws IOException {
		return Files.writeString(
				tempDirectory.resolve(name),
				content,
				StandardCharsets.UTF_8);
	}

	private static String minimalEventLine(String globalEventId) {
		return line(minimalEventFields(globalEventId));
	}

	private static String[] minimalEventFields(String globalEventId) {
		String[] fields = emptyFields(61);
		fields[0] = globalEventId;
		return fields;
	}

	private static String[] minimalMentionFields(String globalEventId, String mentionIdentifier) {
		String[] fields = emptyFields(16);
		fields[0] = globalEventId;
		fields[5] = mentionIdentifier;
		return fields;
	}

	private static String[] emptyFields(int size) {
		String[] fields = new String[size];
		Arrays.fill(fields, "");
		return fields;
	}

	private static String line(String[] fields) {
		return String.join("\t", fields);
	}

	private static void assertSchemaCode(
			Path path,
			DefaultGdeltEventCsvReader reader,
			GdeltCsvErrorCode expectedCode
	) {
		assertThatExceptionOfType(GdeltCsvSchemaException.class)
				.isThrownBy(() -> reader.read(path, _ -> {
				}))
				.satisfies(exception -> assertThat(exception.errorCode()).isEqualTo(expectedCode));
	}
}
