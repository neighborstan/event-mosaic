package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;

final class ProcessingTestFixtures {

	static final Instant SOURCE_UPDATE_TIME = Instant.parse("2026-07-21T14:45:00Z");
	static final String SOURCE_ARCHIVE_KEY = "20260721144500.translation.mentions.CSV.zip";

	private ProcessingTestFixtures() {
	}

	static ArchiveProcessingRequest mentionRequest() {
		return new ArchiveProcessingRequest(
				GdeltArchiveKind.TRANSLATION_MENTIONS,
				SOURCE_UPDATE_TIME,
				SOURCE_ARCHIVE_KEY,
				"1".repeat(64),
				Path.of("mentions.csv"));
	}

	static ArchiveProcessingRequest eventRequest() {
		return new ArchiveProcessingRequest(
				GdeltArchiveKind.TRANSLATION_EVENTS,
				SOURCE_UPDATE_TIME,
				"20260721144500.translation.export.CSV.zip",
				"2".repeat(64),
				Path.of("events.csv"));
	}

	static GdeltMention mention(
			long globalEventId,
			Instant eventTimeDate,
			Instant mentionTimeDate,
			Integer mentionType,
			String mentionIdentifier
	) {
		return new GdeltMention(
				globalEventId,
				eventTimeDate,
				mentionTimeDate,
				mentionType,
				"source-name",
				mentionIdentifier,
				7,
				11,
				12,
				13,
				1,
				85,
				1_024,
				-1.25,
				"translation-info",
				"extras");
	}

	static GdeltMention mentionWithNullableNumericsAndEmptyStrings(
			long globalEventId,
			Instant eventTimeDate,
			Instant mentionTimeDate,
			Integer mentionType,
			String mentionIdentifier
	) {
		return new GdeltMention(
				globalEventId,
				eventTimeDate,
				mentionTimeDate,
				mentionType,
				"",
				mentionIdentifier,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				"",
				"");
	}

	static GdeltEvent event(
			long globalEventId,
			LocalDate day,
			Instant dateAdded,
			Geo actor1,
			Geo actor2,
			Geo action
	) {
		return new GdeltEvent(
				globalEventId,
				day,
				YearMonth.of(2026, 7),
				2026,
				2026.55,
				"A1-CODE",
				"Actor 1",
				"A1-COUNTRY",
				"A1-GROUP",
				"A1-ETHNIC",
				"A1-RELIGION-1",
				"A1-RELIGION-2",
				"A1-TYPE-1",
				"A1-TYPE-2",
				"A1-TYPE-3",
				"A2-CODE",
				"Actor 2",
				"A2-COUNTRY",
				"A2-GROUP",
				"A2-ETHNIC",
				"A2-RELIGION-1",
				"A2-RELIGION-2",
				"A2-TYPE-1",
				"A2-TYPE-2",
				"A2-TYPE-3",
				1,
				"042",
				"042",
				"04",
				1,
				-2.5,
				3,
				2,
				2,
				-1.75,
				actor1.type(),
				actor1.name(),
				actor1.countryCode(),
				actor1.admin1Code(),
				actor1.admin2Code(),
				actor1.latitude(),
				actor1.longitude(),
				actor1.featureId(),
				actor2.type(),
				actor2.name(),
				actor2.countryCode(),
				actor2.admin1Code(),
				actor2.admin2Code(),
				actor2.latitude(),
				actor2.longitude(),
				actor2.featureId(),
				action.type(),
				action.name(),
				action.countryCode(),
				action.admin1Code(),
				action.admin2Code(),
				action.latitude(),
				action.longitude(),
				action.featureId(),
				dateAdded,
				"https://source.example/event");
	}

	static GdeltEvent eventWithNullableNumericsAndEmptyStrings(
			long globalEventId,
			LocalDate day,
			Instant dateAdded
	) {
		return new GdeltEvent(
				globalEventId,
				day,
				null,
				null,
				null,
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				"",
				null,
				"",
				"",
				"",
				null,
				null,
				null,
				null,
				null,
				null,
				null,
				"",
				"",
				"",
				"",
				null,
				null,
				"",
				null,
				"",
				"",
				"",
				"",
				null,
				null,
				"",
				null,
				"",
				"",
				"",
				"",
				null,
				null,
				"",
				dateAdded,
				"");
	}

	static Geo geo(String name, double latitude, double longitude) {
		return geo(1, name, latitude, longitude);
	}

	static Geo geo(Integer type, String name, double latitude, double longitude) {
		return new Geo(type, name, "US", "USCA", "", latitude, longitude, "feature");
	}

	static Geo partialGeo(String name, Double latitude, Double longitude) {
		return new Geo(1, name, "US", "USCA", "", latitude, longitude, "feature");
	}

	static Geo absentGeo() {
		return new Geo(null, "", "", "", "", null, null, "");
	}

	record Geo(
			Integer type,
			String name,
			String countryCode,
			String admin1Code,
			String admin2Code,
			Double latitude,
			Double longitude,
			String featureId
	) {
	}
}
