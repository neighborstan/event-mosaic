package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltMention;

/**
 * Преобразует 16 provider columns Mention, включая физический `Extras`.
 */
final class GdeltMentionCsvMapper implements GdeltCsvRecordMapper<GdeltMention> {

	static final int FIELD_COUNT = 16;

	@Override
	public GdeltCsvMappingResult<GdeltMention> map(String[] fields) {
		try {
			return GdeltCsvMappingResult.accepted(new GdeltMention(
					GdeltCsvValues.requiredLong(fields[0]),
					GdeltCsvValues.nullableTimestamp(fields[1]),
					GdeltCsvValues.nullableTimestamp(fields[2]),
					GdeltCsvValues.nullableInteger(fields[3]),
					GdeltCsvValues.rawString(fields[4]),
					GdeltCsvValues.requiredString(fields[5]),
					GdeltCsvValues.nullableInteger(fields[6]),
					GdeltCsvValues.nullableInteger(fields[7]),
					GdeltCsvValues.nullableInteger(fields[8]),
					GdeltCsvValues.nullableInteger(fields[9]),
					GdeltCsvValues.nullableInteger(fields[10]),
					GdeltCsvValues.nullableInteger(fields[11]),
					GdeltCsvValues.nullableInteger(fields[12]),
					GdeltCsvValues.nullableDecimal(fields[13]),
					GdeltCsvValues.rawString(fields[14]),
					GdeltCsvValues.rawString(fields[15])
			));
		} catch (GdeltCsvValueException exception) {
			return GdeltCsvMappingResult.rejected(exception.reason());
		}
	}
}
