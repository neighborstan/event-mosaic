package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;

/**
 * Преобразует 61 provider column Event в одноименную immutable raw model.
 */
final class GdeltEventCsvMapper implements GdeltCsvRecordMapper<GdeltEvent> {

	static final int FIELD_COUNT = 61;

	@Override
	public GdeltCsvMappingResult<GdeltEvent> map(String[] fields) {
		try {
			return GdeltCsvMappingResult.accepted(new GdeltEvent(
					GdeltCsvValues.requiredLong(fields[0]),
					GdeltCsvValues.nullableDay(fields[1]),
					GdeltCsvValues.nullableMonth(fields[2]),
					GdeltCsvValues.nullableInteger(fields[3]),
					GdeltCsvValues.nullableDecimal(fields[4]),
					GdeltCsvValues.rawString(fields[5]),
					GdeltCsvValues.rawString(fields[6]),
					GdeltCsvValues.rawString(fields[7]),
					GdeltCsvValues.rawString(fields[8]),
					GdeltCsvValues.rawString(fields[9]),
					GdeltCsvValues.rawString(fields[10]),
					GdeltCsvValues.rawString(fields[11]),
					GdeltCsvValues.rawString(fields[12]),
					GdeltCsvValues.rawString(fields[13]),
					GdeltCsvValues.rawString(fields[14]),
					GdeltCsvValues.rawString(fields[15]),
					GdeltCsvValues.rawString(fields[16]),
					GdeltCsvValues.rawString(fields[17]),
					GdeltCsvValues.rawString(fields[18]),
					GdeltCsvValues.rawString(fields[19]),
					GdeltCsvValues.rawString(fields[20]),
					GdeltCsvValues.rawString(fields[21]),
					GdeltCsvValues.rawString(fields[22]),
					GdeltCsvValues.rawString(fields[23]),
					GdeltCsvValues.rawString(fields[24]),
					GdeltCsvValues.nullableInteger(fields[25]),
					GdeltCsvValues.rawString(fields[26]),
					GdeltCsvValues.rawString(fields[27]),
					GdeltCsvValues.rawString(fields[28]),
					GdeltCsvValues.nullableInteger(fields[29]),
					GdeltCsvValues.nullableDecimal(fields[30]),
					GdeltCsvValues.nullableInteger(fields[31]),
					GdeltCsvValues.nullableInteger(fields[32]),
					GdeltCsvValues.nullableInteger(fields[33]),
					GdeltCsvValues.nullableDecimal(fields[34]),
					GdeltCsvValues.nullableInteger(fields[35]),
					GdeltCsvValues.rawString(fields[36]),
					GdeltCsvValues.rawString(fields[37]),
					GdeltCsvValues.rawString(fields[38]),
					GdeltCsvValues.rawString(fields[39]),
					GdeltCsvValues.nullableDecimal(fields[40]),
					GdeltCsvValues.nullableDecimal(fields[41]),
					GdeltCsvValues.rawString(fields[42]),
					GdeltCsvValues.nullableInteger(fields[43]),
					GdeltCsvValues.rawString(fields[44]),
					GdeltCsvValues.rawString(fields[45]),
					GdeltCsvValues.rawString(fields[46]),
					GdeltCsvValues.rawString(fields[47]),
					GdeltCsvValues.nullableDecimal(fields[48]),
					GdeltCsvValues.nullableDecimal(fields[49]),
					GdeltCsvValues.rawString(fields[50]),
					GdeltCsvValues.nullableInteger(fields[51]),
					GdeltCsvValues.rawString(fields[52]),
					GdeltCsvValues.rawString(fields[53]),
					GdeltCsvValues.rawString(fields[54]),
					GdeltCsvValues.rawString(fields[55]),
					GdeltCsvValues.nullableDecimal(fields[56]),
					GdeltCsvValues.nullableDecimal(fields[57]),
					GdeltCsvValues.rawString(fields[58]),
					GdeltCsvValues.nullableTimestamp(fields[59]),
					GdeltCsvValues.rawString(fields[60])
			));
		} catch (GdeltCsvValueException exception) {
			return GdeltCsvMappingResult.rejected(exception.reason());
		}
	}
}
