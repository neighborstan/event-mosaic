package com.neighbor.eventmosaic.gdelt.csv;

/**
 * Преобразует одну строку ожидаемой ширины в raw provider model.
 */
@FunctionalInterface
interface GdeltCsvRecordMapper<T> {

	GdeltCsvMappingResult<T> map(String[] fields);
}
