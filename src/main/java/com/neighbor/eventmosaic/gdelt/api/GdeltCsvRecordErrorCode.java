package com.neighbor.eventmosaic.gdelt.api;

/**
 * Ограниченный каталог причин локального отклонения GDELT CSV record.
 */
public enum GdeltCsvRecordErrorCode {

	/** Физическое число полей не соответствует Event или Mention schema. */
	FIELD_COUNT_MISMATCH,

	/** Обязательное provider value отсутствует или является blank. */
	REQUIRED_VALUE_MISSING,

	/** Непустое typed value имеет недопустимый формат или диапазон Java-типа. */
	INVALID_VALUE
}
