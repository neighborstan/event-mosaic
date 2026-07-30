package com.neighbor.eventmosaic.gdelt.csv;

/**
 * Одна физическая TSV-строка после сохранения всех пустых полей.
 */
final class GdeltTsvRecord {

	private final long lineNumber;
	private final String[] fields;

	GdeltTsvRecord(long lineNumber, String[] fields) {
		this.lineNumber = lineNumber;
		this.fields = fields;
	}

	long lineNumber() {
		return lineNumber;
	}

	String[] fields() {
		return fields;
	}
}
