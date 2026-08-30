package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltCsvErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvSchemaException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.Objects;

/**
 * Читает literal TAB-separated physical records с ограничением памяти одной
 * строки и без RFC 4180 quoting/multiline semantics.
 */
final class GdeltTsvRecordReader {

	private static final int BUFFER_SIZE = 8192;

	private final Reader source;
	private final int maxRecordChars;
	private final OperationBudget budget;
	private final char[] buffer = new char[BUFFER_SIZE];

	private int bufferPosition;
	private int bufferLimit;
	private long nextLineNumber = 1;
	private boolean skipLineFeed;
	private boolean firstCharacter = true;
	private boolean exhausted;

	GdeltTsvRecordReader(Reader source, int maxRecordChars) {
		this(source, maxRecordChars, OperationBudget.start(Duration.ofDays(1)));
	}

	GdeltTsvRecordReader(
			Reader source,
			int maxRecordChars,
			OperationBudget budget
	) {
		this.source = Objects.requireNonNull(source, "source must not be null");
		if (maxRecordChars <= 0) {
			throw new IllegalArgumentException("maxRecordChars must be positive");
		}
		this.maxRecordChars = maxRecordChars;
		this.budget = Objects.requireNonNull(budget, "budget must not be null");
	}

	GdeltTsvRecord readRecord() throws IOException {
		if (exhausted) {
			return null;
		}
		throwIfInterrupted();
		budget.requireLoopAvailable();
		StringBuilder line = new StringBuilder(Math.min(maxRecordChars, 512));
		while (true) {
			int character = readNextCharacter();
			if (character == -1) {
				return finishAtEof(line);
			}
			rejectInitialBom(character);
			if (isRecordSeparator(character)) {
				return toRecord(line);
			}
			appendCharacter(line, character);
		}
	}

	private int readCharacter() throws IOException {
		while (bufferPosition == bufferLimit) {
			throwIfInterrupted();
			budget.requireLoopAvailable();
			bufferLimit = source.read(buffer);
			budget.requireLoopAvailable();
			bufferPosition = 0;
			if (bufferLimit == -1) {
				return -1;
			}
		}
		return buffer[bufferPosition++];
	}

	private int readNextCharacter() throws IOException {
		int character = readCharacter();
		if (!skipLineFeed) {
			return character;
		}
		skipLineFeed = false;
		return character == '\n' ? readCharacter() : character;
	}

	private void rejectInitialBom(int character) {
		if (!firstCharacter) {
			return;
		}
		firstCharacter = false;
		if (character == '\uFEFF') {
			throw new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH);
		}
	}

	private boolean isRecordSeparator(int character) {
		if (character == '\r') {
			skipLineFeed = true;
			return true;
		}
		return character == '\n';
	}

	private void appendCharacter(StringBuilder line, int character) {
		if (line.length() == maxRecordChars) {
			throw new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_RECORD_LIMIT_EXCEEDED);
		}
		line.append((char) character);
	}

	private GdeltTsvRecord finishAtEof(StringBuilder line) {
		exhausted = true;
		return line.isEmpty() ? null : toRecord(line);
	}

	private GdeltTsvRecord toRecord(StringBuilder line) {
		return new GdeltTsvRecord(
				nextLineNumber++,
				line.toString().split("\t", -1));
	}

	private static void throwIfInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throw new GdeltCsvInterruptedException();
		}
	}
}
