package com.neighbor.eventmosaic.gdelt.csv;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvAccessException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecordErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvSchemaException;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serial;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Общий lifecycle строгого Event/Mention reader без накопления полного файла.
 */
abstract class GdeltCsvReaderSupport<T> {

	private final GdeltArchiveKind kind;
	private final int expectedFieldCount;
	private final int maxRecordChars;
	private final GdeltCsvRecordMapper<T> mapper;
	private final GdeltCsvMetrics metrics;

	GdeltCsvReaderSupport(
			GdeltArchiveKind kind,
			int expectedFieldCount,
			GdeltCsvProperties properties,
			GdeltCsvRecordMapper<T> mapper,
			GdeltCsvMetrics metrics
	) {
		this.kind = Objects.requireNonNull(kind, "kind must not be null");
		if (expectedFieldCount <= 0) {
			throw new IllegalArgumentException("expectedFieldCount must be positive");
		}
		this.expectedFieldCount = expectedFieldCount;
		this.maxRecordChars = Objects.requireNonNull(properties, "properties must not be null").maxRecordChars();
		this.mapper = Objects.requireNonNull(mapper, "mapper must not be null");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	/**
	 * Открывает Path, синхронно передает валидные records и закрывает input.
	 */
	protected final GdeltCsvReadSummary readCsv(
			Path csvPath,
			GdeltRecordConsumer<T> consumer
	) {
		Objects.requireNonNull(csvPath, "csvPath must not be null");
		return readCsv(() -> strictUtf8Reader(csvPath), consumer);
	}

	/**
	 * Package-private seam для детерминированной проверки read/close failures и
	 * read-ahead без расширения публичного module API.
	 */
	final GdeltCsvReadSummary readCsv(
			Reader source,
			GdeltRecordConsumer<T> consumer
	) {
		Objects.requireNonNull(source, "source must not be null");
		return readCsv(() -> source, consumer);
	}

	private GdeltCsvReadSummary readCsv(
			ReaderOpener sourceOpener,
			GdeltRecordConsumer<T> consumer
	) {
		Objects.requireNonNull(consumer, "consumer must not be null");
		GdeltCsvCounters counters = new GdeltCsvCounters();
		try {
			GdeltCsvReadSummary summary = readSource(sourceOpener, consumer, counters);
			publish(counters, GdeltCsvFileOutcome.COMPLETED);
			return summary;
		} catch (ConsumerDeliveryFailure failure) {
			throw publishConsumerFailure(counters, failure);
		} catch (GdeltCsvInterruptedException exception) {
			publish(counters, GdeltCsvFileOutcome.INTERRUPTED);
			throw exception;
		} catch (GdeltCsvSchemaException exception) {
			publish(counters, GdeltCsvFileOutcome.SCHEMA_FAILED);
			throw exception;
		} catch (CharacterCodingException exception) {
			publish(counters, GdeltCsvFileOutcome.SCHEMA_FAILED);
			throw new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_ENCODING_INVALID, exception);
		} catch (IOException exception) {
			if (Thread.currentThread().isInterrupted()) {
				publish(counters, GdeltCsvFileOutcome.INTERRUPTED);
				throw new GdeltCsvInterruptedException(exception);
			}
			publish(counters, GdeltCsvFileOutcome.IO_FAILED);
			throw new GdeltCsvAccessException(GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE, exception);
		}
	}

	private RuntimeException publishConsumerFailure(
			GdeltCsvCounters counters,
			ConsumerDeliveryFailure failure
	) {
		RuntimeException original = failure.unwrap();
		try {
			publish(counters, GdeltCsvFileOutcome.CONSUMER_FAILED);
		} catch (RuntimeException metricsFailure) {
			if (metricsFailure != original) {
				original.addSuppressed(metricsFailure);
			}
		}
		return original;
	}

	private GdeltCsvReadSummary readSource(
			ReaderOpener sourceOpener,
			GdeltRecordConsumer<T> consumer,
			GdeltCsvCounters counters
	) throws IOException {
		try (Reader source = sourceOpener.open()) {
			GdeltTsvRecordReader records = new GdeltTsvRecordReader(source, maxRecordChars);
			GdeltTsvRecord csvRecord;
			while ((csvRecord = records.readRecord()) != null) {
				processRecord(csvRecord, consumer, counters);
			}
			if (!counters.hasShapeCompatibleRecords()) {
				throw new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH);
			}
			return counters.toSummary(kind);
		}
	}

	private void processRecord(
			GdeltTsvRecord csvRecord,
			GdeltRecordConsumer<T> consumer,
			GdeltCsvCounters counters
	) {
		if (csvRecord.fields().length != expectedFieldCount) {
			counters.rejected(
					csvRecord.lineNumber(),
					GdeltCsvRecordErrorCode.FIELD_COUNT_MISMATCH);
			return;
		}
		counters.shapeCompatible();
		GdeltCsvMappingResult<T> mapped = mapper.map(csvRecord.fields());
		if (!mapped.accepted()) {
			counters.rejected(csvRecord.lineNumber(), mapped.rejectionReason());
			return;
		}
		counters.accepted();
		try {
			consumer.accept(new GdeltCsvRecord<>(csvRecord.lineNumber(), mapped.value()));
		} catch (RuntimeException exception) {
			throw new ConsumerDeliveryFailure(exception);
		}
	}

	private static Reader strictUtf8Reader(Path csvPath) throws IOException {
		return new InputStreamReader(
				Files.newInputStream(csvPath),
				StandardCharsets.UTF_8.newDecoder()
						.onMalformedInput(CodingErrorAction.REPORT)
						.onUnmappableCharacter(CodingErrorAction.REPORT));
	}

	private void publish(GdeltCsvCounters counters, GdeltCsvFileOutcome outcome) {
		metrics.publish(
				kind,
				counters.validRecords(),
				counters.rejectionCounts(),
				outcome);
	}

	@FunctionalInterface
	private interface ReaderOpener {

		Reader open() throws IOException;
	}

	/**
	 * Отделяет downstream RuntimeException от однотипных source exceptions до
	 * выхода из try-with-resources и сохраняет close failures.
	 */
	private static final class ConsumerDeliveryFailure extends RuntimeException {

		@Serial
		private static final long serialVersionUID = 1L;

		private final RuntimeException original;

		private ConsumerDeliveryFailure(RuntimeException original) {
			super(null, original, true, false);
			this.original = original;
		}

		private RuntimeException unwrap() {
			for (Throwable suppressed : getSuppressed()) {
				if (suppressed != original) {
					original.addSuppressed(suppressed);
				}
			}
			return original;
		}
	}
}
