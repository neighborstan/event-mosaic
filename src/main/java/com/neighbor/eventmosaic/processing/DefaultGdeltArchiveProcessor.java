package com.neighbor.eventmosaic.processing;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvAccessException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvProgressListener;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvSchemaException;
import com.neighbor.eventmosaic.gdelt.api.GdeltEvent;
import com.neighbor.eventmosaic.gdelt.api.GdeltEventCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltMention;
import com.neighbor.eventmosaic.gdelt.api.GdeltMentionCsvReader;
import com.neighbor.eventmosaic.gdelt.api.GdeltRecordConsumer;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexedDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedEventDocument;
import com.neighbor.eventmosaic.indexing.api.IndexedMentionDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingFailureContract;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.stereotype.Service;

/**
 * Потоково отображает GDELT CSV в bounded Elasticsearch bulk operations.
 */
@Service
final class DefaultGdeltArchiveProcessor implements GdeltArchiveProcessor {

	private final GdeltEventCsvReader eventReader;
	private final GdeltMentionCsvReader mentionReader;
	private final GdeltEventDocumentMapper eventMapper;
	private final GdeltMentionDocumentMapper mentionMapper;
	private final GdeltIndexWriter indexWriter;
	private final ProcessingMetrics metrics;

	DefaultGdeltArchiveProcessor(
			GdeltEventCsvReader eventReader,
			GdeltMentionCsvReader mentionReader,
			GdeltEventDocumentMapper eventMapper,
			GdeltMentionDocumentMapper mentionMapper,
			GdeltIndexWriter indexWriter,
			ProcessingMetrics metrics
	) {
		this.eventReader = Objects.requireNonNull(eventReader, "eventReader must not be null");
		this.mentionReader = Objects.requireNonNull(
				mentionReader,
				"mentionReader must not be null");
		this.eventMapper = Objects.requireNonNull(eventMapper, "eventMapper must not be null");
		this.mentionMapper = Objects.requireNonNull(
				mentionMapper,
				"mentionMapper must not be null");
		this.indexWriter = Objects.requireNonNull(indexWriter, "indexWriter must not be null");
		this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
	}

	@Override
	public ArchiveProcessingResult process(
			ArchiveProcessingRequest request,
			ArchiveProcessingProgressListener progressListener,
			ArchiveProcessingDiagnosticListener diagnosticListener
	) {
		Objects.requireNonNull(request, "request must not be null");
		Objects.requireNonNull(progressListener, "progressListener must not be null");
		Objects.requireNonNull(diagnosticListener, "diagnosticListener must not be null");
		GdeltIndexKind indexKind = toIndexKind(request.kind());
		try {
			indexWriter.prepareReadModel();
		}
		catch (IndexingAccessException
				| IndexingInterruptedException
				| IndexingProtocolException exception) {
			return indexingFailure(
					request.kind(),
					ArchiveProcessingProgress.empty(),
					exception,
					exception,
					diagnosticListener);
		}

		int bulkSize = indexWriter.bulkSize();
		if (bulkSize <= 0) {
			throw new IllegalStateException("index writer bulkSize must be positive");
		}
		long maxBulkBytes = indexWriter.maxBulkBytes();
		if (maxBulkBytes <= 0) {
			throw new IllegalStateException("index writer maxBulkBytes must be positive");
		}
		return switch (request.kind()) {
			case TRANSLATION_EVENTS -> this.<GdeltEvent, IndexedEventDocument>processTyped(
					request,
					progressListener,
					diagnosticListener,
					indexKind,
					(consumer, sourceProgressListener) -> eventReader.read(
							request.csvPath(),
							consumer,
							sourceProgressListener),
					sourceRecord -> eventMapper.map(sourceRecord, request),
					bulkSize,
					maxBulkBytes);
			case TRANSLATION_MENTIONS -> this.<GdeltMention, IndexedMentionDocument>processTyped(
					request,
					progressListener,
					diagnosticListener,
					indexKind,
					(consumer, sourceProgressListener) -> mentionReader.read(
							request.csvPath(),
							consumer,
							sourceProgressListener),
					sourceRecord -> mentionMapper.map(sourceRecord, request),
					bulkSize,
					maxBulkBytes);
		};
	}

	private static GdeltIndexKind toIndexKind(GdeltArchiveKind kind) {
		return switch (kind) {
			case TRANSLATION_EVENTS -> GdeltIndexKind.EVENT;
			case TRANSLATION_MENTIONS -> GdeltIndexKind.MENTION;
		};
	}

	private <S, D extends GdeltIndexedDocument> ArchiveProcessingResult processTyped(
			ArchiveProcessingRequest request,
			ArchiveProcessingProgressListener progressListener,
			ArchiveProcessingDiagnosticListener diagnosticListener,
			GdeltIndexKind indexKind,
			CsvReadOperation<S> readOperation,
			RecordMappingOperation<S, D> mappingOperation,
			int bulkSize,
			long maxBulkBytes
	) {
		BatchAccumulator<D> accumulator = new BatchAccumulator<>(
				request.kind(),
				indexKind,
				request.sourceArchiveKey(),
				bulkSize,
				maxBulkBytes,
				indexWriter,
				progressListener,
				metrics);
		try {
			GdeltCsvReadSummary summary = readOperation.read(
					sourceRecord -> accumulator.accept(mappingOperation.map(sourceRecord)),
					accumulator::sourceInvalidProgress);
			accumulator.sourceCompleted(summary);
			accumulator.flushFinal();
			accumulator.refreshIndex();
			accumulator.verifyReceipt();
			ArchiveProcessingResult result = ArchiveProcessingResult.completed(
					request.kind(),
					accumulator.progress());
			metrics.archiveOutcome(request.kind(), result.outcome());
			return result;
		}
		catch (OwnershipLostSignal signal) {
			reportSuppressedDiagnostic(signal, diagnosticListener);
			ArchiveProcessingResult result = ArchiveProcessingResult.ownershipLost(
					request.kind(),
					accumulator.progress());
			metrics.archiveOutcome(request.kind(), result.outcome());
			return result;
		}
		catch (PartialBulkFailureSignal signal) {
			reportSuppressedDiagnostic(signal, diagnosticListener);
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.BULK_PARTIAL_FAILURE,
					signal.retryable(),
					signal.firstFailedLineNumber());
		}
		catch (ReceiptMismatchSignal signal) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					signal.code(),
					signal.retryable(),
					null);
		}
		catch (GdeltCsvInterruptedException exception) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED,
					true,
					null,
					exception,
					diagnosticListener);
		}
		catch (GdeltCsvAccessException exception) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.CSV_SOURCE_ACCESS_FAILURE,
					true,
					null,
					exception,
					diagnosticListener);
		}
		catch (GdeltCsvSchemaException exception) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.CSV_SOURCE_SCHEMA_FAILURE,
					false,
					null,
					exception,
					diagnosticListener);
		}
		catch (IndexingAccessException
				| IndexingInterruptedException
				| IndexingProtocolException exception) {
			return indexingFailure(
					request.kind(),
					accumulator.progress(),
					exception,
					exception,
					diagnosticListener);
		}
	}

	private ArchiveProcessingResult indexingFailure(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			RuntimeException diagnosticFailure,
			IndexingFailureContract failure,
			ArchiveProcessingDiagnosticListener diagnosticListener
	) {
		return expectedFailure(
				kind,
				progress,
				failure.interruptsProcessing()
						? ArchiveProcessingErrorCode.INDEXING_INTERRUPTED
						: ArchiveProcessingErrorCode.INDEXING_OPERATION_FAILURE,
				failure.retryable(),
				null,
				diagnosticFailure,
				diagnosticListener);
	}

	private ArchiveProcessingResult expectedFailure(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingErrorCode code,
			boolean retryable,
			Long firstFailedLineNumber
	) {
		return expectedFailure(
				kind,
				progress,
				new ArchiveProcessingFailure(code, retryable, firstFailedLineNumber));
	}

	private ArchiveProcessingResult expectedFailure(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingErrorCode code,
			boolean retryable,
			Long firstFailedLineNumber,
			RuntimeException diagnosticFailure,
			ArchiveProcessingDiagnosticListener diagnosticListener
	) {
		diagnosticListener.onFailure(diagnosticFailure);
		return expectedFailure(
				kind,
				progress,
				new ArchiveProcessingFailure(code, retryable, firstFailedLineNumber));
	}

	private ArchiveProcessingResult expectedFailure(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingFailure failure
	) {
		ArchiveProcessingErrorCode code = failure.code();
		metrics.failure(kind, code);
		metrics.archiveOutcome(kind, ArchiveProcessingOutcome.FAILED);
		return ArchiveProcessingResult.failed(kind, progress, failure);
	}

	private static void reportSuppressedDiagnostic(
			RuntimeException signal,
			ArchiveProcessingDiagnosticListener diagnosticListener
	) {
		if (signal.getSuppressed().length != 0) {
			diagnosticListener.onFailure(signal);
		}
	}

	@FunctionalInterface
	private interface CsvReadOperation<S> {

		GdeltCsvReadSummary read(
				GdeltRecordConsumer<S> consumer,
				GdeltCsvProgressListener progressListener);
	}

	@FunctionalInterface
	private interface RecordMappingOperation<S, D extends GdeltIndexedDocument> {

		DocumentMappingResult<D> map(GdeltCsvRecord<S> sourceRecord);
	}

	/**
	 * Mutable attempt-local state; никогда не переживает один вызов process.
	 */
	private static final class BatchAccumulator<D extends GdeltIndexedDocument> {

		private final GdeltArchiveKind archiveKind;
		private final GdeltIndexKind indexKind;
		private final String sourceArchiveKey;
		private final int bulkSize;
		private final long maxBulkBytes;
		private final GdeltIndexWriter indexWriter;
		private final ArchiveProcessingProgressListener progressListener;
		private final ProcessingMetrics metrics;
		private final List<D> documents;

		private long deliveredRecords;
		private long bufferedBytes;
		private long sourceInvalidRecords;
		private long mappingRejectedRecords;
		private long submittedOperations;
		private long succeededOperations;
		private long failedOperations;
		private long receiptDocuments;
		private Long firstFailedLineNumber;
		private ArchiveProcessingProgress lastCheckpoint;

		private BatchAccumulator(
				GdeltArchiveKind archiveKind,
				GdeltIndexKind indexKind,
				String sourceArchiveKey,
				int bulkSize,
				long maxBulkBytes,
				GdeltIndexWriter indexWriter,
				ArchiveProcessingProgressListener progressListener,
				ProcessingMetrics metrics
		) {
			this.archiveKind = archiveKind;
			this.indexKind = indexKind;
			this.sourceArchiveKey = sourceArchiveKey;
			this.bulkSize = bulkSize;
			this.maxBulkBytes = maxBulkBytes;
			this.indexWriter = indexWriter;
			this.progressListener = progressListener;
			this.metrics = metrics;
			this.documents = new ArrayList<>(bulkSize);
		}

		private void accept(DocumentMappingResult<D> mapping) {
			deliveredRecords = Math.incrementExact(deliveredRecords);
			if (!mapping.accepted()) {
				mappingRejectedRecords = Math.incrementExact(mappingRejectedRecords);
				metrics.rejected(archiveKind, mapping.rejection());
			}
			else {
				metrics.mapped(archiveKind);
				metrics.invalidGeoCandidates(archiveKind, mapping.invalidGeoCandidates());
				D document = mapping.document();
				long operationBytes = indexWriter.estimateBulkOperationBytes(document);
				if (operationBytes <= 0) {
					throw new IllegalStateException(
							"index writer operation byte estimate must be positive");
				}
				if (operationBytes > maxBulkBytes) {
					throw new IndexingProtocolException(
							IndexingErrorCode.INDEXING_REQUEST_REJECTED);
				}
				if (!documents.isEmpty()
						&& (documents.size() == bulkSize
						|| operationBytes > maxBulkBytes - bufferedBytes)) {
					flush();
				}
				documents.add(document);
				bufferedBytes = Math.addExact(bufferedBytes, operationBytes);
			}
			if (processedSourceRecords() % bulkSize == 0) {
				checkpoint();
			}
		}

		private void sourceInvalidProgress(long invalidRecords) {
			if (invalidRecords < sourceInvalidRecords) {
				throw new IllegalStateException(
						"source invalid progress must be monotonic");
			}
			sourceInvalidRecords = invalidRecords;
			if (processedSourceRecords() % bulkSize == 0) {
				checkpoint();
			}
		}

		private void sourceCompleted(GdeltCsvReadSummary summary) {
			if (summary.kind() != archiveKind
					|| summary.validRecords() != deliveredRecords) {
				throw new IllegalStateException(
						"CSV reader returned inconsistent terminal summary");
			}
			sourceInvalidProgress(summary.invalidRecords());
			checkpoint();
		}

		private void flushFinal() {
			if (!documents.isEmpty()) {
				flush();
			}
		}

		private void flush() {
			long batchCount = documents.size();
			submittedOperations = Math.addExact(submittedOperations, batchCount);
			BulkIndexResult result;
			try {
				result = indexWriter.write(new BulkIndexCommand<>(
						indexKind,
						documents));
			}
			catch (IndexingAccessException
					| IndexingInterruptedException
					| IndexingProtocolException exception) {
				checkpoint();
				throw exception;
			}
			if (result.kind() != indexKind || result.submitted() != batchCount) {
				throw new IllegalStateException("index writer returned inconsistent bulk result");
			}
			succeededOperations = Math.addExact(succeededOperations, result.succeeded());
			failedOperations = Math.addExact(failedOperations, result.failed());
			if (firstFailedLineNumber == null) {
				firstFailedLineNumber = result.firstFailedLineNumber();
			}
			documents.clear();
			bufferedBytes = 0;
			checkpoint();
			if (!result.successful()) {
				throw new PartialBulkFailureSignal(
						result.retryable(),
						result.firstFailedLineNumber());
			}
		}

		private void refreshIndex() {
			indexWriter.refresh(indexKind);
		}

		private void verifyReceipt() {
			ArchiveReceiptVerification receipt = indexWriter.verifyReceipt(new ArchiveReceiptQuery(
					indexKind,
					sourceArchiveKey,
					succeededOperations));
			receiptDocuments = receipt.actualDocumentCount();
			checkpoint();
			if (!receipt.matched()) {
				throw new ReceiptMismatchSignal(
						receipt.expectedDocumentCount(),
						receipt.actualDocumentCount());
			}
		}

		private void checkpoint() {
			ArchiveProcessingProgress current = progress();
			if (current.equals(lastCheckpoint)) {
				return;
			}
			if (!progressListener.onProgress(current)) {
				throw new OwnershipLostSignal();
			}
			lastCheckpoint = current;
		}

		private ArchiveProcessingProgress progress() {
			return new ArchiveProcessingProgress(
					deliveredRecords,
					sourceInvalidRecords,
					mappingRejectedRecords,
					submittedOperations,
					succeededOperations,
					failedOperations,
					receiptDocuments,
					firstFailedLineNumber);
		}

		private long processedSourceRecords() {
			return Math.addExact(deliveredRecords, sourceInvalidRecords);
		}
	}

	/**
	 * Внутренний control-flow signal для немедленной остановки callback reader.
	 */
	private static final class OwnershipLostSignal extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private OwnershipLostSignal() {
			super(null, null, true, false);
		}
	}

	/**
	 * Внутренний signal подтвержденного partial bulk result.
	 */
	private static final class PartialBulkFailureSignal extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private final boolean retryable;
		private final Long firstFailedLineNumber;

		private PartialBulkFailureSignal(boolean retryable, Long firstFailedLineNumber) {
			super(null, null, true, false);
			this.retryable = retryable;
			this.firstFailedLineNumber = firstFailedLineNumber;
		}

		private boolean retryable() {
			return retryable;
		}

		private Long firstFailedLineNumber() {
			return firstFailedLineNumber;
		}
	}

	/**
	 * Внутренний signal failed terminal receipt.
	 */
	private static final class ReceiptMismatchSignal extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private final ArchiveProcessingErrorCode code;
		private final boolean retryable;

		private ReceiptMismatchSignal(long expectedDocuments, long actualDocuments) {
			super(null, null, true, false);
			boolean surplus = actualDocuments > expectedDocuments;
			this.code = surplus
					? ArchiveProcessingErrorCode.INDEX_RECEIPT_SURPLUS
					: ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH;
			this.retryable = !surplus;
		}

		private ArchiveProcessingErrorCode code() {
			return code;
		}

		private boolean retryable() {
			return retryable;
		}
	}
}
