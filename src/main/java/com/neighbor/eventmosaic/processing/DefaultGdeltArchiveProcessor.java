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
import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptStatus;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptVerification;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.EventIdentityConflictException;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
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
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexWriteMode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingFailure;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.GdeltArchiveProcessor;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import io.micrometer.core.instrument.Timer;
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
		Timer.Sample timer = metrics.startArchiveTimer();
		ArchiveProcessingOutcome metricOutcome = ArchiveProcessingOutcome.FAILED;
		try {
			GdeltIndexKind indexKind = toIndexKind(request.kind());
			ExactIndexTarget indexTarget = request.indexTargets().target(indexKind);

			int bulkSize = indexWriter.bulkSize();
			if (bulkSize <= 0) {
				throw new IllegalStateException("index writer bulkSize must be positive");
			}
			long maxBulkBytes = indexWriter.maxBulkBytes();
			if (maxBulkBytes <= 0) {
				throw new IllegalStateException("index writer maxBulkBytes must be positive");
			}
			ArchiveProcessingResult result = switch (request.kind()) {
				case TRANSLATION_EVENTS ->
						this.<GdeltEvent, IndexedEventDocument>processTyped(
								request,
								progressListener,
								diagnosticListener,
								indexKind,
								indexTarget,
								(consumer, sourceProgressListener) -> eventReader.read(
										request.csvPath(),
										consumer,
										sourceProgressListener),
								sourceRecord -> eventMapper.map(sourceRecord, request),
								bulkSize,
								maxBulkBytes);
				case TRANSLATION_MENTIONS ->
						this.<GdeltMention, IndexedMentionDocument>processTyped(
								request,
								progressListener,
								diagnosticListener,
								indexKind,
								indexTarget,
								(consumer, sourceProgressListener) -> mentionReader.read(
										request.csvPath(),
										consumer,
										sourceProgressListener),
								sourceRecord -> mentionMapper.map(sourceRecord, request),
								bulkSize,
								maxBulkBytes);
			};
			metricOutcome = result.outcome();
			return result;
		}
		finally {
			metrics.archiveDuration(timer, request.kind(), metricOutcome);
		}
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
			ExactIndexTarget indexTarget,
			CsvReadOperation<S> readOperation,
			RecordMappingOperation<S, D> mappingOperation,
			int bulkSize,
			long maxBulkBytes
	) {
		BatchAccumulator<D> accumulator = new BatchAccumulator<>(
				request.kind(),
				indexKind,
				indexTarget,
				request.sourceArchiveKey(),
				request.processingFingerprint(),
				request.writeMode(),
				bulkSize,
				maxBulkBytes,
				request.receiptPageSize(),
				request.operationBudget(),
				indexWriter,
				progressListener,
				metrics);
		try {
			ensureRemaining(request.operationBudget());
			GdeltCsvReadSummary summary = readOperation.read(
					sourceRecord -> accumulator.accept(mappingOperation.map(sourceRecord)),
					accumulator::sourceInvalidProgress);
			accumulator.sourceCompleted(summary);
			accumulator.flushFinal();
			accumulator.refreshIndex();
			ArchiveReceiptVerification receipt = accumulator.verifyReceipt();
			ArchiveProcessingResult result = ArchiveProcessingResult.completed(
					request.kind(),
					accumulator.progress(),
					receipt);
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
		catch (DeadlineExceededSignal _) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED,
					true,
					null);
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
			return expectedReceiptFailure(
					request.kind(),
					accumulator.progress(),
					signal.code(),
					signal.retryable(),
					signal.verification());
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
		catch (EventIdentityConflictException exception) {
			return expectedFailure(
					request.kind(),
					accumulator.progress(),
					ArchiveProcessingErrorCode.EVENT_IDENTITY_CONFLICT,
					false,
					exception.sourceLineNumber(),
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

	private ArchiveProcessingResult expectedReceiptFailure(
			GdeltArchiveKind kind,
			ArchiveProcessingProgress progress,
			ArchiveProcessingErrorCode code,
			boolean retryable,
			ArchiveReceiptVerification receipt
	) {
		metrics.failure(kind, code);
		metrics.archiveOutcome(kind, ArchiveProcessingOutcome.FAILED);
		return ArchiveProcessingResult.failed(
				kind,
				progress,
				new ArchiveProcessingFailure(code, retryable, null),
				receipt);
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
		private final ExactIndexTarget indexTarget;
		private final String sourceArchiveKey;
		private final String processingFingerprint;
		private final IndexWriteMode writeMode;
		private final int bulkSize;
		private final long maxBulkBytes;
		private final int receiptPageSize;
		private final OperationBudget operationBudget;
		private final GdeltIndexWriter indexWriter;
		private final ArchiveProcessingProgressListener progressListener;
		private final ProcessingMetrics metrics;
		private final List<D> documents;
		private final ArchiveIdentityDigest.Accumulator expectedDigest;

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
		private ArchiveReceiptVerification receipt;

		private BatchAccumulator(
				GdeltArchiveKind archiveKind,
				GdeltIndexKind indexKind,
				ExactIndexTarget indexTarget,
				String sourceArchiveKey,
				String processingFingerprint,
				IndexWriteMode writeMode,
				int bulkSize,
				long maxBulkBytes,
				int receiptPageSize,
				OperationBudget operationBudget,
				GdeltIndexWriter indexWriter,
				ArchiveProcessingProgressListener progressListener,
				ProcessingMetrics metrics
		) {
			this.archiveKind = archiveKind;
			this.indexKind = indexKind;
			this.indexTarget = indexTarget;
			this.sourceArchiveKey = sourceArchiveKey;
			this.processingFingerprint = processingFingerprint;
			this.writeMode = writeMode;
			this.bulkSize = bulkSize;
			this.maxBulkBytes = maxBulkBytes;
			this.receiptPageSize = receiptPageSize;
			this.operationBudget = operationBudget;
			this.indexWriter = indexWriter;
			this.progressListener = progressListener;
			this.metrics = metrics;
			this.documents = new ArrayList<>(bulkSize);
			this.expectedDigest = ArchiveIdentityDigest.accumulator();
		}

		private void accept(DocumentMappingResult<D> mapping) {
			ensureRemaining(operationBudget);
			deliveredRecords = Math.incrementExact(deliveredRecords);
			if (!mapping.accepted()) {
				mappingRejectedRecords = Math.incrementExact(mappingRejectedRecords);
				metrics.rejected(archiveKind, mapping.rejection());
			}
			else {
				metrics.mapped(archiveKind);
				metrics.invalidGeoCandidates(archiveKind, mapping.invalidGeoCandidates());
				D document = mapping.document();
				expectedDigest.addIdentity(document.documentId());
				long operationBytes = indexWriter.estimateBulkOperationBytes(
						indexTarget,
						document);
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
			ensureRemaining(operationBudget);
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
			ensureRemaining(operationBudget);
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
			ensureRemaining(operationBudget);
			long batchCount = documents.size();
			submittedOperations = Math.addExact(submittedOperations, batchCount);
			BulkIndexResult result;
			try {
				result = indexWriter.write(new BulkIndexCommand<>(
						indexKind,
						indexTarget,
						writeMode,
						documents));
			}
			catch (IndexTargetUnavailableException
					| EventIdentityConflictException
					| IndexingAccessException
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
			ensureRemaining(operationBudget);
			indexWriter.refresh(indexKind, indexTarget);
		}

		private ArchiveReceiptVerification verifyReceipt() {
			ensureRemaining(operationBudget);
			ArchiveIdentityDigest expected = expectedDigest.finish();
			receipt = indexWriter.verifyReceipt(new ArchiveReceiptQuery(
					indexKind,
					indexTarget,
					sourceArchiveKey,
					processingFingerprint,
					succeededOperations,
					expected,
					receiptPageSize));
			receiptDocuments = receipt.actualDocumentCount();
			checkpoint();
			if (!receipt.matched()) {
				throw new ReceiptMismatchSignal(receipt);
			}
			return receipt;
		}

		private void checkpoint() {
			ensureRemaining(operationBudget);
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

	private static void ensureRemaining(OperationBudget budget) {
		if (!budget.hasRemaining()) {
			throw new DeadlineExceededSignal();
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

	/** Внутренний control-flow signal исчерпанной operation deadline. */
	private static final class DeadlineExceededSignal extends RuntimeException {

		private static final long serialVersionUID = 1L;

		private DeadlineExceededSignal() {
			super(null, null, false, false);
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

		private final ArchiveReceiptVerification verification;

		private ReceiptMismatchSignal(ArchiveReceiptVerification verification) {
			super(null, null, true, false);
			this.verification = verification;
			boolean surplus = verification.status() == ArchiveReceiptStatus.SURPLUS;
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

		private ArchiveReceiptVerification verification() {
			return verification;
		}
	}
}
