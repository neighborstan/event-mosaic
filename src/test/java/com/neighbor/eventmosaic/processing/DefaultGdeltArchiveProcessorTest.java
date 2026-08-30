package com.neighbor.eventmosaic.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.neighbor.eventmosaic.gdelt.api.GdeltArchiveKind;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvAccessException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvErrorCode;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvInterruptedException;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvProgressListener;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvRecord;
import com.neighbor.eventmosaic.gdelt.api.GdeltCsvReadSummary;
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
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.EventIdentityConflictException;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexKind;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexedDocument;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingInterruptedException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingDiagnosticListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingErrorCode;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingOutcome;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgress;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingProgressListener;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingResult;
import com.neighbor.eventmosaic.processing.api.ArchiveProcessingRequest;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import com.neighbor.eventmosaic.shared.time.OperationLeaseSnapshot;
import com.neighbor.eventmosaic.shared.time.OperationOwnershipLostException;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Пакетная обработка архивов GDELT")
class DefaultGdeltArchiveProcessorTest {

	private static final String ARCHIVE_DURATION_METER =
			"event_mosaic.processing.archive.duration";
	private static final Instant EVENT_TIME = Instant.parse("2026-07-21T14:30:00Z");
	private static final Instant MENTION_TIME = Instant.parse("2026-07-21T14:44:00Z");

	@Test
	@DisplayName("Успешная обработка архива один раз завершает измерение с видом и итогом")
	void recordsCompletedArchiveDurationOnce() {
		SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
		DefaultGdeltArchiveProcessor processor = processor(
				new FakeIndexWriter(2),
				validMentions(1),
				null,
				meterRegistry);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		Timer timer = meterRegistry.find(ARCHIVE_DURATION_METER)
				.tag("kind", "translation_mentions")
				.tag("outcome", "completed")
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(timer.getId().getTags())
				.extracting(Tag::getKey)
				.containsExactlyInAnyOrder("kind", "outcome");
	}

	@Test
	@DisplayName("Неожиданный отказ один раз завершает измерение как неуспешную обработку")
	void recordsUnexpectedArchiveFailureOnce() {
		SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
		IllegalStateException defect = new IllegalStateException("unexpected defect");
		GdeltMentionCsvReader reader = (path, consumer) -> {
			throw defect;
		};
		DefaultGdeltArchiveProcessor processor = processor(
				new FakeIndexWriter(2),
				reader,
				meterRegistry);

		assertThatThrownBy(() -> processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing()))
				.isSameAs(defect);

		Timer timer = meterRegistry.find(ARCHIVE_DURATION_METER)
				.tag("kind", "translation_mentions")
				.tag("outcome", "failed")
				.timer();
		assertThat(timer).isNotNull();
		assertThat(timer.count()).isEqualTo(1);
		assertThat(meterRegistry.find(ARCHIVE_DURATION_METER).timers()).hasSize(1);
	}

	@Test
	@DisplayName("Исчерпанная deadline не открывает CSV и не отправляет bulk")
	void expiredDeadlinePreventsCsvAndBulk() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		nanoTime.set(Duration.ofSeconds(1).toNanos());
		ArchiveProcessingRequest base = ProcessingTestFixtures.mentionRequest();
		ArchiveProcessingRequest request = new ArchiveProcessingRequest(
				base.kind(),
				base.sourceUpdateTime(),
				base.sourceArchiveKey(),
				base.processingFingerprint(),
				base.indexTargets(),
				base.csvPath(),
				base.receiptPageSize(),
				budget);

		ArchiveProcessingResult result = processor.process(
				request,
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED);
		assertThat(result.failure().retryable()).isTrue();
		assertThat(result.progress()).isEqualTo(ArchiveProcessingProgress.empty());
		assertThat(writer.commands).isEmpty();
	}

	@Test
	@DisplayName("Потерянное global-владение не открывает CSV и останавливает cycle")
	void lostOwnershipPreventsCsvAndBulk() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		AtomicInteger delivered = new AtomicInteger();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(5))
				.withLeaseGuard(OperationLeaseSnapshot::lost, Duration.ZERO);
		ArchiveProcessingRequest base = ProcessingTestFixtures.mentionRequest();
		ArchiveProcessingRequest request = new ArchiveProcessingRequest(
				base.kind(),
				base.sourceUpdateTime(),
				base.sourceArchiveKey(),
				base.processingFingerprint(),
				base.indexTargets(),
				base.csvPath(),
				base.receiptPageSize(),
				budget);

		assertThatThrownBy(() -> processor(
				writer,
				validMentions(1),
				delivered).process(
						request,
						ArchiveProcessingProgressListener.continuing()))
				.isInstanceOf(OperationOwnershipLostException.class);
		assertThat(delivered).hasValue(0);
		assertThat(writer.commands).isEmpty();
	}

	@Test
	@DisplayName("Deadline во время CSV сохраняет partial progress и не отправляет новый bulk")
	void deadlineDuringCsvPreservesPartialProgress() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		AtomicLong nanoTime = new AtomicLong();
		OperationBudget budget = OperationBudget.start(Duration.ofSeconds(1), nanoTime::get);
		List<GdeltMention> mentions = validMentions(2);
		GdeltMentionCsvReader reader = (path, consumer) -> {
			consumer.accept(new GdeltCsvRecord<>(1, mentions.getFirst()));
			nanoTime.set(Duration.ofSeconds(1).toNanos());
			consumer.accept(new GdeltCsvRecord<>(2, mentions.getLast()));
			throw new AssertionError("Expired reader callback must stop processing");
		};
		ArchiveProcessingRequest base = ProcessingTestFixtures.mentionRequest();
		ArchiveProcessingRequest request = new ArchiveProcessingRequest(
				base.kind(),
				base.sourceUpdateTime(),
				base.sourceArchiveKey(),
				base.processingFingerprint(),
				base.indexTargets(),
				base.csvPath(),
				base.receiptPageSize(),
				budget);

		ArchiveProcessingResult result = processor(writer, reader).process(
				request,
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.OPERATION_DEADLINE_EXCEEDED);
		assertThat(result.progress().deliveredRecords()).isEqualTo(1);
		assertThat(result.progress().submittedOperations()).isZero();
		assertThat(writer.commands).isEmpty();
	}

	@Test
	@DisplayName("Отправляет exact full batch и явно refresh-ит индекс перед receipt")
	void keepsExactFullBatchForTerminalRefresh() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(2), null);
		ArchiveProcessingRequest request = ProcessingTestFixtures.mentionRequest();

		ArchiveProcessingResult result = processor.process(
				request,
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(writer.commands).hasSize(1);
		assertThat(writer.commands.getFirst().documents()).hasSize(2);
		assertThat(writer.refreshedKinds).containsExactly(GdeltIndexKind.MENTION);
		assertThat(writer.receiptQueries).containsExactly(new ArchiveReceiptQuery(
				writer.commands.getFirst().kind(),
				ProcessingTestFixtures.ACTIVE_TARGETS.mention(),
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				"1".repeat(64),
				2,
				mentionDigest(1, 2),
				500));
		assertThat(result.receipt())
				.isNotNull()
				.satisfies(receipt -> {
					assertThat(receipt.status()).isEqualTo(ArchiveReceiptStatus.MATCHED);
					assertThat(receipt.expectedDigest()).isEqualTo(mentionDigest(1, 2));
					assertThat(receipt.actualDigest()).isEqualTo(mentionDigest(1, 2));
				});
		assertThat(writer.commands.getFirst().target())
				.isEqualTo(ProcessingTestFixtures.ACTIVE_TARGETS.mention());
		assertThat(writer.estimatedTargets)
				.containsOnly(ProcessingTestFixtures.ACTIVE_TARGETS.mention());
		assertThat(writer.requestBudgets)
				.containsExactly(
						request.operationBudget(),
						request.operationBudget(),
						request.operationBudget());
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(2, 0, 0, 2, 2, 0, 2, null));
	}

	@Test
	@DisplayName("Делит bulkSize умножить на два плюс один на bounded batches")
	void splitsFullBatchesAndFinalTail() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(5), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(writer.commands)
				.extracting(command -> command.documents().size())
				.containsExactly(2, 2, 1);
		assertThat(writer.refreshedKinds).containsExactly(GdeltIndexKind.MENTION);
		assertThat(result.progress().receiptDocuments()).isEqualTo(5);
	}

	@Test
	@DisplayName("Делит bulk по byte limit даже до достижения count limit")
	void splitsBatchesByEstimatedBytes() {
		FakeIndexWriter writer = new FakeIndexWriter(100, 10, 5);
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(3), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(writer.commands)
				.extracting(command -> command.documents().size())
				.containsExactly(2, 1);
		assertThat(result.progress().receiptDocuments()).isEqualTo(3);
	}

	@Test
	@DisplayName("Отклоняет одиночную oversized operation как permanent failure")
	void rejectsSingleOversizedOperation() {
		FakeIndexWriter writer = new FakeIndexWriter(100, 4, 5);
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(2), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.INDEXING_OPERATION_FAILURE);
		assertThat(result.failure().retryable()).isFalse();
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(1, 0, 0, 0, 0, 0, 0, null));
		assertThat(writer.commands).isEmpty();
		assertThat(writer.refreshedKinds).isEmpty();
		assertThat(writer.receiptQueries).isEmpty();
	}

	@Test
	@DisplayName("Завершает archive без пустого bulk при одних mapping rejections")
	void completesWithoutEmptyBulkWhenAllMappingsAreRejected() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		List<GdeltMention> mentions = List.of(
				mentionWithMissingTime(1),
				mentionWithMissingTime(2),
				mentionWithMissingTime(3));
		DefaultGdeltArchiveProcessor processor = processor(writer, mentions, null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(writer.commands).isEmpty();
		assertThat(writer.refreshedKinds).containsExactly(GdeltIndexKind.MENTION);
		assertThat(writer.receiptQueries).containsExactly(new ArchiveReceiptQuery(
				GdeltIndexKind.MENTION,
				ProcessingTestFixtures.ACTIVE_TARGETS.mention(),
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				"1".repeat(64),
				0,
				mentionDigest(),
				500));
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(3, 0, 3, 0, 0, 0, 0, null));
	}

	@Test
	@DisplayName("Digest включает только accepted identities в порядке source")
	void hashesOnlyAcceptedIdentitiesInSourceOrder() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		List<GdeltMention> mentions = List.of(
				validMentions(1).getFirst(),
				mentionWithMissingTime(2),
				ProcessingTestFixtures.mention(
						700_000_003L,
						EVENT_TIME,
						MENTION_TIME,
						1,
						"identifier-2"));
		DefaultGdeltArchiveProcessor processor = processor(writer, mentions, null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.COMPLETED);
		assertThat(writer.receiptQueries).containsExactly(new ArchiveReceiptQuery(
				GdeltIndexKind.MENTION,
				ProcessingTestFixtures.ACTIVE_TARGETS.mention(),
				ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
				"1".repeat(64),
				2,
				mentionDigest(1, 3),
				500));
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(3, 0, 1, 2, 2, 0, 2, null));
	}

	@Test
	@DisplayName("Прекращает чтение после первого retryable partial bulk")
	void stopsAfterFirstRetryablePartialBulk() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		writer.nextResult = new BulkIndexResult(
				GdeltIndexKind.MENTION,
				2,
				1,
				1,
				2L,
				BulkIndexOutcome.RETRYABLE_PARTIAL_FAILURE);
		AtomicInteger delivered = new AtomicInteger();
		DefaultGdeltArchiveProcessor processor = processor(
				writer,
				validMentions(5),
				delivered);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.BULK_PARTIAL_FAILURE);
		assertThat(result.failure().retryable()).isTrue();
		assertThat(result.failure().firstFailedLineNumber()).isEqualTo(2);
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(3, 0, 0, 2, 1, 1, 0, 2L));
		assertThat(delivered).hasValue(3);
		assertThat(writer.commands).hasSize(1);
		assertThat(writer.receiptQueries).isEmpty();
	}

	@Test
	@DisplayName("Передает typed missing target owning processing boundary")
	void propagatesMissingExactTarget() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		IndexTargetUnavailableException failure = new IndexTargetUnavailableException(
				IndexTargetUnavailableReason.MISSING);
		writer.writeFailure = failure;
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(2), null);

		assertThatThrownBy(() -> processor.process(
					ProcessingTestFixtures.mentionRequest(),
					ArchiveProcessingProgressListener.continuing(),
					diagnostic::set))
				.isSameAs(failure);

		assertThat(diagnostic).hasNullValue();
		assertThat(writer.receiptQueries).isEmpty();
	}

	@Test
	@DisplayName("Передает typed write block при refresh exact generation")
	void propagatesWriteBlockDuringRefresh() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		IndexTargetUnavailableException failure = new IndexTargetUnavailableException(
				IndexTargetUnavailableReason.WRITE_BLOCKED);
		writer.refreshFailure = failure;
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(2), null);

		assertThatThrownBy(() -> processor.process(
					ProcessingTestFixtures.mentionRequest(),
					ArchiveProcessingProgressListener.continuing(),
					diagnostic::set))
				.isSameAs(failure);

		assertThat(diagnostic).hasNullValue();
		assertThat(writer.receiptQueries).isEmpty();
	}

	@Test
	@DisplayName("Передает typed replaced target при count receipt")
	void propagatesReplacedTargetDuringReceipt() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		IndexTargetUnavailableException failure = new IndexTargetUnavailableException(
				IndexTargetUnavailableReason.REPLACED);
		writer.receiptFailure = failure;
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(2), null);

		assertThatThrownBy(() -> processor.process(
					ProcessingTestFixtures.mentionRequest(),
					ArchiveProcessingProgressListener.continuing(),
					diagnostic::set))
				.isSameAs(failure);

		assertThat(diagnostic).hasNullValue();
		assertThat(writer.receiptQueries).containsExactly(
				new ArchiveReceiptQuery(
						GdeltIndexKind.MENTION,
						ProcessingTestFixtures.mentionRequest().indexTargets().mention(),
						ProcessingTestFixtures.mentionRequest().sourceArchiveKey(),
						ProcessingTestFixtures.mentionRequest().processingFingerprint(),
						2,
						mentionDigest(1, 2),
						500));
	}

	@Test
	@DisplayName("Останавливает reader как concurrency outcome при потере ownership")
	void stopsReaderWhenProgressListenerLosesOwnership() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		AtomicInteger delivered = new AtomicInteger();
		DefaultGdeltArchiveProcessor processor = processor(
				writer,
				List.of(
						mentionWithMissingTime(1),
						mentionWithMissingTime(2),
						mentionWithMissingTime(3)),
				delivered);
		List<ArchiveProcessingProgress> checkpoints = new ArrayList<>();

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				progress -> {
					checkpoints.add(progress);
					return false;
				});

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.OWNERSHIP_LOST);
		assertThat(result.failure()).isNull();
		assertThat(result.progress().deliveredRecords()).isEqualTo(2);
		assertThat(delivered).hasValue(2);
		assertThat(checkpoints).containsExactly(
				new ArchiveProcessingProgress(2, 0, 2, 0, 0, 0, 0, null));
		assertThat(writer.commands).isEmpty();
	}

	@Test
	@DisplayName("Продлевает lease на invalid-only prefix и сохраняет progress при source failure")
	void checkpointsInvalidOnlyPrefixBeforeSourceFailure() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		GdeltCsvAccessException sourceFailure = new GdeltCsvAccessException(
				GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE);
		GdeltMentionCsvReader reader = new GdeltMentionCsvReader() {
			@Override
			public GdeltCsvReadSummary read(
					Path csvPath,
					GdeltRecordConsumer<GdeltMention> consumer
			) {
				throw new AssertionError("Progress-aware reader overload must be used");
			}

			@Override
			public GdeltCsvReadSummary read(
					Path csvPath,
					GdeltRecordConsumer<GdeltMention> consumer,
					GdeltCsvProgressListener progressListener
			) {
				for (long invalidRecords = 1; invalidRecords <= 5; invalidRecords++) {
					progressListener.onInvalidRecords(invalidRecords);
				}
				throw sourceFailure;
			}
		};
		List<ArchiveProcessingProgress> checkpoints = new ArrayList<>();

		ArchiveProcessingResult result = processor(writer, reader).process(
				ProcessingTestFixtures.mentionRequest(),
				progress -> {
					checkpoints.add(progress);
					return true;
				});

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.CSV_SOURCE_ACCESS_FAILURE);
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(0, 5, 0, 0, 0, 0, 0, null));
		assertThat(checkpoints).containsExactly(
				new ArchiveProcessingProgress(0, 2, 0, 0, 0, 0, 0, null),
				new ArchiveProcessingProgress(0, 4, 0, 0, 0, 0, 0, null));
	}

	@Test
	@DisplayName("Передает cleanup failure из ownership signal в diagnostic")
	void preservesCleanupFailureFromOwnershipSignal() {
		FakeIndexWriter writer = new FakeIndexWriter(1);
		IOException cleanupFailure = new IOException("synthetic close failure");
		GdeltMentionCsvReader reader = new GdeltMentionCsvReader() {
			@Override
			public GdeltCsvReadSummary read(
					Path csvPath,
					GdeltRecordConsumer<GdeltMention> consumer
			) {
				throw new AssertionError("Progress-aware reader overload must be used");
			}

			@Override
			public GdeltCsvReadSummary read(
					Path csvPath,
					GdeltRecordConsumer<GdeltMention> consumer,
					GdeltCsvProgressListener progressListener
			) {
				try {
					progressListener.onInvalidRecords(1);
				}
				catch (RuntimeException signal) {
					signal.addSuppressed(cleanupFailure);
					throw signal;
				}
				throw new AssertionError("Ownership loss must stop the reader");
			}
		};
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();

		ArchiveProcessingResult result = processor(writer, reader).process(
				ProcessingTestFixtures.mentionRequest(),
				_ -> false,
				diagnostic::set);

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.OWNERSHIP_LOST);
		assertThat(result.progress().sourceInvalidRecords()).isEqualTo(1);
		assertThat(diagnostic.get()).isNotNull();
		assertThat(diagnostic.get().getSuppressed()).containsExactly(cleanupFailure);
	}

	@Test
	@DisplayName("Сохраняет submitted без ложных confirmations при transport failure")
	void preservesUnknownTransportOutcome() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		IndexingAccessException writeFailure = new IndexingAccessException(
				IndexingErrorCode.INDEXING_UNAVAILABLE);
		writer.writeFailure = writeFailure;
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing(),
				diagnostic::set);

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.INDEXING_OPERATION_FAILURE);
		assertThat(result.failure().retryable()).isTrue();
		assertThat(diagnostic).hasValue(writeFailure);
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(1, 0, 0, 1, 0, 0, 0, null));
	}

	@Test
	@DisplayName("Проецирует indexing interruption и сохраняет interrupt flag")
	void preservesIndexingInterruptSignal() {
		Thread.currentThread().interrupt();
		try {
			FakeIndexWriter writer = new FakeIndexWriter(2);
			writer.writeFailure =
					new IndexingInterruptedException(new java.io.IOException("interrupted"));
			DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);
			AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();

			ArchiveProcessingResult result = processor.process(
					ProcessingTestFixtures.mentionRequest(),
					ArchiveProcessingProgressListener.continuing(),
					diagnostic::set);

			assertThat(result.failure().code())
					.isEqualTo(ArchiveProcessingErrorCode.INDEXING_INTERRUPTED);
			assertThat(result.failure().retryable()).isTrue();
			assertThat(diagnostic).hasValue(writer.writeFailure);
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		}
		finally {
			Thread.interrupted();
		}
	}

	@Test
	@DisplayName("Event identity conflict остается отдельным non-retryable failure")
	void projectsEventIdentityConflictWithoutOverwriteRetry() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		EventIdentityConflictException conflict = new EventIdentityConflictException(1);
		writer.writeFailure = conflict;
		AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
		DefaultGdeltArchiveProcessor processor = eventProcessor(writer);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.eventRequest(),
				ArchiveProcessingProgressListener.continuing(),
				diagnostic::set);

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure()).satisfies(failure -> {
			assertThat(failure.code())
					.isEqualTo(ArchiveProcessingErrorCode.EVENT_IDENTITY_CONFLICT);
			assertThat(failure.retryable()).isFalse();
			assertThat(failure.firstFailedLineNumber()).isEqualTo(1);
		});
		assertThat(result.receipt()).isNull();
		assertThat(diagnostic).hasValue(conflict);
		assertThat(writer.receiptQueries).isEmpty();
	}

	@Test
	@DisplayName("Не завершает archive без совпавшей terminal receipt")
	void failsOnTerminalReceiptMismatch() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		writer.receiptStatus = ArchiveReceiptStatus.SHORTAGE;
		writer.receiptActualDocuments = 0;
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH);
		assertThat(result.failure().retryable()).isTrue();
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 0, null));
	}

	@Test
	@DisplayName("Равный count с другим identity digest открывает retryable replay")
	void failsOnEqualCountIdentityMismatch() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		writer.receiptStatus = ArchiveReceiptStatus.IDENTITY_MISMATCH;
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.INDEX_RECEIPT_MISMATCH);
		assertThat(result.failure().retryable()).isTrue();
		assertThat(result.receipt())
				.isNotNull()
				.satisfies(receipt -> {
					assertThat(receipt.status())
							.isEqualTo(ArchiveReceiptStatus.IDENTITY_MISMATCH);
					assertThat(receipt.expectedDocumentCount())
							.isEqualTo(receipt.actualDocumentCount());
					assertThat(receipt.actualDigest())
							.isNotEqualTo(receipt.expectedDigest());
				});
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 1, null));
	}

	@Test
	@DisplayName("Лишний stale документ даёт отдельный non-retryable receipt surplus")
	void failsWhenReceiptContainsExtraDocument() {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		writer.receiptStatus = ArchiveReceiptStatus.SURPLUS;
		writer.receiptActualDocuments = 2;
		DefaultGdeltArchiveProcessor processor = processor(writer, validMentions(1), null);

		ArchiveProcessingResult result = processor.process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing());

		assertThat(result.outcome()).isEqualTo(ArchiveProcessingOutcome.FAILED);
		assertThat(result.failure().code())
				.isEqualTo(ArchiveProcessingErrorCode.INDEX_RECEIPT_SURPLUS);
		assertThat(result.failure().retryable()).isFalse();
		assertThat(result.progress()).isEqualTo(
				new ArchiveProcessingProgress(1, 0, 0, 1, 1, 0, 2, null));
	}

	@Test
	@DisplayName("Проецирует ожидаемые CSV failures в стабильные processing codes")
	void projectsExpectedCsvFailures() {
		ArchiveProcessingResult schemaFailure = processReaderFailure(
				new GdeltCsvSchemaException(GdeltCsvErrorCode.CSV_SCHEMA_MISMATCH));
		ArchiveProcessingResult accessFailure = processReaderFailure(
				new GdeltCsvAccessException(GdeltCsvErrorCode.CSV_FILESYSTEM_IO_FAILURE));

		assertThat(schemaFailure.failure()).satisfies(failure -> {
			assertThat(failure.code())
					.isEqualTo(ArchiveProcessingErrorCode.CSV_SOURCE_SCHEMA_FAILURE);
			assertThat(failure.retryable()).isFalse();
		});
		assertThat(accessFailure.failure()).satisfies(failure -> {
			assertThat(failure.code())
					.isEqualTo(ArchiveProcessingErrorCode.CSV_SOURCE_ACCESS_FAILURE);
			assertThat(failure.retryable()).isTrue();
		});
	}

	@Test
	@DisplayName("Проецирует CSV interruption и не очищает interrupt flag")
	void preservesInterruptSignal() {
		Thread.currentThread().interrupt();
		try {
			GdeltCsvInterruptedException interruption =
					new GdeltCsvInterruptedException();
			AtomicReference<RuntimeException> diagnostic = new AtomicReference<>();
			ArchiveProcessingResult result = processReaderFailure(
					interruption,
					diagnostic::set);

			assertThat(result.failure().code())
					.isEqualTo(ArchiveProcessingErrorCode.CSV_SOURCE_INTERRUPTED);
			assertThat(result.failure().retryable()).isTrue();
			assertThat(diagnostic).hasValue(interruption);
			assertThat(Thread.currentThread().isInterrupted()).isTrue();
		}
		finally {
			Thread.interrupted();
		}
	}

	private static DefaultGdeltArchiveProcessor processor(
			FakeIndexWriter writer,
			List<GdeltMention> mentions,
			AtomicInteger delivered
	) {
		return processor(
				writer,
				mentions,
				delivered,
				new SimpleMeterRegistry());
	}

	private static DefaultGdeltArchiveProcessor processor(
			FakeIndexWriter writer,
			List<GdeltMention> mentions,
			AtomicInteger delivered,
			SimpleMeterRegistry meterRegistry
	) {
		GdeltMentionCsvReader mentionReader = (path, consumer) -> {
			long lineNumber = 0;
			for (GdeltMention mention : mentions) {
				lineNumber++;
				if (delivered != null) {
					delivered.incrementAndGet();
				}
				consumer.accept(new GdeltCsvRecord<>(
						lineNumber,
						mention));
			}
			return new GdeltCsvReadSummary(
					GdeltArchiveKind.TRANSLATION_MENTIONS,
					mentions.size(),
					mentions.size(),
					0,
					Map.of(),
					Map.of());
		};
		return processor(writer, mentionReader, meterRegistry);
	}

	private static DefaultGdeltArchiveProcessor processor(
			FakeIndexWriter writer,
			GdeltMentionCsvReader mentionReader
	) {
		return processor(writer, mentionReader, new SimpleMeterRegistry());
	}

	private static DefaultGdeltArchiveProcessor processor(
			FakeIndexWriter writer,
			GdeltMentionCsvReader mentionReader,
			SimpleMeterRegistry meterRegistry
	) {
		GdeltEventCsvReader unusedEventReader = (path, consumer) -> {
			throw new AssertionError("Event reader must not be called");
		};
		return new DefaultGdeltArchiveProcessor(
				unusedEventReader,
				mentionReader,
				new GdeltEventDocumentMapper(),
				new GdeltMentionDocumentMapper(),
				writer,
				new ProcessingMetrics(meterRegistry));
	}

	private static ArchiveProcessingResult processReaderFailure(RuntimeException failure) {
		return processReaderFailure(
				failure,
				ArchiveProcessingDiagnosticListener.ignoring());
	}

	private static ArchiveProcessingResult processReaderFailure(
			RuntimeException failure,
			ArchiveProcessingDiagnosticListener diagnosticListener
	) {
		FakeIndexWriter writer = new FakeIndexWriter(2);
		GdeltMentionCsvReader reader = (path, consumer) -> {
			throw failure;
		};
		return processor(writer, reader).process(
				ProcessingTestFixtures.mentionRequest(),
				ArchiveProcessingProgressListener.continuing(),
				diagnosticListener);
	}

	private static DefaultGdeltArchiveProcessor eventProcessor(FakeIndexWriter writer) {
		GdeltEvent event = ProcessingTestFixtures.event(
				700_000_001L,
				LocalDate.of(2026, 7, 21),
				EVENT_TIME,
				ProcessingTestFixtures.geo("Actor 1", 55.75, 37.61),
				ProcessingTestFixtures.absentGeo(),
				ProcessingTestFixtures.geo("Action", 40.71, -74.00));
		GdeltEventCsvReader eventReader = (path, consumer) -> {
			consumer.accept(new GdeltCsvRecord<>(1, event));
			return new GdeltCsvReadSummary(
					GdeltArchiveKind.TRANSLATION_EVENTS,
					1,
					1,
					0,
					Map.of(),
					Map.of());
		};
		GdeltMentionCsvReader unusedMentionReader = (path, consumer) -> {
			throw new AssertionError("Mention reader must not be called");
		};
		return new DefaultGdeltArchiveProcessor(
				eventReader,
				unusedMentionReader,
				new GdeltEventDocumentMapper(),
				new GdeltMentionDocumentMapper(),
				writer,
				new ProcessingMetrics(new SimpleMeterRegistry()));
	}

	private static ArchiveIdentityDigest mentionDigest(long... sourceLineNumbers) {
		ArchiveIdentityDigest.Accumulator accumulator = ArchiveIdentityDigest.accumulator();
		MentionIdentityFactory identityFactory = new MentionIdentityFactory();
		for (long sourceLineNumber : sourceLineNumbers) {
			accumulator.addIdentity(identityFactory.rawMentionId(
					ProcessingTestFixtures.SOURCE_ARCHIVE_KEY,
					sourceLineNumber));
		}
		return accumulator.finish();
	}

	private static List<GdeltMention> validMentions(int count) {
		List<GdeltMention> mentions = new ArrayList<>(count);
		for (int index = 0; index < count; index++) {
			mentions.add(ProcessingTestFixtures.mention(
					700_000_001L + index,
					EVENT_TIME,
					MENTION_TIME,
					1,
					"identifier-" + index));
		}
		return List.copyOf(mentions);
	}

	private static GdeltMention mentionWithMissingTime(int index) {
		return ProcessingTestFixtures.mention(
				700_000_001L + index,
				EVENT_TIME,
				null,
				1,
				"identifier-" + index);
	}

	private static final class FakeIndexWriter implements GdeltIndexWriter {

		private final int bulkSize;
		private final long maxBulkBytes;
		private final long operationBytes;
		private final List<BulkIndexCommand<? extends GdeltIndexedDocument>> commands =
				new ArrayList<>();
		private final List<ArchiveReceiptQuery> receiptQueries = new ArrayList<>();
		private final List<GdeltIndexKind> refreshedKinds = new ArrayList<>();
		private final List<ExactIndexTarget> refreshedTargets = new ArrayList<>();
		private final List<ExactIndexTarget> estimatedTargets = new ArrayList<>();
		private final List<OperationBudget> requestBudgets = new ArrayList<>();

		private BulkIndexResult nextResult;
		private RuntimeException writeFailure;
		private RuntimeException refreshFailure;
		private RuntimeException receiptFailure;
		private ArchiveReceiptStatus receiptStatus = ArchiveReceiptStatus.MATCHED;
		private long receiptActualDocuments = -1;

		private FakeIndexWriter(int bulkSize) {
			this(bulkSize, Long.MAX_VALUE, 1);
		}

		private FakeIndexWriter(
				int bulkSize,
				long maxBulkBytes,
				long operationBytes
		) {
			this.bulkSize = bulkSize;
			this.maxBulkBytes = maxBulkBytes;
			this.operationBytes = operationBytes;
		}

		@Override
		public int bulkSize() {
			return bulkSize;
		}

		@Override
		public long maxBulkBytes() {
			return maxBulkBytes;
		}

		@Override
		public long estimateBulkOperationBytes(
				ExactIndexTarget target,
				GdeltIndexedDocument document
		) {
			estimatedTargets.add(target);
			return operationBytes;
		}

		@Override
		public void prepareReadModel() {
			// Lifecycle coordinator owns preparation; processor не вызывает этот метод.
		}

		@Override
		public BulkIndexResult write(
				BulkIndexCommand<? extends GdeltIndexedDocument> command
		) {
			commands.add(command);
			if (writeFailure != null) {
				throw writeFailure;
			}
			if (nextResult != null) {
				BulkIndexResult result = nextResult;
				nextResult = null;
				return result;
			}
			return new BulkIndexResult(
					command.kind(),
					command.documents().size(),
					command.documents().size(),
					0,
					null,
					BulkIndexOutcome.SUCCEEDED);
		}

		@Override
		public BulkIndexResult write(
				BulkIndexCommand<? extends GdeltIndexedDocument> command,
				OperationBudget budget
		) {
			requestBudgets.add(budget);
			return write(command);
		}

		@Override
		public void refresh(GdeltIndexKind kind, ExactIndexTarget target) {
			refreshedKinds.add(kind);
			refreshedTargets.add(target);
			if (refreshFailure != null) {
				throw refreshFailure;
			}
		}

		@Override
		public void refresh(
				GdeltIndexKind kind,
				ExactIndexTarget target,
				OperationBudget budget
		) {
			requestBudgets.add(budget);
			refresh(kind, target);
		}

		@Override
		public ArchiveReceiptVerification verifyReceipt(ArchiveReceiptQuery query) {
			if (!refreshedKinds.contains(query.kind())
					|| !refreshedTargets.contains(query.target())) {
				throw new AssertionError("Receipt must follow target index refresh");
			}
			receiptQueries.add(query);
			if (receiptFailure != null) {
				throw receiptFailure;
			}
			long actualDocuments = receiptActualDocuments < 0
					? query.expectedDocumentCount()
					: receiptActualDocuments;
			return new ArchiveReceiptVerification(
					query.kind(),
					query.expectedDocumentCount(),
					actualDocuments,
					query.expectedDigest(),
					receiptStatus == ArchiveReceiptStatus.IDENTITY_MISMATCH
							? ArchiveIdentityDigest.accumulator()
									.addIdentity("different-identity")
									.finish()
							: query.expectedDigest(),
					receiptStatus);
		}

		@Override
		public ArchiveReceiptVerification verifyReceipt(
				ArchiveReceiptQuery query,
				OperationBudget budget
		) {
			requestBudgets.add(budget);
			return verifyReceipt(query);
		}
	}
}
