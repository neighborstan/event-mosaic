package com.neighbor.eventmosaic.ingestion.audit;

import com.neighbor.eventmosaic.indexing.api.ArchiveIdentityDigest;
import com.neighbor.eventmosaic.indexing.api.ArchiveReceiptQuery;
import com.neighbor.eventmosaic.indexing.api.ExactIndexTarget;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexWriter;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import com.neighbor.eventmosaic.indexing.api.IndexingAccessException;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.ingestion.config.BackendDataProperties;
import com.neighbor.eventmosaic.ingestion.config.GdeltIngestionProperties;
import com.neighbor.eventmosaic.ingestion.error.IngestionInterruptedException;
import com.neighbor.eventmosaic.shared.error.ApplicationException;
import com.neighbor.eventmosaic.shared.time.OperationBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Понемногу проверяет уже загруженные архивы и сохраняет обнаруженные причины восстановления. */
@Service
public class ReceiptAuditService {

	private static final Logger LOGGER = LoggerFactory.getLogger(ReceiptAuditService.class);
	private final JdbcReceiptAuditRepository repository;
	private final GdeltIndexWriter writer;
	private final int pageSize;
	private final int batchSize;

	/** Связывает отдельный журнал проверки с чтением документов Elasticsearch. */
	public ReceiptAuditService(
			JdbcReceiptAuditRepository repository,
			GdeltIndexWriter writer,
			BackendDataProperties backendProperties,
			GdeltIngestionProperties properties
	) {
		this.repository = repository;
		this.writer = writer;
		this.pageSize = backendProperties.receiptPageSize();
		this.batchSize = Math.min(2, properties.automatic().receiptAudit().batchSize());
	}

	/**
	 * Проверяет не больше двух самых давно проверенных архивов, когда наступил сохраненный срок.
	 * Ошибка Elasticsearch откладывает только проверку, сохраняя успешный статус обработки архива.
	 *
	 * @param budget оставшееся время и право текущего цикла на работу
	 * @return число проверок и наличие расхождений или недоступности Elasticsearch
	 */
	public AuditBatchResult auditDue(OperationBudget budget) {
		Long continuationVersion = null;
		int checked = 0;
		int mismatched = 0;
		for (int item = 0; item < batchSize; item++) {
			requireNotInterrupted();
			budget.requireAvailable();
			var claimed = repository.claim(continuationVersion);
			if (claimed.isEmpty()) {
				break;
			}
			ReceiptAuditClaim claim = claimed.orElseThrow();
			try {
				requireNotInterrupted();
				budget.requireAvailable();
				var target = claim.currentTarget();
				var receipt = claim.processing().receipt();
				var query = new ArchiveReceiptQuery(
						target.indexKind(),
						new ExactIndexTarget(target.indexName(), target.indexUuid()),
						claim.processing().archiveIdempotencyKey(),
						claim.processing().fingerprint().processingFingerprint(),
						receipt.expectedDocumentCount(),
						new ArchiveIdentityDigest(receipt.expectedIdentityDigest()),
						pageSize);
				var verification = writer.verifyReceipt(query, budget);
				requireNotInterrupted();
				budget.requireAvailableAfterExternalResult();
				var completed = repository.complete(claim, verification);
				if (completed.isEmpty()) {
					break;
				}
				continuationVersion = completed.orElseThrow();
				checked++;
				if (!verification.matched()) {
					mismatched++;
				}
			} catch (IndexTargetUnavailableException exception) {
				requireNotInterrupted();
				budget.requireAvailableAfterExternalResult();
				if (exception.reason() == IndexTargetUnavailableReason.WRITE_BLOCKED) {
					repository.fail(claim, exception.errorCode().code());
					logFailure(exception);
					return new AuditBatchResult(checked, mismatched, true);
				}
				var completed = repository.missing(claim, exception.reason());
				if (completed.isEmpty()) {
					break;
				}
				continuationVersion = completed.orElseThrow();
				checked++;
				mismatched++;
			} catch (IndexingAccessException | IndexingProtocolException exception) {
				requireNotInterrupted();
				budget.requireAvailableAfterExternalResult();
				repository.fail(claim, exception.errorCode().code());
				logFailure(exception);
				return new AuditBatchResult(checked, mismatched, true);
			}
		}
		return new AuditBatchResult(checked, mismatched, false);
	}

	private static void requireNotInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throw new IngestionInterruptedException();
		}
	}

	private static void logFailure(ApplicationException exception) {
		LOGGER.atWarn().addKeyValue("event", "ingestion_receipt_audit_failed")
				.addKeyValue("error_code", exception.errorCode().code())
				.addKeyValue("retryable", true).setCause(exception)
				.log("Проверка сохраненных документов отложена");
	}

	/** Итог небольшого пакета проверок, пригодный для результата всего цикла. */
	public record AuditBatchResult(int checked, int mismatched, boolean infrastructureFailure) {
	}
}
