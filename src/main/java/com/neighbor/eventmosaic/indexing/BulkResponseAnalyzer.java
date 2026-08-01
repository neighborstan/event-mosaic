package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexedDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableException;
import com.neighbor.eventmosaic.indexing.api.IndexTargetUnavailableReason;
import java.util.List;
import java.util.Objects;

/**
 * Сопоставляет каждый Elasticsearch bulk item с исходной физической строкой.
 */
final class BulkResponseAnalyzer {
	private static final String INDEX_NOT_FOUND_ERROR_TYPE = "index_not_found_exception";
	private static final String CLUSTER_BLOCK_ERROR_TYPE = "cluster_block_exception";

	private BulkResponseAnalyzer() {
	}

	/**
	 * Анализирует весь ответ без раннего выхода после первого отказа.
	 *
	 * @param command исходная порция документов
	 * @param items элементы ответа в порядке запроса
	 * @return полные подтвержденные счетчики и агрегированная retryability
	 */
	static BulkIndexResult analyze(
			BulkIndexCommand<? extends GdeltIndexedDocument> command,
			List<BulkResponseItem> items
	) {
		Objects.requireNonNull(command, "command must not be null");
		List<BulkResponseItem> responseItems = List.copyOf(
				Objects.requireNonNull(items, "items must not be null"));
		if (responseItems.size() != command.documents().size()) {
			throw new IndexingProtocolException(IndexingErrorCode.INDEXING_RESPONSE_INVALID);
		}
		validateExactTarget(command, responseItems);

		long succeeded = 0;
		long failed = 0;
		Long firstFailedLineNumber = null;
		boolean permanentFailure = false;

		for (int index = 0; index < responseItems.size(); index++) {
			BulkResponseItem item = responseItems.get(index);
			if (isSuccessful(item)) {
				succeeded++;
				continue;
			}

			failed++;
			if (firstFailedLineNumber == null) {
				firstFailedLineNumber = command.documents().get(index).sourceLineNumber();
			}
			if (!isTransient(item.status())) {
				permanentFailure = true;
			}
		}

		BulkIndexOutcome outcome = outcome(failed, permanentFailure);
		return new BulkIndexResult(
				command.kind(),
				responseItems.size(),
				succeeded,
				failed,
				firstFailedLineNumber,
				outcome);
	}

	private static void validateExactTarget(
			BulkIndexCommand<? extends GdeltIndexedDocument> command,
			List<BulkResponseItem> items
	) {
		for (BulkResponseItem item : items) {
			if (!command.target().indexName().equals(item.index())) {
				throw new IndexingProtocolException(
						IndexingErrorCode.INDEXING_RESPONSE_INVALID);
			}
			if (item.error() == null) {
				continue;
			}
			if (INDEX_NOT_FOUND_ERROR_TYPE.equals(item.error().type())) {
				throw new IndexTargetUnavailableException(
						IndexTargetUnavailableReason.MISSING);
			}
			if (CLUSTER_BLOCK_ERROR_TYPE.equals(item.error().type())) {
				throw new IndexTargetUnavailableException(
						IndexTargetUnavailableReason.WRITE_BLOCKED);
			}
		}
	}

	private static boolean isSuccessful(BulkResponseItem item) {
		int status = item.status();
		return item.error() == null && status >= 200 && status < 300;
	}

	private static boolean isTransient(int status) {
		return status == 408 || status == 429 || (status >= 500 && status <= 599);
	}

	private static BulkIndexOutcome outcome(long failed, boolean permanentFailure) {
		if (failed == 0) {
			return BulkIndexOutcome.SUCCEEDED;
		}
		return permanentFailure
				? BulkIndexOutcome.NON_RETRYABLE_PARTIAL_FAILURE
				: BulkIndexOutcome.RETRYABLE_PARTIAL_FAILURE;
	}

}
