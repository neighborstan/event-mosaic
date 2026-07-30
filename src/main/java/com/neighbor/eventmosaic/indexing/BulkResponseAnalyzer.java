package com.neighbor.eventmosaic.indexing;

import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.neighbor.eventmosaic.indexing.api.BulkIndexCommand;
import com.neighbor.eventmosaic.indexing.api.BulkIndexOutcome;
import com.neighbor.eventmosaic.indexing.api.BulkIndexResult;
import com.neighbor.eventmosaic.indexing.api.GdeltIndexedDocument;
import com.neighbor.eventmosaic.indexing.api.IndexingErrorCode;
import com.neighbor.eventmosaic.indexing.api.IndexingProtocolException;
import java.util.List;
import java.util.Objects;

/**
 * Сопоставляет каждый Elasticsearch bulk item с исходной физической строкой.
 */
final class BulkResponseAnalyzer {

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
